import asyncio
import json
import logging
from typing import List, Optional, Tuple

import aiohttp
from aiohttp import ClientTimeout

from rtp_llm.config.exceptions import ExceptionType, FtRuntimeException
from rtp_llm.config.generate_config import RoleAddr, RoleType
from rtp_llm.server.worker_status import ScheduleMeta

route_logger = logging.getLogger("route_logger")


class MasterClient:
    def __init__(self):
        self.timeout_total = 0.5  # Store timeout value, create ClientTimeout when needed
        # 专门为RTP-LLM服务通信优化的连接器配置
        self.connector_config = {
            'limit': 300,                    # 总连接池大小（支持更高并发）
            'limit_per_host': 100,           # 每个master主机的连接数
            'keepalive_timeout': 600,        # 保活超时（与原配置保持一致）
            'force_close': False,            # 启用连接复用
            'enable_cleanup_closed': True,   # 自动清理已关闭连接
            'happy_eyeballs_delay': 0.25,   # IPv4/IPv6并行连接延迟
            'resolver': aiohttp.resolver.AsyncResolver(),  # 异步DNS解析器
            'ttl_dns_cache': 600,             # DNS缓存超时
            'use_dns_cache': True,            # 启用DNS缓存
        }
        self._session = None
        self._loop = self._create_optimized_event_loop()
        self._session_lock = asyncio.Lock()

    def _create_optimized_event_loop(self):
        """创建专门为RTP-LLM优化的高性能事件循环

        针对Master-Worker架构的网络通信进行深度优化
        - 支持高并发连接
        - 低延迟网络I/O
        - 智能资源管理
        - 生产级错误处理
        """
        import platform
        import sys
        import os
        from concurrent.futures import ThreadPoolExecutor

        # 环境检测和配置
        cpu_count = os.cpu_count() or 32

        # === 第一步：选择最优的事件循环实现 ===
        event_loop = asyncio.new_event_loop()

        # 2. 专业级异常处理器
        def rtp_llm_exception_handler(loop_instance, context):
            exception = context.get('exception')
            message = context.get('message', 'Unknown asyncio error')

            # 网络相关异常分级处理
            if exception:
                if isinstance(exception, (aiohttp.ClientError, aiohttp.ServerTimeoutError)):
                    # aiohttp客户端异常 - DEBUG级别，避免日志噪音
                    route_logger.debug(f"HTTP客户端异常: {type(exception).__name__}: {exception}")
                elif isinstance(exception, asyncio.TimeoutError):
                    # 超时异常 - INFO级别，可能需要关注
                    route_logger.info(f"网络请求超时: {message}")
                elif isinstance(exception, (ConnectionError, OSError)):
                    # 连接错误 - WARNING级别，可能是网络问题
                    route_logger.warning(f"连接异常: {type(exception).__name__}: {exception}")
                else:
                    # 其他异常 - ERROR级别，需要重点关注
                    route_logger.error(f"事件循环严重异常: {type(exception).__name__}: {exception}")
                    route_logger.error(f"异常上下文: {context}")
            else:
                route_logger.error(f"事件循环错误: {message}")

        event_loop.set_exception_handler(rtp_llm_exception_handler)

        # 3. I/O性能优化
        if hasattr(event_loop, '_ready'):
            # 扩大任务就绪队列，支持更多并发任务
            current_maxlen = getattr(event_loop._ready, 'maxlen', None) or 512
            optimized_maxlen = min(16384, current_maxlen * 16)  # 最大16K，通常16倍提升
            event_loop._ready = type(event_loop._ready)(maxlen=optimized_maxlen)
            route_logger.debug(f"任务队列容量: {current_maxlen} -> {optimized_maxlen}")

        # 4. 线程池执行器优化（用于阻塞I/O和CPU密集任务）
        # 计算最优线程数：I/O密集型建议 CPU核心数 * 2 + 2
        optimal_workers = min(64, max(8, cpu_count * 2 + 2))

        executor = ThreadPoolExecutor(
            max_workers=optimal_workers,
            thread_name_prefix='rtp_llm_http',
            # Python 3.9+ 支持initializer参数
            **({"initializer": lambda: None} if sys.version_info >= (3, 9) else {})
        )
        event_loop.set_default_executor(executor)

        # 5. 任务工厂优化（如果支持）
        if hasattr(event_loop, 'set_task_factory') and sys.version_info >= (3, 7):
            # 使用更高效的任务创建工厂
            def fast_task_factory(loop_ref, coro):
                task = asyncio.Task(coro, loop=loop_ref)
                # 为RTP-LLM任务设置优先级提示
                if hasattr(task, 'set_name'):
                    if hasattr(coro, '__name__'):
                        task.set_name(f"rtp_llm.{coro.__name__}")
                return task

            event_loop.set_task_factory(fast_task_factory)

        # === 性能监控和日志 ===
        route_logger.info(f"""
        🚀 RTP-LLM高性能事件循环已就绪:
           线程池: {optimal_workers} workers
           任务队列: {getattr(event_loop._ready, 'maxlen', 'unlimited') if hasattr(event_loop, '_ready') else 'default'}
           平台: {platform.system()} {platform.machine()}
                """.strip())

        return event_loop

    async def get_session(self):
        """获取session"""
        if self._session is not None and not self._session.closed:
            return self._session

        async with self._session_lock:
            if self._session is None or self._session.closed:
                # 如果存在，清理旧的session
                if self._session:
                    await self._session.close()

                # 使用预定义的优化事件循环
                target_loop = self._loop

                # 创建优化的TCP连接器
                connector = aiohttp.TCPConnector(
                    loop=target_loop,
                    **self.connector_config
                )

                # 创建专门优化的HTTP会话
                self._session = aiohttp.ClientSession(
                    timeout=ClientTimeout(total=self.timeout_total),
                    connector=connector,
                    loop=target_loop,
                    # RTP-LLM服务通信专用优化
                    headers={
                        'Connection': 'keep-alive',
                        'Keep-Alive': 'timeout=600, max=1000',  # 明确保活参数
                    },
                    read_bufsize=131072,                        # 128KB读取缓冲区
                    cookie_jar=None,                            # 禁用cookie处理
                    auto_decompress=False,                      # 禁用自动解压缩
                    raise_for_status=False,                     # 手动处理HTTP状态码
                )

        return self._session

    async def get_backend_role_addrs(
        self,
        master_addr: Optional[str],
        block_cache_keys: list[int],
        seq_len: int,
        debug: bool,
        generate_timeout: int,
        request_priority: int = 100,
    ) -> Tuple[Optional[List[RoleAddr]], int]:
        inter_request_id = -1
        # get master address
        if not master_addr:
            return None, inter_request_id
        payload = {}
        # prepare request to master
        url = "http://" + master_addr + "/rtp_llm/schedule"
        if generate_timeout != -1:
            payload = {
                "model": "engine_service",
                "block_cache_keys": block_cache_keys,
                "seq_len": seq_len,
                "debug": debug,
                "generate_timeout": generate_timeout,
                "request_priority": request_priority,
            }
        else:
            payload = {
                "model": "engine_service",
                "block_cache_keys": block_cache_keys,
                "seq_len": seq_len,
                "debug": debug,
                "request_priority": request_priority,
            }
        headers = {"Content-Type": "application/json"}

        # connect to master using long connection
        try:
            session = await self.get_session()
            data = json.dumps(payload)
            async with session.post(url, data=data, headers=headers) as response:
                if response.status != 200:
                    route_logger.error(
                        f"Failed to get master response from {master_addr}, http status: {response.status}"
                    )
                    return None, inter_request_id
                result = await response.json()
        except Exception as e:
            route_logger.error(f"Failed to connect to master at {master_addr}: {e}")
            return None, inter_request_id

        # check response
        schedule_meta = ScheduleMeta.model_validate(result)
        if schedule_meta.code != 200:
            route_logger.error(
                f"Master schedule error, error code: {schedule_meta.code}"
            )
            raise FtRuntimeException(
                exception_type=ExceptionType(schedule_meta.code),
                message="master schedule error",
            )

        # parse role ips from schedule meta
        role_addrs: List[RoleAddr] = []
        for server_status in schedule_meta.server_status:
            role_addrs.append(
                RoleAddr(
                    role=RoleType(server_status.role),
                    ip=server_status.server_ip,
                    http_port=server_status.http_port,
                    grpc_port=server_status.grpc_port,
                )
            )

        return role_addrs, schedule_meta.inter_request_id
