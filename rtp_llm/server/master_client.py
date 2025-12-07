import asyncio
import logging
import socket
from typing import List, Optional, Tuple
import json
import os
import atexit
import time

import aiohttp

from rtp_llm.config.exceptions import ExceptionType, FtRuntimeException
from rtp_llm.config.generate_config import RoleAddr, RoleType
from rtp_llm.server.worker_status import ScheduleMeta

route_logger = logging.getLogger("route_logger")


class MasterClient:
    def __init__(self):
        # 延迟初始化 - 不在 __init__ 中创建 aiohttp 组件
        self.session = None
        self.connector = None
        # 多进程环境下的进程ID追踪
        self._process_id = os.getpid()
        # 注册清理函数
        atexit.register(self._cleanup_on_exit)

    async def _ensure_session(self):
        """确保 session 已经初始化"""
        # 多进程环境检查：如果进程ID变了，需要重建session
        current_pid = os.getpid()
        if current_pid != self._process_id:
            # 进程已fork，需要重新初始化
            self.session = None
            self.connector = None
            self._process_id = current_pid

        if self.session is None or self.session.closed:
            # 关键修改：使用 get_running_loop() 获取当前正在运行的事件循环
            # 这样可以确保 session 在正确的事件循环中创建
            loop = asyncio.get_running_loop()

            # 删除debug模式，它会影响性能
            loop.set_debug(True)  # 已删除

            # 创建高度优化的连接器
            self.connector = aiohttp.TCPConnector(
                # 强制 IPv4，避免 IPv6 回退延迟
                family=socket.AF_INET,

                # DNS 优化 - 增加缓存时间
                use_dns_cache=True,
                ttl_dns_cache=300,  # 增加到5分钟

                # 连接池优化 - 增加连接数
                limit=100,  # 增加总连接数
                limit_per_host=30,  # 增加每主机连接数
                keepalive_timeout=30,  # 增加 keep-alive 时间

                # 关键配置：不强制关闭连接，允许连接复用
                force_close=False,
                enable_cleanup_closed=True,

                # 禁用 SSL 相关检查
                ssl=False
            )

            # 创建长连接 session
            self.session = aiohttp.ClientSession(
                connector=self.connector,
                timeout=aiohttp.ClientTimeout(
                    total=1.0,
                    connect=0.3,
                    sock_connect=0.2,
                    sock_read=0.5
                ),
                # 跳过不必要的自动headers，提升性能
                skip_auto_headers={'User-Agent'},
                # 重要：确保响应自动关闭
                connector_owner=True,
                # 禁用自动解压，减少异步生成器
                auto_decompress=False
            )

    def _cleanup_on_exit(self):
        """进程退出时的清理函数（多进程环境）"""
        if self.session and not self.session.closed:
            # 在多进程环境下，不能使用 await，直接关闭
            try:
                # 关键修改：不创建新的事件循环，直接强制关闭
                # 这避免了在进程退出时创建事件循环的问题
                if hasattr(self.session, '_connector'):
                    self.session._connector._force_close = True
                    if hasattr(self.session._connector, '_conns'):
                        self.session._connector._conns.clear()
                # 标记为已关闭，避免异步清理
                self.session._closed = True
            except Exception:
                # 忽略清理时的异常
                pass
            finally:
                self.session = None
                self.connector = None

    async def close(self):
        """关闭连接"""
        if self.session:
            await self.session.close()
            # 等待一小段时间确保清理完成
            await asyncio.sleep(0)
            self.session = None
            self.connector = None

    async def get_backend_role_addrs(
        self,
        master_addr: Optional[str],
        block_cache_keys: list[int],
        seq_len: int,
        debug: bool,
        generate_timeout: int,
        request_priority: int = 100,
    ) -> Tuple[Optional[List[RoleAddr]], int]:
        # 设置当前任务名称，方便调试
        current_task = asyncio.current_task()
        if current_task:
            # 使用时间戳和进程ID创建唯一的任务名
            task_name = f"MasterClient-HTTP-{self._process_id}-{int(time.time()*1000)}"
            current_task.set_name(task_name)
            route_logger.debug(f"Starting HTTP request with task name: {task_name}")

        # 确保 session 已经初始化
        await self._ensure_session()

        inter_request_id = -1
        # get master address
        if not master_addr:
            return None, inter_request_id
        url = f"http://{master_addr}/rtp_llm/schedule"
        payload = {
            "model": "engine_service",
            "block_cache_keys": block_cache_keys,
            "seq_len": seq_len,
            "debug": debug,
            "request_priority": request_priority,
        }
        if generate_timeout != -1:
            payload["generate_timeout"] = generate_timeout

        try:
            # 使用非常短的超时配置
            timeout = aiohttp.ClientTimeout(
                total=0.8,        # 总超时 800ms
                connect=0.2,      # 连接超时 200ms
                sock_connect=0.15, # Socket 连接 150ms
                sock_read=0.3,    # 读取超时 300ms
            )

            async with self.session.post(
                url,
                json=payload,
                timeout=timeout,
                headers={
                    "Connection": "keep-alive",  # 明确启用 keep-alive
                    "Content-Type": "application/json"
                },
                # 添加额外的优化选项
                allow_redirects=False,  # 禁用重定向
                raise_for_status=False  # 禁用自动状态码检查
            ) as response:
                if response.status != 200:
                    route_logger.error(
                        f"Failed to get master response from {master_addr}, http status: {response.status}"
                    )
                    return None, inter_request_id

                # 关键修改：先读取全部响应体，再解析JSON
                # 这样可以确保响应体被完全消费，避免异步生成器泄漏
                body = await response.read()
                # 立即释放响应对象，避免异步生成器延迟清理
                response.release()
                result = json.loads(body)

        except Exception as e:
            route_logger.error(f"Failed to query master at {master_addr}: {type(e).__name__}: {e}")
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
