import asyncio
import logging
import socket
from typing import List, Optional, Tuple
from contextvars import ContextVar

import aiohttp

from rtp_llm.config.exceptions import ExceptionType, FtRuntimeException
from rtp_llm.config.generate_config import RoleAddr, RoleType
from rtp_llm.server.worker_status import ScheduleMeta

route_logger = logging.getLogger("route_logger")

# 使用 ContextVar 存储 session，确保每个事件循环有自己的 session
_session_context: ContextVar[Optional[aiohttp.ClientSession]] = ContextVar('session', default=None)


class MasterClient:
    def __init__(self):
        # 不在初始化时创建任何异步对象
        self._sessions = {}  # 存储每个事件循环的 session
        self._lock = asyncio.Lock()

    async def _get_session(self) -> aiohttp.ClientSession:
        """获取当前事件循环的 session"""
        loop = asyncio.get_running_loop()
        loop_id = id(loop)

        # 如果当前循环已有 session，直接返回
        if loop_id in self._sessions and not self._sessions[loop_id].closed:
            return self._sessions[loop_id]

        # 否则创建新的 session
        async with self._lock:
            # 双重检查
            if loop_id in self._sessions and not self._sessions[loop_id].closed:
                return self._sessions[loop_id]

            # 创建优化的连接器
            connector = aiohttp.TCPConnector(
                # 强制 IPv4
                family=socket.AF_INET,

                # DNS 缓存
                use_dns_cache=True,
                ttl_dns_cache=300,

                # 连接池配置
                limit=100,
                limit_per_host=30,
                keepalive_timeout=30,

                # 禁用 SSL
                ssl=False,

                # 禁用 Nagle 算法，减少延迟
                force_close=False,
                enable_cleanup_closed=True,
            )

            # 创建 session
            session = aiohttp.ClientSession(
                connector=connector,
                timeout=aiohttp.ClientTimeout(
                    total=2.0,
                    connect=0.5,
                    sock_connect=0.3,
                    sock_read=1.0
                ),
                # 禁用自动解压，减少 CPU 开销
                auto_decompress=False,
                # 跳过自动 headers
                skip_auto_headers={'User-Agent'},
            )

            self._sessions[loop_id] = session

            # 注册清理函数
            loop.call_soon(self._register_cleanup, loop, session)

            return session

    def _register_cleanup(self, loop: asyncio.AbstractEventLoop, session: aiohttp.ClientSession):
        """注册 session 清理函数"""
        def cleanup():
            if not session.closed:
                asyncio.create_task(session.close())

        # 当事件循环关闭时清理 session
        try:
            loop.add_signal_handler(signal.SIGTERM, cleanup)
        except:
            pass

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

        # 获取当前事件循环的 session
        session = await self._get_session()

        try:
            # 使用更短的超时，避免长时间等待
            async with session.post(
                url,
                json=payload,
                # 针对每个请求的超时设置
                timeout=aiohttp.ClientTimeout(
                    total=1.0,
                    connect=0.3,
                    sock_connect=0.2,
                    sock_read=0.5
                ),
                headers={
                    "Connection": "keep-alive",
                    "Content-Type": "application/json",
                },
                # 禁用重定向
                allow_redirects=False,
                # 跳过状态码检查
                raise_for_status=False,
            ) as response:
                if response.status != 200:
                    route_logger.error(
                        f"Failed to get master response from {master_addr}, http status: {response.status}"
                    )
                    return None, inter_request_id

                # 直接读取响应体，避免 json() 方法的额外开销
                result = await response.json()

        except asyncio.TimeoutError:
            route_logger.error(f"Request to {master_addr} timed out")
            return None, inter_request_id
        except Exception as e:
            route_logger.error(f"Failed to query master at {master_addr}: {type(e).__name__}: {e}")
            return None, inter_request_id

        # 解析响应
        try:
            schedule_meta = ScheduleMeta.model_validate(result)
        except Exception as e:
            route_logger.error(f"Failed to parse schedule meta: {e}")
            return None, inter_request_id

        if schedule_meta.code != 200:
            route_logger.error(f"Master schedule error, error code: {schedule_meta.code}")
            raise FtRuntimeException(
                exception_type=ExceptionType(schedule_meta.code),
                message="master schedule error",
            )

        # 构建角色地址列表
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

    async def close(self):
        """关闭所有 sessions"""
        for session in self._sessions.values():
            if not session.closed:
                await session.close()
        self._sessions.clear()


# 单例模式，避免创建多个实例
_master_client = None

def get_master_client() -> MasterClient:
    """获取 MasterClient 单例"""
    global _master_client
    if _master_client is None:
        _master_client = MasterClient()
    return _master_client