import asyncio
import logging
import socket
from typing import List, Optional, Tuple

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

    async def _ensure_session(self):
        """确保 session 已经初始化"""
        if self.session is None:
            # 创建高度优化的连接器
            self.connector = aiohttp.TCPConnector(
                # 强制 IPv4，避免 IPv6 回退延迟
                family=socket.AF_INET,

                # DNS 优化 - 使用系统 DNS 而不是 aiodns
                use_dns_cache=True,
                ttl_dns_cache=60,

                # 连接池优化
                limit=50,
                limit_per_host=10,
                keepalive_timeout=10,
                enable_cleanup_closed=True,

                # 禁用 SSL 相关检查
                ssl=False,
            )

            # 创建长连接 session
            loop = asyncio.new_event_loop()
            loop.set_debug(True)
            self.session = aiohttp.ClientSession(
                connector=self.connector,
                loop=loop,
                timeout=aiohttp.ClientTimeout(total=1.0)  # 默认总超时 1 秒
            )

    async def close(self):
        """关闭连接"""
        if self.session:
            await self.session.close()
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
                headers={"Connection": "keep-alive"}  # 明确启用 keep-alive
            ) as response:
                if response.status != 200:
                    route_logger.error(
                        f"Failed to get master response from {master_addr}, http status: {response.status}"
                    )
                    return None, inter_request_id

                result = await response.json()

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
