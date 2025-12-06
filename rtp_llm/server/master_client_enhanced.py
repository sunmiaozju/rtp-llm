"""
增强版的 Master Client，添加详细的监控和排查功能
"""
import asyncio
import json
import logging
import time
import uuid
from typing import List, Optional, Tuple

import aiohttp
from aiohttp import ClientTimeout

from rtp_llm.config.exceptions import ExceptionType, FtRuntimeException
from rtp_llm.config.generate_config import RoleAddr, RoleType
from rtp_llm.server.event_loop_monitor import RequestTimer, get_global_monitor, monitor_request
from rtp_llm.server.worker_status import ScheduleMeta

route_logger = logging.getLogger("route_logger")


class EnhancedMasterClient:
    """增强版 Master Client，带监控和调试功能"""

    def __init__(self, enable_monitoring: bool = True):
        # 延长超时，分别设置连接和读取超时
        self.timeout = ClientTimeout(
            total=5.0,      # 总超时改为 5 秒
            connect=2.0,    # 连接超时 2 秒
            sock_read=3.0   # 读取超时 3 秒
        )

        # 优化连接器配置
        self.connector_config = {
            'limit': 50,                    # 降低总连接数
            'limit_per_host': 10,          # 降低单主机连接数，避免过多连接
            'keepalive_timeout': 300,       # 降低 keepalive 超时
            'force_close': False,
            'enable_cleanup_closed': True,
            'use_dns_cache': True,
            'ttl_dns_cache': 300,
            'happy_eyeballs_delay': 0.25,   # 优化双栈连接
        }

        self._session = None
        self._session_lock = asyncio.Lock()
        self._session_create_time = None
        self._session_request_count = 0

        # 监控相关
        self.enable_monitoring = enable_monitoring
        self._request_stats = {}
        self._slow_requests = []

        if enable_monitoring:
            # 启动全局事件循环监控
            get_global_monitor()

        # 定期重建 session，避免连接池问题
        self._session_max_requests = 1000
        self._session_max_age = 3600  # 1 小时

    async def get_session(self):
        """获取 session，带详细监控"""
        request_id = str(uuid.uuid4())[:8]
        timer = RequestTimer(f"get_session_{request_id}")

        timer.stage_start("check_existing_session")

        # 检查现有 session 是否可用
        if self._session is not None and not self._session.closed:
            # 检查是否需要重建 session
            if self._should_recreate_session():
                route_logger.info(f"[{request_id}] Session needs recreation")
                timer.stage_end("check_existing_session")
                timer.stage_start("recreate_session")
                await self._recreate_session()
                timer.stage_end("recreate_session")
            else:
                timer.stage_end("check_existing_session")
                return self._session

        timer.stage_end("check_existing_session")
        timer.stage_start("acquire_lock")

        # 使用锁保护 session 创建
        try:
            # 设置获取锁的超时时间
            async with asyncio.wait_for(self._session_lock.acquire(), timeout=2.0):
                try:
                    timer.stage_end("acquire_lock")
                    timer.stage_start("create_new_session")

                    if self._session is None or self._session.closed or self._should_recreate_session():
                        await self._create_new_session(request_id)

                    timer.stage_end("create_new_session")
                    return self._session

                finally:
                    self._session_lock.release()

        except asyncio.TimeoutError:
            timer.stage_end("acquire_lock")
            route_logger.error(f"[{request_id}] Timeout waiting for session lock")
            raise FtRuntimeException(
                exception_type=ExceptionType.INTERNAL_ERROR,
                message="Session creation timeout"
            )

    def _should_recreate_session(self) -> bool:
        """判断是否需要重建 session"""
        if self._session_create_time is None:
            return True

        # 检查请求数量
        if self._session_request_count >= self._session_max_requests:
            route_logger.info(f"Session request count exceeded: {self._session_request_count}")
            return True

        # 检查存活时间
        age = time.time() - self._session_create_time
        if age >= self._session_max_age:
            route_logger.info(f"Session age exceeded: {age:.1f}s")
            return True

        return False

    async def _recreate_session(self):
        """重建 session"""
        if self._session and not self._session.closed:
            try:
                await asyncio.wait_for(self._session.close(), timeout=1.0)
            except Exception as e:
                route_logger.warning(f"Error closing old session: {e}")

        self._session = None
        await self._create_new_session("recreate")

    async def _create_new_session(self, request_id: str):
        """创建新的 session"""
        try:
            # 清理旧 session
            if self._session:
                try:
                    await asyncio.wait_for(self._session.close(), timeout=1.0)
                except Exception as e:
                    route_logger.warning(f"[{request_id}] Error closing old session: {e}")

            # 创建新的 connector 和 session
            connector = aiohttp.TCPConnector(**self.connector_config)
            self._session = aiohttp.ClientSession(
                timeout=self.timeout,
                connector=connector,
                raise_for_status=False  # 手动处理 HTTP 状态码
            )

            self._session_create_time = time.time()
            self._session_request_count = 0

            route_logger.info(f"[{request_id}] New session created")

        except Exception as e:
            route_logger.error(f"[{request_id}] Failed to create session: {e}")
            raise

    async def get_backend_role_addrs(
        self,
        master_addr: Optional[str],
        block_cache_keys: list[int],
        seq_len: int,
        debug: bool,
        generate_timeout: int,
        request_priority: int = 100,
    ) -> Tuple[Optional[List[RoleAddr]], int]:
        """获取后端角色地址，带详细监控"""

        request_id = str(uuid.uuid4())[:8]
        start_time = time.time()

        route_logger.debug(f"[{request_id}] Starting request to {master_addr}")

        timer = RequestTimer(request_id)
        timer.stage_start("validation")

        inter_request_id = -1

        # 参数验证
        if not master_addr:
            timer.stage_end("validation")
            route_logger.warning(f"[{request_id}] No master address provided")
            return None, inter_request_id

        timer.stage_end("validation")
        timer.stage_start("prepare_request")

        # 准备请求
        url = "http://" + master_addr + "/rtp_llm/schedule"
        payload = {
            "model": "engine_service",
            "block_cache_keys": block_cache_keys,
            "seq_len": seq_len,
            "debug": debug,
            "request_priority": request_priority,
        }

        if generate_timeout != -1:
            payload["generate_timeout"] = generate_timeout

        headers = {"Content-Type": "application/json"}
        data = json.dumps(payload)

        timer.stage_end("prepare_request")

        # 执行请求，带监控
        try:
            async with monitor_request(f"master_request_{request_id}", timeout=4.0):
                timer.stage_start("get_session")
                session = await self.get_session()
                timer.stage_end("get_session")

                timer.stage_start("http_request")
                self._session_request_count += 1

                async with session.post(url, data=data, headers=headers) as response:
                    timer.stage_end("http_request")
                    timer.stage_start("process_response")

                    # 检查 HTTP 状态
                    if response.status != 200:
                        timer.stage_end("process_response")
                        route_logger.error(
                            f"[{request_id}] HTTP error {response.status} from {master_addr}"
                        )
                        return None, inter_request_id

                    # 解析响应
                    try:
                        result = await response.json()
                        route_logger.info(f"[{request_id}] Response: {result}")
                    except Exception as e:
                        timer.stage_end("process_response")
                        route_logger.error(f"[{request_id}] JSON decode error: {e}")
                        return None, inter_request_id

                    timer.stage_end("process_response")

        except asyncio.TimeoutError:
            total_time = time.time() - start_time
            route_logger.error(f"[{request_id}] Request timeout after {total_time:.3f}s")
            self._record_slow_request(request_id, total_time, "timeout", timer.get_summary())
            raise FtRuntimeException(
                exception_type=ExceptionType.TIMEOUT,
                message=f"Master request timeout: {total_time:.3f}s"
            )

        except Exception as e:
            total_time = time.time() - start_time
            route_logger.error(f"[{request_id}] Request failed after {total_time:.3f}s: {e}")
            self._record_slow_request(request_id, total_time, f"error: {e}", timer.get_summary())
            return None, inter_request_id

        # 处理业务响应
        timer.stage_start("parse_business_response")

        try:
            schedule_meta = ScheduleMeta.model_validate(result)
            if schedule_meta.code != 200:
                timer.stage_end("parse_business_response")
                route_logger.error(f"[{request_id}] Master schedule error: {schedule_meta.code}")
                raise FtRuntimeException(
                    exception_type=ExceptionType(schedule_meta.code),
                    message="master schedule error",
                )

            # 解析角色地址
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

            timer.stage_end("parse_business_response")

            # 记录成功请求的统计
            total_time = time.time() - start_time
            self._record_request_stats(request_id, total_time, timer.get_summary())

            route_logger.debug(f"[{request_id}] Request completed in {total_time:.3f}s")

            return role_addrs, schedule_meta.inter_request_id

        except Exception as e:
            timer.stage_end("parse_business_response")
            total_time = time.time() - start_time
            route_logger.error(f"[{request_id}] Response parsing failed: {e}")
            self._record_slow_request(request_id, total_time, f"parse_error: {e}", timer.get_summary())
            raise

    def _record_request_stats(self, request_id: str, total_time: float, timer_summary: dict):
        """记录请求统计信息"""
        if not self.enable_monitoring:
            return

        self._request_stats[request_id] = {
            'total_time': total_time,
            'timestamp': time.time(),
            'timer_summary': timer_summary
        }

        # 如果请求时间过长，记录为慢请求
        if total_time > 1.0:  # 1 秒以上认为是慢请求
            self._record_slow_request(request_id, total_time, "slow", timer_summary)

        # 清理老的统计信息
        if len(self._request_stats) > 1000:
            # 保留最新的 500 个
            sorted_keys = sorted(
                self._request_stats.keys(),
                key=lambda k: self._request_stats[k]['timestamp']
            )
            for key in sorted_keys[:-500]:
                del self._request_stats[key]

    def _record_slow_request(self, request_id: str, total_time: float, reason: str, timer_summary: dict):
        """记录慢请求"""
        slow_request = {
            'request_id': request_id,
            'total_time': total_time,
            'reason': reason,
            'timestamp': time.time(),
            'timer_summary': timer_summary
        }

        self._slow_requests.append(slow_request)
        route_logger.warning(f"Slow request recorded: {slow_request}")

        # 限制慢请求记录数量
        if len(self._slow_requests) > 100:
            self._slow_requests = self._slow_requests[-50:]  # 保留最新的 50 个

    def get_stats(self) -> dict:
        """获取客户端统计信息"""
        recent_requests = list(self._request_stats.values())[-20:]  # 最近 20 个请求

        avg_time = 0
        if recent_requests:
            avg_time = sum(r['total_time'] for r in recent_requests) / len(recent_requests)

        return {
            'session_info': {
                'created_time': self._session_create_time,
                'request_count': self._session_request_count,
                'is_closed': self._session.closed if self._session else None,
            },
            'recent_performance': {
                'request_count': len(recent_requests),
                'avg_time': avg_time,
                'slow_requests': len(self._slow_requests),
            },
            'slow_requests': self._slow_requests[-5:] if self._slow_requests else [],  # 最近 5 个慢请求
        }

    async def close(self):
        """关闭客户端"""
        if self._session and not self._session.closed:
            await self._session.close()

        route_logger.info(f"Master client closed. Final stats: {self.get_stats()}")


# 为了兼容性，保持原类名
class MasterClient(EnhancedMasterClient):
    """兼容性别名"""
    pass