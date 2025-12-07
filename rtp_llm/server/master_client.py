import asyncio
import logging
import socket
from typing import List, Optional, Tuple
import json
import os
import atexit
import time
from concurrent.futures import ThreadPoolExecutor
import threading
import requests

from rtp_llm.config.exceptions import ExceptionType, FtRuntimeException
from rtp_llm.config.generate_config import RoleAddr, RoleType
from rtp_llm.server.worker_status import ScheduleMeta

route_logger = logging.getLogger("route_logger")


def slow_callback_detector(loop, context):
    """检测慢回调"""
    print(f"Slow callback detected: {context}")

class MasterClient:
    def __init__(self):
        # 线程池配置
        self.executor = ThreadPoolExecutor(
            max_workers=20,  # 可以根据需要调整线程数
            thread_name_prefix="MasterClient-HTTP-"
        )

        # 创建一个线程安全的session池
        self._session_pool = []
        self._pool_lock = threading.Lock()
        self._pool_size = 5  # 每个线程池维护5个session

        # 初始化session池
        self._init_session_pool()

        # 进程ID追踪
        self._process_id = os.getpid()
        # 注册清理函数
        atexit.register(self._cleanup_on_exit)

    def _init_session_pool(self):
        """初始化session池"""
        for _ in range(self._pool_size):
            session = self._create_session()
            self._session_pool.append(session)

    def _create_session(self):
        """创建一个配置好的requests session"""
        session = requests.Session()

        # 配置连接池
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=10,  # 连接池中的连接数
            pool_maxsize=30,      # 连接池的最大连接数
            max_retries=0,        # 不自动重试
            pool_block=False      # 连接池满时不阻塞
        )

        session.mount('http://', adapter)
        session.mount('https://', adapter)

        # 设置默认headers
        session.headers.update({
            'Connection': 'keep-alive',
            'Accept': 'application/json'
        })

        return session

    def _get_session(self):
        """从池中获取一个session"""
        with self._pool_lock:
            if self._session_pool:
                return self._session_pool.pop()
            else:
                # 池为空时创建新的session
                return self._create_session()

    def _return_session(self, session):
        """将session返回池中"""
        with self._pool_lock:
            if len(self._session_pool) < self._pool_size:
                self._session_pool.append(session)
            else:
                # 池满了就关闭session
                session.close()

    def _cleanup_on_exit(self):
        """进程退出时的清理函数（多进程环境）"""
        try:
            # 清理session池
            with self._pool_lock:
                for session in self._session_pool:
                    try:
                        session.close()
                    except:
                        pass
                self._session_pool.clear()

            # 关闭线程池
            self.executor.shutdown(wait=False)
        except:
            pass

    def _make_http_request(self, url: str, payload: dict, timeout: float) -> Tuple[Optional[dict], str]:
        """在线程中执行同步HTTP请求"""
        session = self._get_session()
        try:
            # 发起同步请求
            response = session.post(
                url,
                json=payload,
                timeout=timeout,
                headers={'Content-Type': 'application/json'},
                allow_redirects=False
            )

            if response.status_code != 200:
                error_msg = f"HTTP status: {response.status_code}"
                return None, error_msg

            # 解析响应
            result = response.json()
            return result, ""

        except requests.exceptions.Timeout:
            return None, "Request timeout"
        except requests.exceptions.ConnectionError as e:
            return None, f"Connection error: {str(e)}"
        except Exception as e:
            return None, f"{type(e).__name__}: {str(e)}"
        finally:
            # 确保session返回池中
            self._return_session(session)

    def close(self):
        """同步关闭方法"""
        # 关闭所有session
        with self._pool_lock:
            for session in self._session_pool:
                session.close()
            self._session_pool.clear()

        # 关闭线程池
        self.executor.shutdown(wait=True)

    async def aclose(self):
        """异步关闭方法，兼容原有接口"""
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self.close)

    async def get_backend_role_addrs(
        self,
        master_addr: Optional[str],
        block_cache_keys: list[int],
        seq_len: int,
        debug: bool,
        generate_timeout: int,
        request_priority: int = 100,
    ) -> Tuple[Optional[List[RoleAddr]], int]:
        """异步接口，内部使用线程池执行同步请求"""

        # 设置当前任务名称
        current_task = asyncio.current_task()
        if current_task:
            task_name = f"MasterClient-Sync-{self._process_id}-{int(time.time()*1000)}"
            current_task.set_name(task_name)
            route_logger.debug(f"Starting HTTP request with task name: {task_name}")

        inter_request_id = -1

        # 检查master地址
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
            # 获取当前事件循环
            loop = asyncio.get_running_loop()
            loop.set_debug(True)
            # 配置日志
            logging.basicConfig(level=logging.DEBUG)
            logging.getLogger('asyncio').setLevel(logging.DEBUG)
            loop.slow_callback_duration = 0.1  # 100ms
            loop.set_exception_handler(slow_callback_detector)

            # 在线程池中执行同步请求
            result, error_msg = await loop.run_in_executor(
                self.executor,
                self._make_http_request,
                url,
                payload,
                0.8  # 800ms超时
            )

            if error_msg:
                route_logger.error(
                    f"Failed to get master response from {master_addr}: {error_msg}"
                )
                return None, inter_request_id

            if result is None:
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
            route_logger.error(
                f"Master schedule error, error code: {schedule_meta.code}"
            )
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

        return role_addrs, schedule_meta.inter_request_id

    def __del__(self):
        """析构函数，确保资源被释放"""
        try:
            # 清理session池
            with self._pool_lock:
                for session in self._session_pool:
                    try:
                        session.close()
                    except:
                        pass
                self._session_pool.clear()

            # 关闭线程池
            self.executor.shutdown(wait=False)
        except:
            pass
