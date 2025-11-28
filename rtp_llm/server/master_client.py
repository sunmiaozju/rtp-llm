import json
import logging
import time
from typing import List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter

from rtp_llm.config.exceptions import ExceptionType, FtRuntimeException
from rtp_llm.config.generate_config import RoleAddr, RoleType
from rtp_llm.server.worker_status import ScheduleMeta
from rtp_llm.utils.viztracer_util import trace_func

route_logger = logging.getLogger("route_logger")

def _create_client() -> requests.Session:
    """创建HTTP客户端实例"""
    session = requests.Session()

    # 连接池配置参数
    pool_connections = 20  # 支持的不同主机连接池数量
    pool_maxsize = 30     # 每个连接池的最大连接数

    # 显式配置连接池参数
    adapter = HTTPAdapter(
        pool_connections=pool_connections,
        pool_maxsize=pool_maxsize,
        max_retries=0,          # 不自动重试
        pool_block=False        # 连接池满时立即失败而不是等待
    )

    # 应用到所有HTTP和HTTPS请求
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    # requests库的超时设置为 (连接超时, 读取超时) 的元组
    session.timeout = (0.1, 0.4)  # (连接超时, 读取超时)

    # 记录连接池配置
    route_logger.info(f"HTTP客户端初始化完成 - 连接池配置: connections={pool_connections}, maxsize={pool_maxsize}")

    return session


class MasterClient:
    def __init__(self):
        self._client = _create_client()

    def _get_client(self):
        """获取HTTP client"""
        return self._client

    def close(self):
        """关闭HTTP client"""
        if self._client:
            self._client.close()
            self._client = None

    @trace_func(
        name="inference",
        min_duration_ms=100,
    )
    def get_backend_role_addrs(
        self,
        master_addr: Optional[str],
        block_cache_keys: list[int],
        seq_len: int,
        debug: bool,
        generate_timeout: int,
        request_priority: int = 100,
    ) -> Tuple[Optional[List[RoleAddr]], int]:
        method_start_time = time.time()
        route_logger.info(f"[TIMING] Starting get_backend_role_addrs for master: {master_addr}")
        
        inter_request_id = -1
        # get master address
        if not master_addr:
            route_logger.info(f"[TIMING] No master_addr provided, returning early")
            return None, inter_request_id
            
        # prepare request to master
        prepare_start = time.time()
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
        prepare_time = (time.time() - prepare_start) * 1000
        route_logger.info(f"[TIMING] Payload preparation took: {prepare_time:.2f}ms")

        # connect to master using requests
        http_start_time = time.time()
        route_logger.info(f"[TIMING] Starting HTTP request to: {url}")
        
        try:
            client_start = time.time()
            client = self._get_client()
            client_time = (time.time() - client_start) * 1000
            route_logger.info(f"[TIMING] Get client took: {client_time:.2f}ms")

            request_start = time.time()
            # requests库使用 (连接超时, 读取超时) 元组
            timeout = (0.1, 0.3)  # (连接超时, 读取超时)
            response = client.post(
                url,
                data=json.dumps(payload),
                headers=headers,
                timeout=timeout
            )
            request_time = (time.time() - request_start) * 1000
            route_logger.info(f"[TIMING] HTTP POST request took: {request_time:.2f}ms, status: {response.status_code}")
            
            if response.status_code != 200:
                route_logger.error(
                    f"Failed to get master response from {master_addr}, http status: {response.status_code}"
                )
                return None, inter_request_id
                
            json_start = time.time()
            result = response.json()
            json_time = (time.time() - json_start) * 1000
            route_logger.info(f"[TIMING] JSON parsing took: {json_time:.2f}ms")
            
        except Exception as e:
            rt_ms = (time.time() - http_start_time) * 1000
            if isinstance(e, requests.ConnectTimeout):
                route_logger.error(f"Connect timeout to master at {master_addr}: {e}, RT: {rt_ms:.2f}ms")
            elif isinstance(e, requests.ReadTimeout):
                route_logger.error(f"Read timeout from master at {master_addr}: {e}, RT: {rt_ms:.2f}ms")
            elif isinstance(e, requests.Timeout):
                route_logger.error(f"General timeout to master at {master_addr}: {e}, RT: {rt_ms:.2f}ms")
            elif isinstance(e, requests.ConnectionError):
                route_logger.error(f"Connection error to master at {master_addr}: {e}, RT: {rt_ms:.2f}ms")
            else:
                route_logger.error(f"Failed to query master at {master_addr}: {type(e).__name__}: {e}, RT: {rt_ms:.2f}ms")
            return None, inter_request_id

        # check response
        validate_start = time.time()
        schedule_meta = ScheduleMeta.model_validate(result)
        validate_time = (time.time() - validate_start) * 1000
        route_logger.info(f"[TIMING] Response validation took: {validate_time:.2f}ms")
        
        if schedule_meta.code != 200:
            route_logger.error(
                f"Master schedule error, error code: {schedule_meta.code}"
            )
            raise FtRuntimeException(
                exception_type=ExceptionType(schedule_meta.code),
                message="master schedule error",
            )

        # parse role ips from schedule meta
        parse_start = time.time()
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
        parse_time = (time.time() - parse_start) * 1000
        route_logger.info(f"[TIMING] Role parsing took: {parse_time:.2f}ms, parsed {len(role_addrs)} roles")

        total_time = (time.time() - method_start_time) * 1000
        route_logger.info(f"[TIMING] Total get_backend_role_addrs took: {total_time:.2f}ms, inter_request_id: {schedule_meta.inter_request_id}")
        
        return role_addrs, schedule_meta.inter_request_id
