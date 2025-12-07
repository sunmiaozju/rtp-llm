import json
import logging
from typing import List, Optional, Tuple

import requests

from rtp_llm.config.exceptions import ExceptionType, FtRuntimeException
from rtp_llm.config.generate_config import RoleAddr, RoleType
from rtp_llm.server.worker_status import ScheduleMeta

route_logger = logging.getLogger("route_logger")


class MasterClient:
    def __init__(self):
        pass

    def get_backend_role_addrs(
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

        # connect to master using new session for each request (no connection pooling)
        try:
            # 每次请求都创建新的session，完全禁用连接复用
            with requests.Session() as session:
                response = session.post(
                    url,
                    data=json.dumps(payload),
                    headers=headers,
                    timeout=0.5  # 设置超时时间
                )
                if response.status_code != 200:
                    route_logger.error(
                        f"Failed to get master response from {master_addr}, http status: {response.status_code}"
                    )
                    return None, inter_request_id
                result = response.json()
        except Exception as e:
            route_logger.error(f"Failed to query to master at {master_addr}: {type(e).__name__}: {e}")
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
