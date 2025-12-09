"""
AsyncIO 配置优化模块

用于配置 asyncio 的性能参数，避免长时间执行任务的警告
"""

import asyncio
import logging
import os
from typing import Optional


def configure_asyncio_performance(
    slow_callback_duration: float = 0.5,
    debug: Optional[bool] = None
) -> None:
    """
    配置 asyncio 的性能参数

    Args:
        slow_callback_duration: 慢回调警告阈值（秒）
        debug: 是否启用调试模式，None 表示使用环境变量
    """
    loop = asyncio.get_event_loop()

    # 设置慢回调警告阈值
    loop.slow_callback_duration = slow_callback_duration

    # 根据环境变量或参数设置调试模式
    if debug is None:
        debug = os.environ.get('PYTHONASYNCIODEBUG', '0') == '1'

    loop.set_debug(debug)

    if debug:
        logging.info(
            f"AsyncIO 调试模式已启用，慢回调阈值: {slow_callback_duration}秒"
        )
    else:
        logging.info(
            f"AsyncIO 性能优化已配置，慢回调阈值: {slow_callback_duration}秒"
        )


def setup_grpc_channel_options() -> list:
    """
    返回优化的 gRPC channel 配置选项

    Returns:
        gRPC channel 配置选项列表
    """
    return [
        # 基础配置
        ("grpc.max_metadata_size", 1024 * 1024 * 1024),

        # 连接保活配置
        ("grpc.keepalive_time_ms", 10000),  # 10秒发送一次keepalive ping
        ("grpc.keepalive_timeout_ms", 5000),  # 5秒等待ping响应
        ("grpc.keepalive_permit_without_calls", 1),  # 即使没有调用也发送ping

        # HTTP/2 配置
        ("grpc.http2.max_pings_without_data", 0),  # 不限制ping次数
        ("grpc.http2.min_time_between_pings_ms", 5000),  # ping之间最小间隔5秒
        ("grpc.http2.min_ping_interval_without_data_ms", 5000),  # 无数据时ping间隔5秒

        # 连接池配置
        ("grpc.use_local_subchannel_pool", 1),  # 使用本地子通道池
        ("grpc.max_connection_idle_ms", 300000),  # 连接空闲5分钟后关闭
        ("grpc.max_connection_age_ms", 3600000),  # 连接最大存活1小时

        # 重试配置
        ("grpc.enable_retries", 1),
        ("grpc.max_retry_attempts", 3),
    ]