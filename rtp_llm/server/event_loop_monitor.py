"""
事件循环监控工具，用于诊断 aiohttp 卡死问题
"""
import asyncio
import time
import threading
import logging
from typing import Optional, Dict, Any
from contextlib import asynccontextmanager
import traceback
import weakref

logger = logging.getLogger(__name__)


class EventLoopMonitor:
    """事件循环监控器，用于检测阻塞和性能问题"""

    def __init__(self, loop: Optional[asyncio.AbstractEventLoop] = None):
        self.loop = loop or asyncio.get_event_loop()
        self.monitoring = False
        self.monitor_thread = None
        self.last_tick = time.time()
        self.blocked_threshold = 0.1  # 100ms 认为是阻塞
        self.stats = {
            'total_blocks': 0,
            'max_block_time': 0,
            'current_tasks': 0,
            'pending_callbacks': 0
        }
        self._task_tracker = weakref.WeakSet()

    def start_monitoring(self):
        """开始监控事件循环"""
        if self.monitoring:
            return

        self.monitoring = True
        self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.monitor_thread.start()

        # 在事件循环中设置心跳
        if self.loop.is_running():
            self.loop.call_soon(self._heartbeat)
        else:
            self.loop.call_later(0, self._heartbeat)

        logger.info("Event loop monitoring started")

    def stop_monitoring(self):
        """停止监控"""
        self.monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=1)
        logger.info("Event loop monitoring stopped")

    def _heartbeat(self):
        """事件循环心跳，更新最后活跃时间"""
        if not self.monitoring:
            return

        self.last_tick = time.time()

        # 统计当前任务数
        try:
            all_tasks = asyncio.all_tasks(self.loop)
            self.stats['current_tasks'] = len(all_tasks)

            # 统计待处理的回调数量
            # 这个需要访问私有属性，仅用于调试
            if hasattr(self.loop, '_ready'):
                self.stats['pending_callbacks'] = len(self.loop._ready)
        except Exception as e:
            logger.debug(f"Error collecting stats: {e}")

        # 继续心跳
        self.loop.call_later(0.05, self._heartbeat)  # 50ms 间隔

    def _monitor_loop(self):
        """在单独线程中监控事件循环状态"""
        while self.monitoring:
            time.sleep(0.1)  # 100ms 检查一次

            current_time = time.time()
            time_since_tick = current_time - self.last_tick

            if time_since_tick > self.blocked_threshold:
                self.stats['total_blocks'] += 1
                self.stats['max_block_time'] = max(self.stats['max_block_time'], time_since_tick)

                logger.warning(
                    f"Event loop blocked for {time_since_tick:.3f}s, "
                    f"current_tasks: {self.stats['current_tasks']}, "
                    f"pending_callbacks: {self.stats['pending_callbacks']}"
                )

                # 如果阻塞时间超过 1 秒，打印所有任务的堆栈
                if time_since_tick > 1.0:
                    self._dump_tasks()

    def _dump_tasks(self):
        """打印所有正在运行的任务信息"""
        try:
            all_tasks = asyncio.all_tasks(self.loop)
            logger.error(f"Dumping {len(all_tasks)} tasks:")

            for i, task in enumerate(all_tasks):
                stack = task.get_stack()
                if stack:
                    logger.error(f"Task {i}: {task}")
                    for frame in stack:
                        logger.error(f"  {frame.filename}:{frame.lineno} in {frame.name}")
        except Exception as e:
            logger.error(f"Error dumping tasks: {e}")

    def get_stats(self) -> Dict[str, Any]:
        """获取监控统计信息"""
        return self.stats.copy()


@asynccontextmanager
async def monitor_request(operation_name: str, timeout: float = 10.0):
    """监控单个请求的执行时间和状态"""
    start_time = time.time()
    task_name = f"{operation_name}_{id(asyncio.current_task())}"

    logger.debug(f"[{task_name}] Starting operation: {operation_name}")

    try:
        # 简单的监控，不设置超时（超时由调用者处理）
        yield

        duration = time.time() - start_time
        logger.debug(f"[{task_name}] Completed in {duration:.3f}s")

    except asyncio.TimeoutError:
        duration = time.time() - start_time
        logger.error(f"[{task_name}] Timeout after {duration:.3f}s")
        # 记录当前任务状态用于调试
        monitor = get_global_monitor()
        if monitor:
            try:
                await monitor.dump_current_tasks()
            except Exception as dump_e:
                logger.warning(f"Failed to dump tasks: {dump_e}")
        raise

    except Exception as e:
        duration = time.time() - start_time
        logger.error(f"[{task_name}] Failed after {duration:.3f}s: {e}")
        # 记录当前任务状态用于调试
        monitor = get_global_monitor()
        if monitor:
            try:
                await monitor.dump_current_tasks()
            except Exception as dump_e:
                logger.warning(f"Failed to dump tasks: {dump_e}")
        raise


class RequestTimer:
    """请求计时器，用于监控各个阶段的耗时"""

    def __init__(self, request_id: str):
        self.request_id = request_id
        self.stages = {}
        self.start_time = time.time()

    def stage_start(self, stage_name: str):
        """开始一个阶段"""
        self.stages[stage_name] = {'start': time.time()}
        logger.debug(f"[{self.request_id}] Stage '{stage_name}' started")

    def stage_end(self, stage_name: str):
        """结束一个阶段"""
        if stage_name in self.stages:
            end_time = time.time()
            duration = end_time - self.stages[stage_name]['start']
            self.stages[stage_name]['duration'] = duration

            logger.debug(f"[{self.request_id}] Stage '{stage_name}' completed in {duration:.3f}s")

            # 如果某个阶段超过 1 秒，记录警告
            if duration > 1.0:
                logger.warning(f"[{self.request_id}] Slow stage '{stage_name}': {duration:.3f}s")

    def get_summary(self) -> Dict[str, Any]:
        """获取完整的时间统计"""
        total_time = time.time() - self.start_time
        return {
            'request_id': self.request_id,
            'total_time': total_time,
            'stages': {k: v.get('duration', 'incomplete') for k, v in self.stages.items()}
        }


# 全局监控器实例
_global_monitor: Optional[EventLoopMonitor] = None


def get_global_monitor() -> EventLoopMonitor:
    """获取全局事件循环监控器"""
    global _global_monitor

    if _global_monitor is None:
        _global_monitor = EventLoopMonitor()
        _global_monitor.start_monitoring()

    return _global_monitor


def init_monitoring():
    """初始化事件循环监控"""
    monitor = get_global_monitor()
    logger.info(f"Event loop monitoring initialized, stats: {monitor.get_stats()}")
    return monitor