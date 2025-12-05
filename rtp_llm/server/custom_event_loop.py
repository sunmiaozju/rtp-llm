"""
自定义事件循环实现，用于隔离和监控 aiohttp 请求
"""
import asyncio
import threading
import logging
import time
import queue
import concurrent.futures
from typing import Optional, Dict, Any, Callable, Awaitable
from contextlib import asynccontextmanager

logger = logging.getLogger(__name__)


class IsolatedEventLoop:
    """隔离的事件循环，专门用于 HTTP 请求，避免被其他任务阻塞"""

    def __init__(self, thread_name: str = "http_client_loop"):
        self.thread_name = thread_name
        self.loop: Optional[asyncio.AbstractEventLoop] = None
        self.thread: Optional[threading.Thread] = None
        self.running = False

        # 用于线程间通信的队列
        self.result_queue = queue.Queue()

        # 性能统计
        self.stats = {
            'tasks_submitted': 0,
            'tasks_completed': 0,
            'tasks_failed': 0,
            'avg_execution_time': 0.0,
            'max_execution_time': 0.0,
            'loop_start_time': None
        }

    def start(self):
        """启动隔离的事件循环线程"""
        if self.running:
            return

        self.running = True
        self.thread = threading.Thread(target=self._run_loop, name=self.thread_name, daemon=True)
        self.thread.start()

        # 等待事件循环启动
        start_time = time.time()
        while self.loop is None and time.time() - start_time < 5.0:
            time.sleep(0.01)

        if self.loop is None:
            raise RuntimeError("Failed to start isolated event loop")

        logger.info(f"Isolated event loop '{self.thread_name}' started")

    def stop(self):
        """停止事件循环"""
        if not self.running or self.loop is None:
            return

        # 在事件循环中停止
        self.loop.call_soon_threadsafe(self._stop_loop)

        # 等待线程结束
        if self.thread:
            self.thread.join(timeout=5.0)

        self.running = False
        logger.info(f"Isolated event loop '{self.thread_name}' stopped")

    def _run_loop(self):
        """在独立线程中运行事件循环"""
        try:
            # 创建新的事件循环
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)

            self.stats['loop_start_time'] = time.time()

            # 启动监控任务
            self.loop.create_task(self._monitor_loop())

            logger.info(f"Event loop thread '{self.thread_name}' running")
            self.loop.run_forever()

        except Exception as e:
            logger.error(f"Event loop thread '{self.thread_name}' error: {e}")
        finally:
            # 清理
            if self.loop:
                try:
                    # 取消所有剩余任务
                    pending = asyncio.all_tasks(self.loop)
                    for task in pending:
                        task.cancel()

                    if pending:
                        self.loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
                finally:
                    self.loop.close()

    def _stop_loop(self):
        """停止事件循环"""
        if self.loop and self.loop.is_running():
            self.loop.stop()

    async def _monitor_loop(self):
        """监控事件循环性能"""
        while self.running:
            await asyncio.sleep(10)  # 每10秒统计一次

            all_tasks = asyncio.all_tasks(self.loop)
            logger.debug(f"Loop '{self.thread_name}' has {len(all_tasks)} active tasks")

            # 统计信息
            uptime = time.time() - self.stats['loop_start_time']
            logger.debug(f"Loop '{self.thread_name}' uptime: {uptime:.1f}s, stats: {self.stats}")

    def run_coroutine(self, coro: Awaitable, timeout: float = 10.0) -> Any:
        """在隔离的事件循环中运行协程"""
        if not self.running or self.loop is None:
            raise RuntimeError("Isolated event loop not running")

        start_time = time.time()
        self.stats['tasks_submitted'] += 1

        # 创建 Future 用于跨线程通信
        future = concurrent.futures.Future()

        async def _wrapped_coro():
            try:
                # 添加超时保护
                result = await asyncio.wait_for(coro, timeout=timeout)
                future.set_result(result)
            except Exception as e:
                future.set_exception(e)

        # 在事件循环中调度任务
        self.loop.call_soon_threadsafe(
            lambda: self.loop.create_task(_wrapped_coro())
        )

        try:
            # 等待结果，添加额外的超时保护
            result = future.result(timeout=timeout + 1.0)

            execution_time = time.time() - start_time
            self._update_stats(execution_time, success=True)

            return result

        except Exception as e:
            execution_time = time.time() - start_time
            self._update_stats(execution_time, success=False)

            logger.error(f"Task failed in isolated loop: {e}")
            raise

    def _update_stats(self, execution_time: float, success: bool):
        """更新性能统计"""
        if success:
            self.stats['tasks_completed'] += 1
        else:
            self.stats['tasks_failed'] += 1

        # 更新执行时间统计
        completed = self.stats['tasks_completed']
        if completed > 0:
            current_avg = self.stats['avg_execution_time']
            self.stats['avg_execution_time'] = (current_avg * (completed - 1) + execution_time) / completed

        self.stats['max_execution_time'] = max(self.stats['max_execution_time'], execution_time)

    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        stats = self.stats.copy()
        if self.loop:
            all_tasks = asyncio.run_coroutine_threadsafe(
                asyncio.gather(*[t for t in asyncio.all_tasks(self.loop)], return_exceptions=True),
                self.loop
            )
            try:
                tasks = all_tasks.result(timeout=1.0)
                stats['active_tasks'] = len([t for t in tasks if not isinstance(t, Exception)])
            except:
                stats['active_tasks'] = 'unknown'

        return stats


class IsolatedHTTPClient:
    """使用隔离事件循环的 HTTP 客户端"""

    def __init__(self, loop_name: str = "http_client"):
        self.isolated_loop = IsolatedEventLoop(loop_name)
        self._session = None
        self._connector_config = {
            'limit': 20,
            'limit_per_host': 5,
            'keepalive_timeout': 300,
            'force_close': False,
            'enable_cleanup_closed': True,
            'use_dns_cache': True,
            'ttl_dns_cache': 300,
        }

    def start(self):
        """启动客户端"""
        self.isolated_loop.start()

        # 在隔离循环中创建 session
        async def _create_session():
            import aiohttp
            connector = aiohttp.TCPConnector(**self._connector_config)
            timeout = aiohttp.ClientTimeout(total=5.0, connect=2.0)
            self._session = aiohttp.ClientSession(
                connector=connector,
                timeout=timeout
            )
            return True

        self.isolated_loop.run_coroutine(_create_session())
        logger.info("Isolated HTTP client started")

    def stop(self):
        """停止客户端"""
        if self._session:
            async def _close_session():
                if not self._session.closed:
                    await self._session.close()
                return True

            try:
                self.isolated_loop.run_coroutine(_close_session(), timeout=2.0)
            except Exception as e:
                logger.warning(f"Error closing session: {e}")

        self.isolated_loop.stop()
        logger.info("Isolated HTTP client stopped")

    def request(self, method: str, url: str, timeout: float = 5.0, **kwargs) -> Any:
        """执行 HTTP 请求"""
        if not self._session:
            raise RuntimeError("HTTP client not started")

        async def _make_request():
            async with self._session.request(method, url, **kwargs) as response:
                return {
                    'status': response.status,
                    'headers': dict(response.headers),
                    'content': await response.text()
                }

        return self.isolated_loop.run_coroutine(_make_request(), timeout=timeout)

    def get_stats(self) -> Dict[str, Any]:
        """获取客户端统计信息"""
        return {
            'loop_stats': self.isolated_loop.get_stats(),
            'session_closed': self._session.closed if self._session else True
        }


# 全局实例管理
_global_isolated_client: Optional[IsolatedHTTPClient] = None


def get_isolated_http_client() -> IsolatedHTTPClient:
    """获取全局隔离的 HTTP 客户端"""
    global _global_isolated_client

    if _global_isolated_client is None:
        _global_isolated_client = IsolatedHTTPClient("master_client_loop")
        _global_isolated_client.start()

    return _global_isolated_client


@asynccontextmanager
async def isolated_http_context():
    """上下文管理器，确保隔离的 HTTP 客户端正确启动和清理"""
    client = get_isolated_http_client()
    try:
        yield client
    except Exception as e:
        logger.error(f"Error in isolated HTTP context: {e}")
        raise
    # 注意：这里不关闭全局客户端，因为可能被其他地方使用


# 兼容性函数
def run_in_isolated_loop(coro: Awaitable, timeout: float = 10.0) -> Any:
    """在隔离的事件循环中运行协程"""
    client = get_isolated_http_client()
    return client.isolated_loop.run_coroutine(coro, timeout)


def cleanup_isolated_client():
    """清理全局隔离客户端"""
    global _global_isolated_client

    if _global_isolated_client:
        _global_isolated_client.stop()
        _global_isolated_client = None