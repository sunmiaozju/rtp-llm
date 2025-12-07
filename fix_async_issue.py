"""
修复异步请求3秒延迟的解决方案
"""

import asyncio
import aiohttp
import socket
from typing import Optional, Dict, Any
import logging

logger = logging.getLogger(__name__)


class FixedMasterClient:
    """修复后的 MasterClient - 解决3秒延迟问题"""

    def __init__(self):
        # 不存储任何异步对象
        self._sessions: Dict[int, aiohttp.ClientSession] = {}

    async def _get_or_create_session(self) -> aiohttp.ClientSession:
        """
        确保在当前事件循环中获取或创建 session
        关键：使用 get_running_loop() 而不是 get_event_loop()
        """
        # 获取当前正在运行的事件循环
        loop = asyncio.get_running_loop()
        loop_id = id(loop)

        # 如果当前循环已有 session，直接返回
        if loop_id in self._sessions and not self._sessions[loop_id].closed:
            return self._sessions[loop_id]

        # 创建新的 session
        logger.info(f"Creating new session for event loop {loop_id}")

        # 关键优化点：
        connector = aiohttp.TCPConnector(
            # 1. 强制 IPv4 - 避免 IPv6 回退造成的延迟
            family=socket.AF_INET,

            # 2. 启用 DNS 缓存
            use_dns_cache=True,
            ttl_dns_cache=300,  # 5分钟缓存

            # 3. 连接池配置
            limit=100,  # 总连接数
            limit_per_host=30,  # 每个主机的连接数

            # 4. Keep-alive 配置
            keepalive_timeout=30,
            force_close=False,  # 不强制关闭连接

            # 5. 清理配置
            enable_cleanup_closed=True,

            # 6. 禁用 SSL（如果不需要 HTTPS）
            ssl=False,
        )

        # 创建 session
        session = aiohttp.ClientSession(
            connector=connector,
            # 设置合理的超时
            timeout=aiohttp.ClientTimeout(
                total=1.0,  # 总超时
                connect=0.3,  # 连接超时
                sock_connect=0.2,  # socket 连接超时
                sock_read=0.5,  # 读取超时
            ),
            # 跳过自动 headers 以提升性能
            skip_auto_headers={'User-Agent'},
        )

        self._sessions[loop_id] = session
        return session

    async def request(self, url: str, payload: Dict[str, Any]) -> Optional[Dict]:
        """执行异步请求"""
        # 关键：获取当前事件循环的 session
        session = await self._get_or_create_session()

        try:
            async with session.post(
                url,
                json=payload,
                # 针对单个请求的超时可以更短
                timeout=aiohttp.ClientTimeout(
                    total=0.8,
                    connect=0.2,
                    sock_read=0.4,
                ),
                headers={
                    "Connection": "keep-alive",
                    "Content-Type": "application/json",
                },
            ) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    logger.error(f"Request failed with status: {response.status}")
                    return None

        except asyncio.TimeoutError:
            logger.error("Request timed out")
            return None
        except Exception as e:
            logger.error(f"Request failed: {type(e).__name__}: {e}")
            return None

    async def close(self):
        """关闭所有 sessions"""
        for session in self._sessions.values():
            if not session.closed:
                await session.close()
        self._sessions.clear()


# 使用示例
async def test_fixed_client():
    """测试修复后的客户端"""
    client = FixedMasterClient()

    # 模拟多个请求
    tasks = []
    for i in range(5):
        task = client.request(
            "http://your_server/api",
            {"test": i}
        )
        tasks.append(task)

    # 并发执行
    import time
    start = time.time()
    results = await asyncio.gather(*tasks)
    end = time.time()

    print(f"5个请求总耗时: {end - start:.3f}秒")
    print(f"平均每个请求: {(end - start) / 5:.3f}秒")

    # 清理
    await client.close()


# 快速修复方案 - 可以直接应用到您的代码
def get_quick_fix_patch():
    """
    返回可以直接应用到您现有代码的补丁
    """
    return '''
# 在 master_client.py 中修改 _ensure_session 方法：

async def _ensure_session(self):
    """确保 session 已经初始化"""
    if self.session is None or self.session.closed:
        # 关键修改：使用 get_running_loop() 而不是 get_event_loop()
        loop = asyncio.get_running_loop()

        # 删除 debug 模式设置（会影响性能）
        # loop.set_debug(True)  # 删除这行

        # 优化连接器配置
        self.connector = aiohttp.TCPConnector(
            family=socket.AF_INET,
            use_dns_cache=True,
            ttl_dns_cache=300,  # 增加到5分钟
            limit=100,  # 增加总连接数
            limit_per_host=30,  # 增加每主机连接数
            keepalive_timeout=30,  # 增加 keep-alive 时间
            force_close=False,  # 添加这个配置
            enable_cleanup_closed=True,
            ssl=False
        )

        # 创建 session
        self.session = aiohttp.ClientSession(
            connector=self.connector,
            timeout=aiohttp.ClientTimeout(
                total=1.0,
                connect=0.3,
                sock_connect=0.2,
                sock_read=0.5
            )
        )
    '''


if __name__ == "__main__":
    # 运行测试
    asyncio.run(test_fixed_client())

    # 打印快速修复方案
    print("\n" + "="*60)
    print("快速修复方案：")
    print("="*60)
    print(get_quick_fix_patch())