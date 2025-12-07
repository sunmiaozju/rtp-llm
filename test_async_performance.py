#!/usr/bin/env python3
"""
测试脚本：对比同步和异步请求的性能
"""

import asyncio
import time
import logging
from concurrent.futures import ThreadPoolExecutor
import requests
import aiohttp

# 配置日志
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# 测试配置
TEST_URL = "http://your_master_addr/rtp_llm/schedule"  # 替换为您的实际地址
TEST_PAYLOAD = {
    "model": "engine_service",
    "block_cache_keys": [1, 2, 3],
    "seq_len": 100,
    "debug": True,
    "request_priority": 100,
}
REQUESTS_COUNT = 10


def sync_request():
    """同步请求测试"""
    start = time.time()
    response = requests.post(TEST_URL, json=TEST_PAYLOAD, timeout=1.0)
    end = time.time()
    return end - start, response.status_code


async def async_request_problem():
    """模拟有问题的异步请求（在错误的事件循环中创建 session）"""
    # 这是错误的做法：在函数外部创建 session
    connector = aiohttp.TCPConnector(limit=10)
    session = aiohttp.ClientSession(connector=connector)

    try:
        start = time.time()
        async with session.post(TEST_URL, json=TEST_PAYLOAD, timeout=aiohttp.ClientTimeout(total=1.0)) as response:
            status = response.status
        end = time.time()
        return end - start, status
    finally:
        await session.close()


async def async_request_optimized():
    """优化后的异步请求"""
    # 正确做法：在当前事件循环中创建 session
    connector = aiohttp.TCPConnector(
        family=socket.AF_INET,
        use_dns_cache=True,
        limit=10,
        ssl=False,
    )

    async with aiohttp.ClientSession(
        connector=connector,
        timeout=aiohttp.ClientTimeout(
            total=0.8,
            connect=0.2,
            sock_connect=0.15,
            sock_read=0.3,
        )
    ) as session:
        start = time.time()
        async with session.post(
            TEST_URL,
            json=TEST_PAYLOAD,
            headers={"Connection": "keep-alive"}
        ) as response:
            status = response.status
        end = time.time()
        return end - start, status


async def test_with_shared_session():
    """使用共享 session 的优化版本"""
    # 创建一个在当前事件循环中的 session
    connector = aiohttp.TCPConnector(
        family=socket.AF_INET,
        use_dns_cache=True,
        limit=50,
        limit_per_host=30,
        keepalive_timeout=30,
        ssl=False,
    )

    session = aiohttp.ClientSession(
        connector=connector,
        timeout=aiohttp.ClientTimeout(total=1.0)
    )

    async def single_request():
        start = time.time()
        async with session.post(
            TEST_URL,
            json=TEST_PAYLOAD,
            headers={"Connection": "keep-alive"}
        ) as response:
            status = response.status
        end = time.time()
        return end - start, status

    try:
        # 并发执行多个请求
        tasks = [single_request() for _ in range(REQUESTS_COUNT)]
        results = await asyncio.gather(*tasks)
        return results
    finally:
        await session.close()


def main():
    print("="*60)
    print("性能测试：同步 vs 异步请求")
    print("="*60)

    # 1. 测试同步请求
    print("\n1. 测试同步请求:")
    sync_times = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(sync_request) for _ in range(REQUESTS_COUNT)]
        for future in futures:
            duration, status = future.result()
            sync_times.append(duration)
            print(f"   同步请求耗时: {duration:.3f}秒, 状态: {status}")

    # 2. 测试有问题的异步请求
    print("\n2. 测试有问题的异步请求（可能会看到3秒延迟）:")
    async def run_problem_async():
        tasks = [async_request_problem() for _ in range(REQUESTS_COUNT)]
        return await asyncio.gather(*tasks)

    problem_results = asyncio.run(run_problem_async())
    for duration, status in problem_results:
        print(f"   问题异步请求耗时: {duration:.3f}秒, 状态: {status}")

    # 3. 测试优化后的异步请求
    print("\n3. 测试优化后的异步请求:")
    async def run_optimized_async():
        tasks = [async_request_optimized() for _ in range(REQUESTS_COUNT)]
        return await asyncio.gather(*tasks)

    optimized_results = asyncio.run(run_optimized_async())
    for duration, status in optimized_results:
        print(f"   优化异步请求耗时: {duration:.3f}秒, 状态: {status}")

    # 4. 测试共享 session 的版本
    print("\n4. 测试共享 session 的优化版本:")
    shared_results = asyncio.run(test_with_shared_session())
    for duration, status in shared_results:
        print(f"   共享session请求耗时: {duration:.3f}秒, 状态: {status}")

    # 汇总统计
    print("\n" + "="*60)
    print("汇总统计:")
    print(f"同步请求平均耗时: {sum(sync_times)/len(sync_times):.3f}秒")
    print(f"问题异步请求平均耗时: {sum(d for d,_ in problem_results)/len(problem_results):.3f}秒")
    print(f"优化异步请求平均耗时: {sum(d for d,_ in optimized_results)/len(optimized_results):.3f}秒")
    print(f"共享session平均耗时: {sum(d for d,_ in shared_results)/len(shared_results):.3f}秒")


if __name__ == "__main__":
    import socket  # 导入需要的模块
    main()