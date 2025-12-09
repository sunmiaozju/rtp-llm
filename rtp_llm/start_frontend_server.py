import logging
import logging.config
import os
import sys
import traceback
import asyncio
import functools
import time
import weakref
from typing import Dict, Any, Set

from setproctitle import setproctitle

from rtp_llm.config.py_config_modules import PyEnvConfigs

CUR_PATH = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(str(CUR_PATH), ".."))

from rtp_llm.distribute.worker_info import FrontendServerInfo
from rtp_llm.frontend.frontend_app import FrontendApp
from rtp_llm.utils.concurrency_controller import (
    ConcurrencyController,
    set_global_controller,
)

# ========== 异步生成器调试代码开始 ==========
# 存储所有异步生成器的信息
active_generators: Dict[int, Dict[str, Any]] = {}
generator_refs: Set[weakref.ref] = set()

# 原始方法
original_athrow = None
original_aclose = None


def patch_async_generator_methods():
    """Patch 异步生成器的方法来追踪 athrow 和 aclose"""

    global original_athrow, original_aclose

    # 获取异步生成器类型
    async def dummy():
        yield

    AsyncGeneratorType = type(dummy())

    # 保存原始方法
    original_athrow = AsyncGeneratorType.athrow
    original_aclose = AsyncGeneratorType.aclose

    def tracked_athrow(self, *args, **kwargs):
        """追踪 athrow 操作"""
        gen_id = id(self)
        start_time = time.time()

        # 获取生成器信息
        gen_info = active_generators.get(gen_id, {})
        gen_name = gen_info.get('name', 'Unknown')

        logging.warning(f"🔴 [ATHROW 开始] 生成器: {gen_name} (ID: {gen_id})")
        logging.warning(f"  类型: {type(self)}")
        logging.warning(f"  参数: {args}")

        # 获取代码信息
        if hasattr(self, 'ag_code'):
            code = self.ag_code
            logging.warning(f"  代码位置: {code.co_filename}:{code.co_firstlineno} in {code.co_name}")

        # 打印创建时的调用栈
        if 'stack_trace' in gen_info:
            logging.warning("  创建位置:")
            for frame in gen_info['stack_trace'][-8:-1]:
                if 'site-packages' not in frame.filename:
                    logging.warning(f"    {frame.filename}:{frame.lineno} in {frame.name}")

        # 打印当前调用栈
        current_stack = traceback.extract_stack()
        logging.warning("  当前调用栈:")
        for frame in current_stack[-10:-1]:
            if 'asyncio' not in frame.filename:
                logging.warning(f"    {frame.filename}:{frame.lineno} in {frame.name}")

        try:
            # 调用原始方法
            result = original_athrow(self, *args, **kwargs)
            duration = time.time() - start_time

            if duration >= 0.05:  # 匹配用户的慢回调阈值
                logging.error(f"⚠️  [ATHROW 完成] 生成器: {gen_name} (ID: {gen_id}), 耗时: {duration:.3f}秒 (超过阈值!)")
            else:
                logging.info(f"✅ [ATHROW 完成] 生成器: {gen_name} (ID: {gen_id}), 耗时: {duration:.3f}秒")

            return result
        except Exception as e:
            duration = time.time() - start_time
            logging.error(f"❌ [ATHROW 异常] 生成器: {gen_name} (ID: {gen_id}), 耗时: {duration:.3f}秒, 异常: {e}")
            raise

    def tracked_aclose(self):
        """追踪 aclose 操作"""
        gen_id = id(self)
        gen_info = active_generators.get(gen_id, {})
        gen_name = gen_info.get('name', 'Unknown')

        logging.info(f"🟡 [ACLOSE] {gen_name} (ID: {gen_id})")

        return original_aclose(self)

    # 替换方法
    AsyncGeneratorType.athrow = tracked_athrow
    AsyncGeneratorType.aclose = tracked_aclose

    logging.info("✅ 异步生成器方法追踪已启用")


def setup_async_generator_tracking():
    """设置异步生成器生命周期追踪"""

    # 获取事件循环
    loop = asyncio.get_event_loop()

    def tracked_firstiter_hook(agen):
        """当异步生成器第一次迭代时调用"""
        gen_id = id(agen)

        # 获取创建时的调用栈
        stack = traceback.extract_stack()

        # 获取生成器名称
        gen_name = 'Unknown'
        if hasattr(agen, '__name__'):
            gen_name = agen.__name__
        elif hasattr(agen, 'ag_code'):
            gen_name = agen.ag_code.co_name
        elif hasattr(agen, '__qualname__'):
            gen_name = agen.__qualname__

        # 尝试从调用栈中获取更多信息
        for frame in reversed(stack):
            if 'model_rpc_client' in frame.filename:
                gen_name = f"{gen_name} (from model_rpc_client)"
                break
            elif 'frontend_worker' in frame.filename:
                gen_name = f"{gen_name} (from frontend_worker)"
                break

        # 记录生成器信息
        active_generators[gen_id] = {
            'name': gen_name,
            'created_at': time.time(),
            'stack_trace': stack,
            'type': type(agen).__name__,
            'repr': repr(agen),
        }

        # 创建弱引用来追踪生成器
        ref = weakref.ref(agen, lambda r: on_generator_deleted(gen_id))
        generator_refs.add(ref)

        logging.info(f"🟢 [AsyncGen 创建] {gen_name} (ID: {gen_id})")
        # 打印关键的调用栈帧
        for frame in stack[-10:-1]:
            if 'site-packages' not in frame.filename and 'asyncio' not in frame.filename:
                logging.info(f"  -> {frame.filename}:{frame.lineno} in {frame.name}")

    def tracked_finalizer_hook(agen):
        """当异步生成器被垃圾回收时调用"""
        gen_id = id(agen)

        if gen_id in active_generators:
            info = active_generators[gen_id]
            lifetime = time.time() - info['created_at']

            if lifetime > 0.1:
                logging.warning(f"🔵 [AsyncGen GC] {info['name']} (ID: {gen_id}), 存活时间: {lifetime:.3f}秒 ⚠️")
            else:
                logging.info(f"🔵 [AsyncGen GC] {info['name']} (ID: {gen_id}), 存活时间: {lifetime:.3f}秒")

    # 设置钩子
    loop.set_asyncgen_hooks(
        firstiter=tracked_firstiter_hook,
        finalizer=tracked_finalizer_hook
    )

    logging.info("✅ AsyncIO 生成器生命周期追踪已启用")


def on_generator_deleted(gen_id: int):
    """当生成器被删除时调用"""
    if gen_id in active_generators:
        info = active_generators[gen_id]
        lifetime = time.time() - info['created_at']
        logging.info(f"[AsyncGen 删除] {info['name']} (ID: {gen_id}), 存活时间: {lifetime:.3f}秒")
        del active_generators[gen_id]


def print_active_generators():
    """打印当前所有活跃的异步生成器"""
    if active_generators:
        logging.info(f"📊 当前活跃的异步生成器: {len(active_generators)}个")
        for gen_id, info in active_generators.items():
            lifetime = time.time() - info['created_at']
            logging.info(f"  - {info['name']} (ID: {gen_id}, 存活时间: {lifetime:.3f}秒)")
    else:
        logging.info("✅ 没有活跃的异步生成器")


async def periodic_report():
    """定期报告活跃的异步生成器"""
    while True:
        await asyncio.sleep(30)
        print_active_generators()

# ========== 异步生成器调试代码结束 ==========


def start_frontend_server(rank_id: int, server_id: int, global_controller: ConcurrencyController):
    ## collect all args and envs.
    py_env_configs = PyEnvConfigs()
    py_env_configs.update_from_env()
    py_env_configs.server_config.frontend_server_id = server_id
    py_env_configs.server_config.rank_id = rank_id
    setproctitle(f"rtp_llm_frontend_server_rank_{rank_id}_server_{server_id}")

    # ========== 启用异步生成器调试 ==========
    logging.info("=" * 80)
    logging.info("启动异步生成器调试工具")
    logging.info("=" * 80)

    # 先打补丁，这个必须在任何异步生成器创建之前
    patch_async_generator_methods()

    logging.info("异步生成器方法追踪已就绪")
    logging.info("-" * 80)
    # ========== 异步生成器调试结束 ==========

    app = None
    g_frontend_server_info = FrontendServerInfo(
        py_env_configs.server_config.frontend_server_id
    )
    try:
        logging.info(f"g_frontend_server_info = {g_frontend_server_info}")
        set_global_controller(global_controller)
        separated_frontend = os.environ.get("ROLE_TYPE", "") == "FRONTEND"
        app = FrontendApp(py_env_configs, separated_frontend)
        app.start()
    except BaseException as e:
        logging.error(
            f"start frontend server error: {e}, trace: {traceback.format_exc()}"
        )
        raise e
    return app
