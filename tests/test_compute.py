import pytest
import os
import asyncio
from piko.compute.manager import cpu_manager


# 定义一个模块级函数 (普通 pickle 也能处理)
def cpu_heavy_task(x):
    return x * x, os.getpid()


@pytest.mark.asyncio
async def test_cpu_manager():
    # 1. Start
    cpu_manager.start()
    main_pid = os.getpid()

    try:
        # 2. Test Single Submit
        result, worker_pid = await cpu_manager.submit(cpu_heavy_task, 10)
        assert result == 100
        assert worker_pid != main_pid  # 必须在不同进程执行

        # 3. Test Cloudpickle (Closure / Lambda)
        # 这是一个普通 pickle 无法处理的场景：闭包
        factor = 5

        def closure_task(x):
            return x * factor, os.getpid()

        result, worker_pid = await cpu_manager.submit(closure_task, 10)
        assert result == 50
        assert worker_pid != main_pid

        # 4. Test Map Reduce
        items = list(range(10))
        results = await cpu_manager.map_reduce(lambda x: x + 1, items, chunk_size=2)
        assert results == [x + 1 for x in items]

    finally:
        cpu_manager.shutdown()
