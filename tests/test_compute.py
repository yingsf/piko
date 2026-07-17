import asyncio
import os
import time

import pytest

from piko import PikoApp


def cpu_heavy_task(value: int) -> tuple[int, int]:
    """返回输入平方和执行进程号"""
    return value * value, os.getpid()


def increment(value: int) -> int:
    """递增一个整数"""
    return value + 1


def fail_on_three(value: int) -> int:
    """在指定输入上抛出异常"""
    if value == 3:
        raise ValueError("three is invalid")
    return value


def delayed_increment(value: int) -> int:
    """以反向延迟模拟乱序完成的计算任务"""
    time.sleep((4 - value) * 0.02)
    return value + 1


def slow_increment(value: int) -> int:
    """以较长延迟模拟可取消的计算任务"""
    time.sleep(0.5)
    return value + 1


@pytest.mark.asyncio
async def test_cpu_manager() -> None:
    """验证应用实例持有的 CPU 计算池"""
    app = PikoApp(name="compute-test")
    app.cpu_manager.startup()
    main_pid = os.getpid()

    try:
        result, worker_pid = await app.cpu_manager.submit(cpu_heavy_task, 10)
        assert result == 100
        assert worker_pid != main_pid

        factor = 5

        def closure_task(value: int) -> tuple[int, int]:
            return value * factor, os.getpid()

        result, worker_pid = await app.cpu_manager.submit(closure_task, 10)
        assert result == 50
        assert worker_pid != main_pid

        results = await app.cpu_manager.map_reduce(increment, range(10), concurrency=2)
        assert results == list(range(1, 11))

        ordered_results = await app.cpu_manager.map_reduce(
            delayed_increment, range(4), concurrency=4
        )
        assert ordered_results == list(range(1, 5))

        with pytest.raises(ValueError, match="three is invalid"):
            await app.cpu_manager.map_reduce(fail_on_three, range(5), concurrency=2)

        cancelled = asyncio.create_task(
            app.cpu_manager.map_reduce(slow_increment, range(4), concurrency=2)
        )
        await asyncio.sleep(0.05)
        cancelled.cancel()
        with pytest.raises(asyncio.CancelledError):
            await cancelled
    finally:
        app.cpu_manager.shutdown()
