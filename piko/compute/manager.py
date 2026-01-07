import asyncio
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import get_context
from typing import Callable, Iterable, TypeVar, List, Set, Tuple

import cloudpickle

from piko.config import settings
from piko.infra.logging import get_logger, setup_logging

logger = get_logger(__name__)

# T: map_fn 的输入类型（如 User, Order 等业务对象）
T = TypeVar("T")
# R: map_fn 的返回类型（如 int, dict 等计算结果）
R = TypeVar("R")


class _CloudPickledCallable:
    """可跨进程序列化的可调用对象包装器

    标准库的 pickle 无法序列化 lambda、闭包、本地函数等复杂对象，这在分布式计算和多进程场景中会导致序列化失败
    本类使用 cloudpickle 库解决这一问题，支持几乎所有 Python 对象的序列化

    Attributes:
        fn (Callable): 被包装的可调用对象（函数、方法、lambda 等）
        args (tuple): 位置参数
        kwargs (dict): 关键字参数
    """

    def __init__(self, fn: Callable[..., R], *args, **kwargs):
        """初始化可序列化的可调用对象包装器

        Args:
            fn (Callable[..., R]): 需要在子进程中执行的函数
            *args: 传递给 fn 的位置参数
            **kwargs: 传递给 fn 的关键字参数
        """
        self.fn = fn
        self.args = args
        self.kwargs = kwargs

    def __call__(self) -> R:
        """执行被包装的函数

        在子进程中反序列化后，通过调用此方法来执行实际的计算逻辑

        Returns:
            R: 函数执行结果
        """
        return self.fn(*self.args, **self.kwargs)

    def __getstate__(self):
        """序列化对象状态"""
        return cloudpickle.dumps((self.fn, self.args, self.kwargs))

    def __setstate__(self, state):
        """反序列化对象状态"""
        self.fn, self.args, self.kwargs = cloudpickle.loads(state)


def _worker_entry(pickled_task: _CloudPickledCallable):
    """子进程入口函数

    ProcessPoolExecutor 将此函数作为子进程的执行入口。接收序列化的任务对象，反序列化后执行实际的计算逻辑

    Args:
        pickled_task (_CloudPickledCallable): 已序列化的可调用对象包装器

    Returns:
        Any: 任务执行结果
    """
    return pickled_task()


class CpuManager:
    """CPU 密集型任务的多进程管理器，实现 MapReduce 模式

    本类负责管理一个进程池，用于执行 CPU 密集型任务（如数据处理、科学计算等）。
    通过多进程并行计算，绕过 Python GIL 的限制。

    Attributes:
        _pool (ProcessPoolExecutor | None): 底层进程池实例
        _max_workers (int): 进程池的最大工作进程数
    """

    def __init__(self):
        """初始化 CPU 管理器

        根据配置或系统 CPU 核心数自动确定工作进程数
        """
        self._pool: ProcessPoolExecutor | None = None

        # 从配置文件读取 cpu_workers 配置项
        self._max_workers = settings.cpu_workers
        if self._max_workers == 0:
            # 预留一个核心给主进程和操作系统，避免 CPU 100% 导致的响应延迟
            self._max_workers = max(1, multiprocessing.cpu_count() - 1)

    def startup(self, initializer: Callable = None, initargs: Tuple = ()):
        """启动进程池

        创建固定大小的工作进程池，预热子进程以减少任务提交时的延迟。

        Args:
            initializer (Callable, optional): 每个工作进程启动时调用的初始化函数。
                                            默认使用 setup_logging。
                                            可传入自定义函数以同时初始化 DB 等资源。
            initargs (Tuple, optional): 传递给 initializer 的参数。
        """
        if self._pool is None:
            # 使用 "spawn" 上下文而非 "fork"，确保跨平台兼容性和状态隔离
            ctx = get_context("spawn")

            # 默认初始化日志，允许外部覆盖（比如同时初始化 DB 和 Log）
            real_initializer = initializer or setup_logging
            real_initargs = initargs

            self._pool = ProcessPoolExecutor(
                max_workers=self._max_workers,
                mp_context=ctx,
                initializer=real_initializer,
                initargs=real_initargs
            )
            logger.info("cpu_manager_started", workers=self._max_workers)

    def shutdown(self):
        """优雅关闭进程池

        等待所有正在执行的任务完成后，再关闭进程池并回收资源。
        """
        if self._pool:
            self._pool.shutdown(wait=True)
            self._pool = None
            logger.info("cpu_manager_shutdown")

    async def submit(self, fn: Callable[..., R], *args, **kwargs) -> R:
        """异步提交单个任务到进程池

        将同步的 CPU 密集型函数包装为异步任务，无缝集成到 asyncio 事件循环。

        Args:
            fn (Callable[..., R]): 需要在子进程中执行的函数
            *args: 位置参数
            **kwargs: 关键字参数

        Returns:
            R: 函数执行结果

        Raises:
            RuntimeError: 如果进程池未启动
        """
        if self._pool is None:
            raise RuntimeError("CpuManager not started")

        loop = asyncio.get_running_loop()
        task = _CloudPickledCallable(fn, *args, **kwargs)
        return await loop.run_in_executor(self._pool, _worker_entry, task)

    async def map_reduce(
            self,
            map_fn: Callable[[T], R],
            items: Iterable[T],
            concurrency: int = 0
    ) -> List[R]:
        """批量并行处理数据集（MapReduce 模式的 Map 阶段）

        对 items 中的每个元素应用 map_fn，并发执行以加速处理。
        使用"有界信号量"模式控制并发度，防止一次性创建大量协程导致 OOM。

        Args:
            map_fn (Callable[[T], R]): 对单个元素的处理函数
            items (Iterable[T]): 待处理的数据集
            concurrency (int): 最大并发任务数。默认为 0 (使用 max_workers)

        Returns:
            List[R]: 所有元素的处理结果列表
        """
        if self._pool is None:
            raise RuntimeError("CpuManager not started")

        limit = concurrency if concurrency > 0 else self._max_workers
        results = []
        pending: Set[asyncio.Task] = set()

        for item in items:
            # 1. 如果在此刻正在运行的任务达到了上限，等待至少一个完成
            if len(pending) >= limit:
                done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    results.append(await task)

            # 2. 提交新任务
            task = asyncio.create_task(self.submit(map_fn, item))
            pending.add(task)

        # 3. 等待剩余任务完成
        if pending:
            done, _ = await asyncio.wait(pending, return_when=asyncio.ALL_COMPLETED)
            for task in done:
                results.append(await task)

        return results
