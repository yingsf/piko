import asyncio
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import get_context
from typing import Callable, Iterable, TypeVar, List, Set

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

    设计考量：
        - 在 ProcessPoolExecutor 中，任务函数及其参数需要通过 pickle 序列化后
          传递给子进程。标准 pickle 的限制会导致很多实用的函数无法使用
        - cloudpickle 通过序列化函数的字节码和闭包环境，突破了这一限制
        - 本类实现了 __getstate__ 和 __setstate__，确保在序列化/反序列化过程中正确保存和恢复函数及其参数

    Attributes:
        self.fn (Callable): 被包装的可调用对象（函数、方法、lambda 等）
        self.args (tuple): 位置参数
        self.kwargs (dict): 关键字参数
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

        Note:
            此方法在子进程的上下文中执行，无法访问父进程的内存状态
        """
        return self.fn(*self.args, **self.kwargs)

    def __getstate__(self):
        """序列化对象状态

        使用 cloudpickle 将函数及其参数序列化为字节流，以便通过进程间通信（IPC）传递给子进程

        Returns:
            bytes: 序列化后的字节流
        """
        return cloudpickle.dumps((self.fn, self.args, self.kwargs))

    def __setstate__(self, state):
        """反序列化对象状态

        在子进程中接收到序列化字节流后，通过此方法恢复函数及其参数

        Args:
            state (bytes): 序列化的字节流。

        Note:
            反序列化后，self.fn 在子进程中是一个独立的函数对象，与父进程中的原函数对象没有内存上的关联
        """
        self.fn, self.args, self.kwargs = cloudpickle.loads(state)


def _worker_entry(pickled_task: _CloudPickledCallable):
    """子进程入口函数

    ProcessPoolExecutor 将此函数作为子进程的执行入口。接收序列化的任务对象，反序列化后执行实际的计算逻辑

    Args:
        pickled_task (_CloudPickledCallable): 已序列化的可调用对象包装器

    Returns:
        任务执行结果（类型由具体任务函数决定）。

    Note:
        此函数在子进程中运行，拥有独立的内存空间和 Python 解释器实例。因此任何全局状态（如日志配置、数据库连接）都需要在子进程中重新初始化
    """
    return pickled_task()


class CpuManager:
    """CPU 密集型任务的多进程管理器，实现 MapReduce 模式

    本类负责管理一个进程池，用于执行 CPU 密集型任务（如数据处理、科学计算等）。通过多进程并行计算，绕过 Python GIL 的限制

    核心设计：
        1. 进程池生命周期管理：
           - startup() 创建固定大小的进程池，避免频繁创建/销毁进程的开销
           - shutdown() 优雅关闭进程池，等待所有任务完成后再退出

        2. 异步任务提交：
           - submit() 将同步函数包装为异步任务，无缝集成到 asyncio 事件循环
           - 使用 cloudpickle 支持 lambda、闭包等复杂函数的序列化

        3. MapReduce 模式：
           - map_reduce() 实现了有界并发的 map 操作，防止内存溢出
           - 通过流式提交任务（而非一次性创建所有协程），控制内存使用
           - 适用于大规模数据处理场景（如处理百万级数据集）

        4. 并发控制（关键优化）：
           - 使用"有界信号量"模式，限制同时执行的任务数
           - 当并发任务数达到上限时，等待至少一个任务完成后再提交新任务
           - 这避免了一次性创建百万个协程导致的 OOM（内存溢出）问题

    使用场景：
        - 大规模数据的并行处理（如日志分析、批量数据转换）
        - CPU 密集型计算（如图像处理、机器学习推理）
        - 需要绕过 GIL 的多核并行任务

    性能考量：
        - 进程间通信（IPC）有序列化开销，不适合频繁传递大对象
        - 推荐将大数据集存储在共享存储（如数据库、Redis），仅传递索引/键
        - 进程创建有初始成本（约 10-50ms），适合长时间运行的任务

    Attributes:
        self._pool (ProcessPoolExecutor | None): 底层进程池实例，启动前为 None
        self._max_workers (int): 进程池的最大工作进程数

    Warning:
        使用 spawn 上下文启动子进程，子进程不会继承父进程的全局状态。因此日志配置、数据库连接等需要在 initializer 中重新设置
    """

    def __init__(self):
        """初始化 CPU 管理器

        根据配置或系统 CPU 核心数自动确定工作进程数
        """
        self._pool: ProcessPoolExecutor | None = None

        # 从配置文件读取 cpu_workers 配置项
        # 若配置为 0，则自动检测 CPU 核心数（保留一个核心给系统/其他进程）
        self._max_workers = settings.cpu_workers
        if self._max_workers == 0:
            # 预留一个核心给主进程和操作系统，避免 CPU 100% 导致的响应延迟
            self._max_workers = max(1, multiprocessing.cpu_count() - 1)

    def startup(self):
        """启动进程池

        创建固定大小的工作进程池，预热子进程以减少任务提交时的延迟

        Note:
            此方法应在服务启动阶段调用，确保进程池在接收任务前已就绪。多次调用 startup() 不会创建多个进程池（已存在时跳过）
        """
        if self._pool is None:
            # 使用 "spawn" 上下文而非 "fork" 的原因：
            #   1. 跨平台兼容性：Windows 不支持 fork，spawn 在所有平台上都可用
            #   2. 状态隔离：spawn 创建全新的 Python 解释器，避免继承父进程的锁、文件句柄等状态，减少死锁和资源泄漏风险
            #   3. 安全性：fork 在多线程环境下不安全（可能导致死锁），asyncio 环境下建议用 spawn
            ctx = get_context("spawn")

            # 创建进程池，并通过 initializer 在每个子进程启动时调用 setup_logging
            # 确保子进程的日志配置与主进程一致（否则子进程日志可能丢失或格式错误）
            self._pool = ProcessPoolExecutor(
                max_workers=self._max_workers,
                mp_context=ctx,
                # 在子进程中重新初始化日志配置
                initializer=setup_logging
            )
            logger.info("cpu_manager_started", workers=self._max_workers)

    def shutdown(self):
        """优雅关闭进程池

        等待所有正在执行的任务完成后，再关闭进程池并回收资源

        Note:
            调用此方法后，进程池将不再接受新任务。如果需要继续使用，必须重新调用 startup()。
        """
        if self._pool:
            # wait=True 确保在所有任务完成前阻塞，防止进程被强制杀死导致数据丢失
            self._pool.shutdown(wait=True)
            self._pool = None
            logger.info("cpu_manager_shutdown")

    async def submit(self, fn: Callable[..., R], *args, **kwargs) -> R:
        """异步提交单个任务到进程池

        将同步的 CPU 密集型函数包装为异步任务，无缝集成到 asyncio 事件循环

        Args:
            fn (Callable[..., R]): 需要在子进程中执行的函数。可以是普通函数、lambda、闭包等
            *args: 传递给 fn 的位置参数
            **kwargs: 传递给 fn 的关键字参数

        Returns:
            R: 函数执行结果

        Raises:
            RuntimeError: 如果进程池未启动（忘记调用 startup()）

        Note:
            - 参数和返回值都会经过序列化/反序列化，避免传递大对象（如 GB 级数据）。
            - 使用 cloudpickle 支持几乎所有 Python 对象，但仍无法序列化某些特殊对象（如打开的文件句柄、数据库连接等）
        """
        if self._pool is None:
            raise RuntimeError("CpuManager not started")

        # 获取当前运行的事件循环（asyncio.run() 或 uvicorn 等框架创建的循环）
        loop = asyncio.get_running_loop()

        # 将函数和参数封装为可序列化的任务对象
        task = _CloudPickledCallable(fn, *args, **kwargs)

        # 将任务提交到进程池，并通过 run_in_executor 将其转换为协程
        # 这样调用方可以 await 此方法，而不阻塞事件循环的其他任务
        return await loop.run_in_executor(self._pool, _worker_entry, task)

    async def map_reduce(
            self,
            map_fn: Callable[[T], R],
            items: Iterable[T],
            concurrency: int = 0
    ) -> List[R]:
        """批量并行处理数据集（MapReduce 模式的 Map 阶段）

        对 items 中的每个元素应用 map_fn，并发执行以加速处理。使用"有界信号量"模式控制并发度，防止一次性创建百万个协程导致 OOM

        设计思路：
            1. 流式提交：逐个迭代 items，动态提交任务，而非一次性创建所有协程。
            2. 有界并发：当正在执行的任务数达到 concurrency 上限时，等待至少一个任务完成后再提交新任务

        Args:
            map_fn (Callable[[T], R]): 对单个元素的处理函数，应为纯函数或无副作用函数
            items (Iterable[T]): 待处理的数据集，可以是列表、生成器等任意可迭代对象
            concurrency (int): 最大并发任务数。默认为 0，表示使用 max_workers

        Returns:
            List[R]: 所有元素的处理结果列表

        Raises:
            RuntimeError: 如果进程池未启动

        Note:
            - 结果顺序可能与输入顺序不同（乱序问题）。如果需要保序，调用方应：
              1. 为每个 item 附加索引，在 map_fn 中返回 (index, result)
              2. 最终按 index 排序
            - 当前实现优先解决 OOM 问题，后续版本可考虑提供 ordered=True 参数。对于需要 reduce 的场景（如求和、合并），通常对顺序不敏感

        Warning:
            如果 items 是生成器且生成速度很慢，可能导致进程池空闲等待
            建议将生成器转为列表（如果内存足够），或使用更复杂的生产者-消费者模式
        """
        if self._pool is None:
            raise RuntimeError("CpuManager not started")

        # 显式指定 concurrency 时使用指定值，否则使用进程池的最大工作进程数
        limit = concurrency if concurrency > 0 else self._max_workers

        # 存储所有任务的结果
        results = []
        # 当前正在执行的任务集合
        pending: Set[asyncio.Task] = set()

        # 流式提交，防止一次性创建百万个协程撑爆内存
        for item in items:
            # 1. 如果在此刻正在运行的任务达到了上限，等待至少一个完成
            if len(pending) >= limit:
                # asyncio.wait 会挂起当前协程，直到至少一个任务完成
                # return_when=FIRST_COMPLETED 表示只要有一个任务完成就返回
                done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)

                # 收集已完成的结果
                for task in done:
                    results.append(await task)

            # 2. 提交新任务
            # 使用 asyncio.create_task 将协程转换为 Task 对象，并加入事件循环调度
            task = asyncio.create_task(self.submit(map_fn, item))
            pending.add(task)

        # 3. 等待剩余任务完成
        # 循环结束后，pending 中可能还有未完成的任务（少于 limit 个），需要等待它们全部完成
        if pending:
            # return_when=ALL_COMPLETED 确保所有任务都完成后才返回
            done, _ = await asyncio.wait(pending, return_when=asyncio.ALL_COMPLETED)
            for task in done:
                results.append(await task)

        return results


# 全局单例模式，确保整个应用共享同一个进程池实例
cpu_manager = CpuManager()
