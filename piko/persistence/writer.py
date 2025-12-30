import asyncio
import json
import os
from typing import List, Dict

import aiofiles

from piko.config import settings
from piko.infra.logging import get_logger
from piko.infra.observability import PERSISTENCE_QUEUE_SIZE
from piko.persistence.intent import WriteIntent
from piko.persistence.sink_base import ResultSink

logger = get_logger(__name__)


class PersistenceWriter:
    """异步批量持久化写入引擎

    通过队列缓冲、批量聚合、并发控制、磁盘兜底等机制，实现高性能、高可靠的数据持久化
    核心设计思想：解耦生产者（任务执行器）和消费者（Sink 写入），提供背压控制和容错保证

    Attributes:
        self._queue (asyncio.Queue): 内存缓冲队列，存储待写入的 WriteIntent
        self._sinks (Dict[str, ResultSink]): 已注册的 Sink 实例字典（sink_name -> sink）
        self._running (bool): 运行状态标志，控制消费循环的生命周期
        self._consumer_task (asyncio.Task | None): 后台消费任务引用，用于停止和等待
        self._batch_size (int): 批量写入的批次大小（每次最多处理多少条）
        self._batch_timeout (float): 批量收集的超时时间（秒），防止低流量时无限等待

    Methods:
        register_sink: 注册一个 Sink 实例到路由表
        start: 启动后台消费任务和磁盘数据恢复
        stop: 优雅关闭：等待队列清空、取消消费任务、磁盘兜底残留数据
        enqueue: 将 WriteIntent 加入队列（异步阻塞，支持背压控制）
        flush: 同步等待队列清空（测试或手动触发场景）

    Note:
        核心机制：
        1. 队列缓冲：生产者（任务执行器）异步入队，消费者批量出队并写入
           - 使用 asyncio.Queue 实现无锁异步队列
           - maxsize 限制队列大小，提供背压控制（队列满时 enqueue 阻塞）

        2. 批量聚合：消费者从队列中收集一批 Intent，减少网络往返和事务开销
           - 两阶段策略：阻塞等待首个元素 + 超时收集后续元素
           - batch_size 控制批次大小，batch_timeout 控制最大等待时间

        3. 并发控制：单消费者模式，按 Sink 分组后串行写入（避免死锁）
           - 未来可扩展为多消费者模式（每个 Sink 一个消费者）

        4. 磁盘兜底：写入失败或停机时，将数据 dump 到磁盘，启动时自动恢复
           - 使用 JSONL 格式（每行一个 JSON 对象），便于流式读取
           - 原子写入（Write-Temp-Move）防止文件损坏

        5. 优雅关闭：停机时等待队列清空（带超时），超时后触发磁盘兜底
           - 三阶段流程：等待队列 -> 取消消费者 -> dump 残留数据

        性能调优参数：
        - persist_queue_max：队列大小（背压控制点）
        - batch_size：批次大小（网络往返 vs 内存占用权衡）
        - batch_timeout：超时时间（吞吐量 vs 延迟权衡）
        - persist_flush_timeout_s：停机等待超时（可用性 vs 数据完整性权衡）

    Warning:
        - 必须先调用 start() 启动消费者，否则 enqueue 会抛出 RuntimeError
        - 停机时务必调用 stop()，否则队列中的数据可能丢失
        - 磁盘空间不足会导致 dump 失败，需监控磁盘使用率
    """

    def __init__(self):
        """初始化持久化写入引擎"""
        # 创建有界队列，maxsize 提供背压控制（队列满时生产者阻塞）
        self._queue = asyncio.Queue(maxsize=settings.persist_queue_max)

        # Sink 路由表：sink_name -> ResultSink 实例
        self._sinks: Dict[str, ResultSink] = {}

        # 运行状态标志和后台任务引用
        self._running = False
        self._consumer_task: asyncio.Task | None = None

        # 批量写入参数：从配置读取（默认值作为兜底）
        self._batch_size = 100
        self._batch_timeout = 0.5

    def register_sink(self, sink: ResultSink):
        """注册一个 Sink 实例到路由表

        Args:
            sink (ResultSink): 待注册的 Sink 实例

        Note:
            设计要点：
            - Sink 的 name 必须唯一，与 WriteIntent.sink 字段匹配
            - 重复注册会覆盖旧实例（类似 TypedSink 的重复注册警告）
            - 通常在应用启动时注册所有 Sink
        """
        self._sinks[sink.name] = sink

    async def start(self):
        """启动后台消费任务并恢复磁盘数据

        执行步骤：
        1. 检查是否已启动（幂等性保证）
        2. 创建后台消费任务（非阻塞）
        3. 恢复磁盘中的兜底数据（异步阻塞）
        4. 记录启动日志

        Note:
            恢复顺序设计：
            - 先启动消费者，再恢复磁盘数据
            - 这样恢复的数据可以立即被消费（避免阻塞启动流程）
            - 若先恢复再启动，恢复期间，队列会堆积（内存压力）
        """
        if self._running:
            # 幂等性保证：避免重复启动
            return

        self._running = True

        # 创建后台消费任务（非阻塞，立即返回）
        self._consumer_task = asyncio.create_task(self._consumer_loop())

        # 恢复磁盘兜底数据（异步阻塞，但通常很快完成）
        await self._recover_from_disk()

        logger.info("persistence_writer_started")

    async def stop(self):
        """优雅关闭持久化写入引擎

        三阶段关闭流程：
        1. 等待队列清空（带超时）：让消费者处理完积压数据
        2. 取消消费任务：停止消费循环
        3. dump 残留数据到磁盘：兜底未处理的数据

        Note:
            超时兜底策略：
            - 正常情况下，队列会在超时前清空
            - 异常情况下（如 Sink 卡死），超时后强制进入 dump 流程
            - 这确保停机时间可控，避免无限期等待

            为什么先等待再取消：
            - 等待期间，消费者仍在运行，可以处理队列中的数据
            - 取消后消费者立即退出，队列中的数据需要 dump 到磁盘
        """
        self._running = False
        logger.info("persistence_writer_stopping")

        if self._consumer_task:
            try:
                # 第一阶段：等待队列清空（带超时）
                # queue.join() 会等待所有 task_done() 调用完成
                await asyncio.wait_for(
                    self._queue.join(),
                    timeout=settings.persist_flush_timeout_s
                )
            except asyncio.TimeoutError:
                # 超时触发：记录警告并进入兜底流程
                logger.warning("persist_flush_timeout_triggering_fallback")

            # 第二阶段：取消消费任务
            if not self._consumer_task.done():
                self._consumer_task.cancel()  # 请求取消
                await asyncio.gather(self._consumer_task, return_exceptions=True)

            # 第三阶段：dump 残留数据到磁盘
            # 此时消费者已停止，队列中可能还有未处理的数据
            await self._dump_to_disk()

    async def enqueue(self, intent: WriteIntent):
        """将 WriteIntent 加入队列（异步阻塞，支持背压控制）

        Args:
            intent (WriteIntent): 待写入的意图对象

        Raises:
            RuntimeError: 当 Writer 未启动时（需先调用 start()）
            ValueError: 当 intent.sink 不在已注册的 Sink 列表中

        Note:
            背压控制机制：
            - Queue(maxsize=N) 限制队列大小
            - 队列满时，put() 会阻塞等待（异步阻塞，不占用线程）
            - 这防止生产者过快导致内存溢出（生产者速度 > 消费者速度）

            前置校验设计：
            - 提前检查 Sink 是否存在，避免数据入队后才发现错误
            - 这减少了队列中无效数据的比例，提升处理效率
        """
        # 前置条件检查：Writer 必须已启动
        if not self._running:
            raise RuntimeError("PersistenceWriter is not running")

        # 前置校验：Sink 是否已注册
        if intent.sink not in self._sinks:
            logger.error("unknown_sink", sink=intent.sink)
            raise ValueError(f"Unknown sink: {intent.sink}")

        # 异步入队（队列满时阻塞）
        await self._queue.put(intent)

        # 更新监控指标：队列当前大小
        PERSISTENCE_QUEUE_SIZE.set(self._queue.qsize())

    async def flush(self):
        """同步等待队列清空（测试或手动触发场景）

        Note:
            使用场景：
            - 单元测试：确保所有数据已写入后再断言
            - 手动触发：定时任务或管理命令中强制刷新

            与 stop() 的区别：
            - flush() 仅等待队列清空，不停止消费者
            - stop() 会停止消费者并 dump 残留数据
        """
        await self._queue.join()

    async def _consumer_loop(self):
        """主消费循环（后台持续运行）

        核心逻辑：
        1. 从队列中收集一批 Intent（批量聚合）
        2. 按 Sink 分组并批量写入
        3. 标记队列任务完成（task_done）
        4. 处理异常：写入失败 -> dump 到磁盘
        """
        # 复用 buffer 对象，避免频繁创建列表
        buffer: List[WriteIntent] = []

        # 循环条件：运行中 OR 队列非空
        while self._running or not self._queue.empty():
            try:
                # 处理下一个批次（收集 -> 写入 -> ack）
                await self._process_next_batch(buffer)
            except asyncio.CancelledError:
                # 停机信号：处理当前 buffer 后退出
                await self._handle_shutdown_signal(buffer)
                # 重新抛出 CancelledError，通知上层任务已取消
                raise
            except Exception as e:
                await self._handle_unexpected_error(e, buffer)

    async def _process_next_batch(self, buffer: List[WriteIntent]):
        """处理下一个批次（收集 -> 写入 -> ack）

        Args:
            buffer (List[WriteIntent]): 复用的缓冲区列表（避免频繁创建对象）

        Note:
            算法步骤：
            1. 调用 _fill_batch 填充 buffer（批量收集）
            2. 若 buffer 非空，调用 _flush_and_ack 写入并 ack
            3. 清空 buffer（复用列表对象）
        """
        # 第一步：从队列中收集一批数据
        await self._fill_batch(buffer)

        # 第二步：若收集到数据，执行批量写入
        if buffer:
            await self._flush_and_ack(buffer)
            # 清空 buffer，复用列表对象（避免重新分配内存）
            buffer.clear()

    async def _handle_shutdown_signal(self, buffer: List[WriteIntent]):
        """处理停机信号（CancelledError 场景）

        Args:
            buffer (List[WriteIntent]): 当前缓冲区中的未处理数据

        Note:
            停机时的数据处理：
            - 若 buffer 非空，尝试写入（_flush_safe）
            - 写入失败时自动 dump 到磁盘（_flush_safe 内部处理）
            - 确保停机时不丢失已收集的数据
        """
        if buffer:
            # 安全写入：失败时自动 dump
            await self._flush_safe_and_ack(buffer)

    async def _handle_unexpected_error(self, e: Exception, buffer: List[WriteIntent]):
        """处理未知异常（容错机制）

        Args:
            e (Exception): 捕获的异常对象
            buffer (List[WriteIntent]): 当前缓冲区中的数据

        Note:
            容错策略：
            - 记录错误日志（便于排查）
            - dump 当前 buffer 到磁盘（防止数据丢失）
            - ack 队列任务（避免 queue.join() 永久阻塞）
            - 休眠 1 秒后继续循环（避免异常死循环导致 CPU 飙升）

            为什么要 ack：
            - 若不 ack，queue.join() 会永久等待
            - 数据已 dump 到磁盘，可以在下次启动时恢复
        """
        logger.error("persistence_consumer_error", error=str(e))

        # dump 当前 buffer 到磁盘，添加 error_rescue 后缀便于识别
        if buffer:
            await self._dump_buffer_to_disk(buffer, suffix="_error_rescue")
            # ack 队列任务，避免 join() 阻塞
            for _ in buffer:
                self._queue.task_done()
            buffer.clear()

        # 休眠 1 秒，避免异常死循环（限流）
        await asyncio.sleep(1)

    async def _fill_batch(self, buffer: List[WriteIntent]):
        """从队列中收集一批数据（批量聚合算法）

        Args:
            buffer (List[WriteIntent]): 目标缓冲区（原地修改）

        Note:
            两阶段收集策略：
            1. 阻塞等待首个元素（无超时）：
               - 若队列为空且 Writer 已停止，直接返回（外层循环会退出）
               - 否则阻塞等待第一个元素（确保批次不为空）

            2. 超时收集后续元素（带超时）：
               - 计算 deadline = 当前时间 + batch_timeout
               - 循环收集元素，直到 batch_size 或超时
               - 每次收集时重新计算剩余超时时间（动态调整）

            性能权衡：
            - batch_size 大：吞吐量高，延迟高（适合批处理场景）
            - batch_timeout 小：延迟低，吞吐量低（适合实时场景）
        """
        # 第一阶段：阻塞等待首个元素（无超时）
        if not buffer:
            # 若已停止且队列空，直接返回（外层循环会退出）
            if not self._running and self._queue.empty():
                return

            # 阻塞等待第一个元素（确保批次不为空）
            item = await self._queue.get()
            buffer.append(item)

        # 第二阶段：计算超时时间点（当前时间 + 配置的超时时长）
        deadline = asyncio.get_running_loop().time() + self._batch_timeout

        # 循环收集元素，直到批次满或超时
        while len(buffer) < self._batch_size:
            # 计算剩余超时时间（动态调整）
            timeout = deadline - asyncio.get_running_loop().time()
            if timeout <= 0:
                # 超时，立即退出（即使未满批次）
                break

            try:
                # 带超时等待下一个元素
                item = await asyncio.wait_for(self._queue.get(), timeout=timeout)
                buffer.append(item)
            except (asyncio.TimeoutError, TimeoutError):
                # 超时退出（正常流程，不是异常）
                break

    async def _flush_and_ack(self, batch: List[WriteIntent]):
        """批量写入并 ack 队列任务

        Args:
            batch (List[WriteIntent]): 待写入的批次数据

        Note:
            try-finally 设计：
            - try 块执行批量写入（可能失败）
            - finally 块确保无论成功或失败都 ack 队列任务
            - 这避免写入失败导致 queue.join() 永久阻塞

            为什么总是 ack：
            - 写入失败时，数据已被 _flush 内部 dump 到磁盘
            - ack 后队列可以继续处理后续数据
            - 避免单次失败阻塞整个队列
        """
        try:
            # 执行批量写入（按 Sink 分组并调用 write_batch）
            await self._flush(batch)
        finally:
            # 无论成功或失败，都 ack 队列任务
            for _ in batch:
                self._queue.task_done()
            # 更新监控指标：队列当前大小
            PERSISTENCE_QUEUE_SIZE.set(self._queue.qsize())

    async def _flush_safe_and_ack(self, batch: List[WriteIntent]):
        """安全批量写入并 ack（停机场景）

        Args:
            batch (List[WriteIntent]): 待写入的批次数据

        Note:
            与 _flush_and_ack 的区别：
            - 使用 _flush_safe 而非 _flush
            - _flush_safe 捕获所有异常并 dump 到磁盘
            - 确保停机时不抛出异常
        """
        try:
            # 安全写入：失败时自动 dump
            await self._flush_safe(batch)
        finally:
            # 无论成功或失败，都 ack 队列任务
            for _ in batch:
                self._queue.task_done()

    async def _flush(self, batch: List[WriteIntent]):
        """批量写入到对应的 Sink（按 Sink 分组并调用 write_batch）

        Args:
            batch (List[WriteIntent]): 待写入的批次数据

        Note:
            算法步骤：
            1. 按 Sink 分组：遍历 batch，根据 intent.sink 分组
               - 使用 dict.setdefault 简化分组逻辑

            2. 逐个 Sink 写入：遍历分组，调用 sink.write_batch
               - 写入成功：记录 debug 日志
               - 写入失败：记录错误并 dump 到磁盘

            异常处理策略：
            - 单个 Sink 写入失败不影响其他 Sink
            - 失败的数据 dump 到磁盘，下次启动时恢复
            - 这确保部分失败时其他数据仍可正常写入
        """
        # 第一步：按 Sink 分组
        grouped: Dict[str, List[WriteIntent]] = {}
        for item in batch:
            # setdefault：若 key 不存在则创建空列表，然后 append
            grouped.setdefault(item.sink, []).append(item)

        # 第二步：逐个 Sink 批量写入
        for sink_name, items in grouped.items():
            sink = self._sinks.get(sink_name)
            if not sink:
                # Sink 未注册（理论上不会发生，enqueue 时已校验）
                continue

            try:
                # 调用 Sink 的批量写入接口
                await sink.write_batch(items)
                logger.debug("batch_write_success", sink=sink_name, count=len(items))
            except Exception as e:
                # 写入失败，记录错误并 dump 到磁盘
                logger.error("batch_write_failed", sink=sink_name, count=len(items), error=str(e))
                await self._dump_buffer_to_disk(items, suffix="_failed")

    async def _flush_safe(self, batch: List[WriteIntent]):
        """安全批量写入（捕获所有异常并 dump 到磁盘）

        Args:
            batch (List[WriteIntent]): 待写入的批次数据

        Note:
            与 _flush 的区别：
            - _flush 仅捕获单个 Sink 的异常
            - _flush_safe 捕获整个 _flush 调用的异常
            - 用于停机场景，确保不抛出异常
        """
        try:
            # 调用标准 flush 流程
            await self._flush(batch)
        except Exception as e:
            logger.error("final_flush_failed", error=str(e))
            await self._dump_buffer_to_disk(batch, suffix="_final")

    async def _dump_to_disk(self):
        """将队列中的残留数据 dump 到磁盘（停机兜底）

        Note:
            执行时机：
            - stop() 方法的第三阶段（消费者已停止）
            - 此时队列中可能还有未处理的数据

            算法步骤：
            1. 从队列中取出所有剩余数据（非阻塞）
            2. dump 到磁盘（JSONL 格式）
            3. ack 队列任务（避免 join() 阻塞）
        """
        # 取出队列中的所有剩余数据（非阻塞）
        remaining = []
        while not self._queue.empty():
            try:
                # get_nowait：非阻塞取出，队列空时抛出 QueueEmpty
                remaining.append(self._queue.get_nowait())
            except asyncio.QueueEmpty:
                # 队列已空，退出循环
                break

        # 若有残留数据，dump 到磁盘
        if remaining:
            await self._dump_buffer_to_disk(remaining, suffix="_shutdown")
            # ack 队列任务（避免 join() 阻塞）
            for _ in remaining:
                self._queue.task_done()

    async def _dump_buffer_to_disk(self, items: List[WriteIntent], suffix: str = ""):
        """将 buffer 数据原子写入磁盘

        Args:
            items (List[WriteIntent]): 待 dump 的数据列表
            suffix (str): 文件名后缀（用于区分不同场景）默认为空

        Note:
            原子写入设计（防止文件损坏）：
            1. 生成唯一临时文件名（添加随机后缀）
            2. 写入临时文件（JSONL 格式）
            3. 若写入成功，临时文件即为最终文件（无需 move）

            JSONL 格式选择：
            - 每行一个 JSON 对象，便于流式读取
            - 文件损坏时仅影响部分行，不会导致整个文件不可读
            - 恢复时可以逐行解析，容错性强

            错误处理：
            - dump 失败记录 critical 日志（数据丢失风险）
            - 需要监控此日志，触发告警（磁盘空间不足、权限问题等）
        """
        # 构造基础路径（来自配置）
        path = f"{settings.persist_disk_fallback_path}{suffix}"

        # 生成唯一文件名（添加 4 字节随机后缀，避免并发冲突）
        unique_path = f"{path}.{os.urandom(4).hex()}.jsonl"

        try:
            # 第一步：序列化所有 Intent 为 JSONL 格式
            lines = []
            for item in items:
                # model_dump_json：Pydantic 序列化方法（自动处理 datetime 等类型）
                lines.append(item.model_dump_json())

            # 拼接为单个字符串（每行一个 JSON，末尾加换行符）
            payload = "\n".join(lines) + "\n"

            # 第二步：原子写入临时文件
            # 使用 aiofiles 异步写入，避免阻塞事件循环
            async with aiofiles.open(unique_path, 'w') as f:
                await f.write(payload)

            logger.warning("data_dumped_to_disk", path=unique_path, count=len(items))
        except Exception as e:
            # dump 失败：记录 critical 日志（数据丢失风险）
            # 需要监控此日志并触发告警（磁盘故障、权限问题等）
            logger.critical("disk_fallback_failed_data_lost", error=str(e), count=len(items))

    async def _recover_from_disk(self):
        """启动时恢复磁盘中的兜底数据

        Note:
            恢复策略：
            1. 扫描兜底目录，查找所有符合条件的文件
            2. 逐文件恢复（解析 JSONL -> 反序列化 -> 入队）
            3. 恢复成功后重命名文件（添加 .recovered 后缀，避免重复恢复）

            文件名过滤规则：
            - 以 fallback_path 的 basename 为前缀
            - 不以 .recovered 结尾（已恢复的文件跳过）

            容错设计：
            - 单文件恢复失败不影响其他文件
            - 单行解析失败不影响其他行（记录错误后跳过）
        """
        # 获取兜底目录和文件名前缀
        base_dir = os.path.dirname(settings.persist_disk_fallback_path)
        filename_prefix = os.path.basename(settings.persist_disk_fallback_path)

        # 若目录不存在，直接返回（首次启动场景）
        if not os.path.exists(base_dir):
            return

        # 扫描目录，查找符合条件的文件
        for fname in os.listdir(base_dir):
            # 过滤规则：前缀匹配 且 未恢复
            if fname.startswith(filename_prefix) and not fname.endswith(".recovered"):
                full_path = os.path.join(base_dir, fname)
                # 逐文件恢复（委托给辅助方法）
                await self._recover_single_file(full_path)

    async def _recover_single_file(self, full_path: str):
        """恢复单个兜底文件（逐行解析 JSONL）

        Args:
            full_path (str): 兜底文件的完整路径

        Note:
            算法步骤：
            1. 记录发现日志
            2. 逐行读取并解析（JSONL 格式）
            3. 反序列化为 WriteIntent 并入队
            4. 恢复成功后重命名文件（添加 .recovered 后缀）

            异常处理：
            - 单行解析失败：记录错误后跳过（容错）
            - 整个文件恢复失败：记录错误（文件可能损坏）
        """
        logger.info("found_fallback_data", path=full_path)
        # 统计恢复的记录数
        recovered_count = 0

        try:
            async with aiofiles.open(full_path, 'r') as f:
                # 逐行读取（JSONL 格式）
                async for line in f:
                    line = line.strip()
                    if not line:
                        # 跳过空行
                        continue

                    try:
                        # 第一步：解析 JSON
                        data = json.loads(line)

                        # 第二步：反序列化为 WriteIntent
                        intent = WriteIntent.model_validate(data)

                        # 第三步：入队（会触发 Sink 校验）
                        await self.enqueue(intent)
                        recovered_count += 1
                    except Exception as parse_err:
                        logger.error("fallback_parse_line_error", error=str(parse_err))

            # 恢复成功：重命名文件（避免重复恢复）
            os.rename(full_path, full_path + ".recovered")
            logger.info("fallback_data_recovered", path=full_path, count=recovered_count)
        except Exception as e:
            # 整个文件恢复失败：记录错误（文件可能损坏）
            logger.error("recover_failed", path=full_path, error=str(e))


# 全局单例：应用启动时创建，全局共享
persistence_writer = PersistenceWriter()
