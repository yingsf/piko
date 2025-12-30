import asyncio
import datetime
import json
import os
import socket
from contextlib import AsyncExitStack
from typing import Optional, List, cast, Tuple, Dict, Any

from croniter import croniter
from sqlalchemy import insert, update, select, CursorResult
from sqlalchemy.exc import IntegrityError

from piko.config import settings
from piko.core.cache import config_cache
from piko.core.registry import registry, JobOptions
from piko.core.types import DataInterval, BackfillPolicy
from piko.infra.db import get_session, JobLock, JobRun, ScheduledJob, utcnow
from piko.infra.leader import get_leader_mutex
from piko.infra.logging import get_logger
from piko.infra.observability import JOB_RUN_TOTAL, JOB_DURATION_SECONDS
from piko.persistence.writer import persistence_writer

logger = get_logger(__name__)


class JobRunner:
    """任务执行引擎（支持有状态回填 + 资源依赖注入）

    本类是 Piko 框架的核心执行组件,负责任务的生命周期管理、幂等性保证、资源注入、状态维护和可观测性记录

    执行流程（run_job方法）：
        1. 前置校验：检查是否为 Leader 节点、任务是否已注册
        2. 计算执行区间：
           - 无状态任务：直接执行（scheduled_time 作为触发点）
           - 有状态任务：计算需要补跑的时间窗口列表（基于水位线和 backfill_policy）
        3. 循环执行：对每个时间窗口执行以下步骤：
           a. 获取幂等锁（防止重复执行）
           b. 创建执行记录（job_run 表）
           c. 实例化并注入资源（AsyncExitStack 管理生命周期）
           d. 调用任务处理函数（传入 ctx、scheduled_time 和注入的资源）
           e. 刷新持久化缓冲区（persistence_writer）
           f. 更新执行记录（状态、耗时、错误信息）
           g. 记录 Prometheus 指标
           h. 释放资源（AsyncExitStack 自动清理）
        4. 后处理：
           - 有状态任务：成功后更新水位线（last_data_time），失败则中断回填
           - 无状态任务：无后处理

    Attributes:
        host_id (str): 主机标识符，格式为 "hostname:pid"，用于记录任务的执行节点

    Example:
        ```python
        from piko.core.runner import job_runner

        # APScheduler 会自动调用此方法
        await job_runner.run_job("my_task", scheduled_time=datetime(2025, 12, 30, 10, 0, 0))

        # 任务函数的签名（资源自动注入）
        @job("my_task", resources={"db": PostgresResource, "cache": RedisResource})
        async def my_task(ctx, scheduled_time, db, cache):
            # db 和 cache 是自动注入的资源实例
            data = await db.fetch("SELECT * FROM users")
            await cache.set("users", data)
        ```

    Note:
        - 本类应作为全局单例使用（见模块底部的 `job_runner = JobRunner()`）
        - 任务函数必须是协程函数（async def）
        - 资源的生命周期完全由 AsyncExitStack 管理，任务函数无需手动关闭资源
    """

    def __init__(self):
        """初始化任务执行引擎

        获取主机标识符（hostname:pid），用于记录任务的执行节点
        """
        # 构建主机标识符：hostname:pid
        self.host_id = f"{socket.gethostname()}:{os.getpid()}"

    async def run_job(self, job_id: str, scheduled_time: datetime.datetime | None = None):
        """任务执行的核心入口（支持有状态回填 + 资源注入）

        本方法是 APScheduler 触发任务的入口，负责整个任务的生命周期管理

        Args:
            job_id (str): 任务的唯一标识符
            scheduled_time (datetime.datetime | None): 计划触发时间（来自 PikoExecutor 注入）
                - 如果为 None，使用当前 UTC 时间（兼容手动触发场景）
                - 如果带时区信息，会转换为 UTC Naive（与数据库约定一致）

        工作流程：
            1. 验证运行条件（Leader 检查、任务是否已注册）
            2. 计算需要执行的时间窗口（无状态任务：1 个点；有状态任务：N 个区间）
            3. 循环执行每个时间窗口：
               a. 获取幂等锁
               b. 创建执行记录
               c. 注入资源并调用任务处理函数
               d. 更新执行记录和水位线
            4. 记录 Prometheus 指标

        Note:
            - 无状态任务：每次触发都是独立的，scheduled_time 作为触发点（start == end）
            - 有状态任务：根据 backfill_policy 计算需要补跑的时间窗口，逐个执行
            - 如果中途失败，有状态任务会中断回填（避免脏数据传播）
        """
        # 默认使用当前 UTC 时间（如果 scheduled_time 未提供）
        if scheduled_time is None:
            scheduled_time = utcnow()

        # 时区规范化：转换为 UTC Naive
        if scheduled_time.tzinfo:
            scheduled_time = scheduled_time.astimezone(datetime.timezone.utc).replace(tzinfo=None)

        # 绑定结构化日志上下文
        log = logger.bind(job_id=job_id, scheduled_time=scheduled_time.isoformat())

        # 1. 验证运行条件并获取 Handler
        handler_info = self._validate_and_get_handler(job_id, log)
        if not handler_info:
            # 验证失败（如非 Leader 节点、任务未注册），直接返回
            return

        # 解包验证结果
        _, opts = handler_info

        # 2. 计算需要执行的时间窗口 (Intervals)
        # 无状态任务：返回 [DataInterval(scheduled_time, scheduled_time)]（1 个点）
        # 有状态任务：根据 backfill_policy 计算需要补跑的时间窗口列表（可能 0 到 N 个）
        intervals = await self._resolve_intervals(job_id, scheduled_time, opts, log)
        if not intervals:
            # 没有需要执行的时间窗口（如有状态任务已处理到最新）
            return

        # 判断任务是否为有状态
        is_stateful = opts["stateful"]

        # 3. 循环执行 (Catch-up Mode)
        # 对于无状态任务，只循环一次（intervals 只有一个元素）
        # 对于有状态任务，逐个执行需要补跑的时间窗口
        for interval in intervals:
            # 计算逻辑触发时间（Logic Scheduled Time）
            logic_scheduled_time = interval.start if is_stateful else scheduled_time

            # 执行单个时间窗口的任务
            success = await self._execute_single_run(
                job_id,
                logic_scheduled_time,
                # 无状态任务不传递时间窗口
                interval if is_stateful else None,
                opts["resources"]
            )

            # 4. 后处理：更新水位线或中断回填
            if is_stateful:
                if success:
                    await self._update_watermark(job_id, interval.end)
                else:
                    log.warning("backfill_interrupted_by_failure", failed_interval=str(interval))
                    break

    def _validate_and_get_handler(self, job_id: str, log) -> Optional[Tuple[Any, JobOptions]]:
        """前置校验：检查运行条件并获取任务处理函数

        Args:
            job_id (str): 任务的唯一标识符
            log: 绑定了上下文的日志对象

        Returns:
            Optional[Tuple[Any, JobOptions]]:
                - 成功：返回 (handler函数, JobOptions元数据)
                - 失败：返回 None

        校验逻辑：
            1. Leader 检查：如果启用 Leader Election 且当前节点不是 Leader，跳过执行
            2. 白名单检查：任务是否已在 Registry 中注册
        """
        # Leader 检查
        if settings.leader_enabled and not get_leader_mutex().is_leader:
            log.trace("runner_skip_standby")
            return None

        # 白名单检查：任务是否已注册
        handler = registry.get_job(job_id)
        if not handler:
            log.warning("runner_skip_unregistered")
            return None

        # 获取任务元数据
        opts = registry.get_options(job_id)

        # 返回验证结果
        return handler, opts

    async def _resolve_intervals(
            self,
            job_id: str,
            scheduled_time: datetime.datetime,
            opts: JobOptions,
            log
    ) -> List[DataInterval]:
        """计算需要执行的时间窗口列表

        Args:
            job_id (str): 任务的唯一标识符
            scheduled_time (datetime.datetime): 计划触发时间
            opts (JobOptions): 任务元数据
            log: 绑定了上下文的日志对象

        Returns:
            List[DataInterval]: 需要执行的时间窗口列表
                - 无状态任务：返回 [DataInterval(scheduled_time, scheduled_time)]（1 个点）
                - 有状态任务：根据 backfill_policy 计算补跑区间（0 到 N 个）
        """
        # 无状态任务：直接返回触发点
        if not opts["stateful"]:
            # 注意：对于无状态任务，start=end 表示触发时间点而非时间段
            return [DataInterval(start=scheduled_time, end=scheduled_time)]

        # 有状态任务：计算回填区间
        intervals = await self._calculate_backfill_intervals(
            job_id, scheduled_time, opts["backfill_policy"]
        )

        # 如果没有需要补的区间（如已处理到最新），返回空列表
        if not intervals:
            log.info("stateful_job_no_interval_needed")
            return []

        log.info("stateful_job_backfill_plan", count=len(intervals), intervals=[str(i) for i in intervals])
        return intervals

    async def _calculate_backfill_intervals(
            self,
            job_id: str,
            trigger_time: datetime.datetime,
            policy: BackfillPolicy
    ) -> List[DataInterval]:
        """计算有状态任务需要补跑的时间窗口列表（回填算法核心）

        Args:
            job_id (str): 任务的唯一标识符
            trigger_time (datetime.datetime): 本次触发时间
            policy (BackfillPolicy): 补跑策略（CATCH_UP 或 SKIP）

        Returns:
            List[DataInterval]: 需要补跑的时间窗口列表（按时间顺序）

        回填算法：
            1. 从数据库加载任务的状态（last_data_time、schedule_expr）
            2. 如果 last_data_time 为 None（首次运行），初始化为 trigger_time 的前一周期
            3. 根据 policy 计算需要补跑的区间：
               - SKIP：只返回最新的一个区间（如果有的话）
               - CATCH_UP：返回从 last_data_time 到 trigger_time 之间的所有区间
            4. 使用 croniter 逐个计算 Cron 周期（避免硬编码时间间隔）

        边界保护：
            - 通过 backfill_max_loops 限制回填次数，防止高频任务长时间阻塞
            - 对于每分钟执行的任务，停机 1 小时会产生 60 个回填区间，
              如果不限制，可能导致任务长时间占用资源

        Note:
            - 使用配置值代替硬编码，防止高频任务长时间回填导致阻塞
            - 如果任务的调度表达式不是 Cron（如 Interval、Date），会降级为单区间模式（interval = [trigger_time, trigger_time]）
        """
        # 从数据库加载任务状态
        state_row = await self._get_job_state(job_id)
        if not state_row:
            # 任务在数据库中不存在（可能被删除或配置错误）
            logger.warning("stateful_job_not_found_in_db", job_id=job_id)
            return []

        # 解析调度表达式（JSON -> Dict）
        try:
            schedule_cfg = json.loads(state_row.schedule_expr)
            cron_expr = schedule_cfg.get("cron")
            if not cron_expr:
                # 调度表达式不是 Cron（如 Interval、Date），降级为单 区间 模式（触发点作为时间窗口）
                return [DataInterval(start=trigger_time, end=trigger_time)]
        except Exception:
            # JSON 解析失败或格式错误，降级为单 区间 模式
            return [DataInterval(start=trigger_time, end=trigger_time)]

        # 获取上次处理的数据截止时间（水位线）
        last_data_time = state_row.last_data_time

        # 首次运行初始化：如果 last_data_time 为 None
        if last_data_time is None:
            cron_iter = croniter(cron_expr, trigger_time)
            prev_time = cron_iter.get_prev(datetime.datetime)
            # 更新数据库中的水位线（避免下次再初始化）
            await self._update_watermark(job_id, prev_time)
            last_data_time = prev_time

        # 根据补跑策略计算回填区间
        if policy == BackfillPolicy.SKIP:
            # 跳过模式：只执行最新的一个周期（如果有的话）
            cron_iter = croniter(cron_expr, trigger_time)
            start = cron_iter.get_prev(datetime.datetime)
            if start > last_data_time:
                # 有漏跑的周期，返回最新一个区间
                return [DataInterval(start=start, end=trigger_time)]
            else:
                # 已处理到最新，无需补
                return []

        # 追赶模式（CATCH_UP）：补齐所有漏跑的周期
        intervals = []
        # 当前的水位线（起始点）
        current_head = last_data_time
        cron_iter = croniter(cron_expr, current_head)

        max_loops = settings.get("backfill_max_loops", 100)
        count = 0

        # 逐个计算 Cron 周期，直到达到 trigger_time 或超过最大循环次数
        while count < max_loops:
            # 计算下一个周期的结束时间
            next_time = cron_iter.get_next(datetime.datetime)

            # 如果下一周期的结束时间 > trigger_time，停止计算
            if next_time > trigger_time:
                break

            # 添加当前周期的时间窗口，时间窗口为 [current_head, next_time)（左闭右开）
            intervals.append(DataInterval(start=current_head, end=next_time))

            # 移动水位线到下周期的起始点
            current_head = next_time
            count += 1

        # 返回回填区间列表（按时间顺序）
        return intervals

    async def _execute_single_run(
            self,
            job_id: str,
            scheduled_time: datetime.datetime,
            data_interval: Optional[DataInterval],
            resource_defs: Dict[str, Any]  # [新增 v0.3]
    ) -> bool:
        """执行单次任务逻辑

        本方法是单次任务执行的核心逻辑，负责：
            1. 获取配置和幂等锁
            2. 创建执行记录
            3. 实例化并注入资源
            4. 调用任务处理函数
            5. 刷新持久化缓冲区
            6. 更新执行记录和 Prometheus 指标

        Args:
            job_id (str): 任务的唯一标识符
            scheduled_time (datetime.datetime): 逻辑触发时间（幂等锁键）
            data_interval (Optional[DataInterval]): 数据时间窗口（有状态任务）或 None（无状态任务）
            resource_defs (Dict[str, Any]): 资源定义字典（参数名 -> Resource 类）

        Returns:
            bool: True（成功）或 False（失败）

        执行流程：
            1. 从缓存获取配置
            2. 尝试获取幂等锁（失败则跳过）
            3. 创建执行记录（job_run 表）
            4. 实例化并注入资源（AsyncExitStack 管理生命周期）：
               a. 遍历 resource_defs，实例化每个 Resource 类
               b. 调用 acquire(ctx) 获取异步上下文管理器
               c. 入栈（enter），获取资源实例
               d. 将资源实例添加到 injected_kwargs 中
            5. 调用任务处理函数（传入 ctx、scheduled_time 和注入的资源）
            6. 刷新持久化缓冲区（persistence_writer.flush）
            7. 更新执行记录（状态、耗时、错误信息）
            8. 记录 Prometheus 指标
            9. 释放资源（AsyncExitStack 自动清理）
        """
        # 从缓存获取配置
        cached_conf = config_cache.get(job_id)
        config_version = cached_conf.version if cached_conf else None

        # 尝试获取幂等锁
        if not await self._acquire_lock(job_id, scheduled_time):
            # 锁获取失败，跳过执行
            return False

        # 创建执行记录（job_run 表）
        # 返回值：run_id（主键）或 None（创建失败）
        run_id = await self._create_run_record(
            job_id, scheduled_time, config_version, data_interval
        )
        if not run_id:
            # 执行记录创建失败，跳过执行
            return False

        # 构建任务上下文（ctx）
        # 设计考量：ctx 是字典，包含任务执行所需的所有元数据
        ctx: Dict[str, Any] = {
            "run_id": run_id,
            "job_id": job_id,
            "config": cached_conf.config_json if cached_conf else {}
        }
        # 如果是有状态任务，添加 data_interval 到上下文
        if data_interval:
            ctx["data_interval"] = data_interval

        # 初始化执行结果变量
        status = "FAILED"
        error_type = None
        error_msg = None

        # 记录开始时间（用于计算耗时）
        start_ts = asyncio.get_running_loop().time()

        # 绑定结构化日志上下文（添加 run_id）
        log = logger.bind(job_id=job_id, run_id=run_id)

        # 资源管理栈
        async with AsyncExitStack() as stack:
            try:
                # 获取任务处理函数
                handler = registry.get_job(job_id)
                if not handler:
                    raise ValueError(f"Job {job_id} handler missing")

                # 如果有配置，使用 Pydantic 验证配置数据
                if cached_conf:
                    typed_config = registry.validate_config(job_id, cached_conf.config_json)
                    ctx["config"] = typed_config

                # 遍历资源定义字典，逐个实例化并注入资源
                injected_kwargs = {}
                for arg_name, res_cls in resource_defs.items():
                    try:
                        # 1. 实例化资源工厂（Resource 类）
                        res_factory = res_cls()

                        # 2. 获取异步上下文管理器（调用 acquire(ctx)）
                        cm = res_factory.acquire(ctx)

                        # 3. 入栈（enter）
                        resource_instance = await stack.enter_async_context(cm)

                        # 4. 将资源实例添加到注入参数字典中
                        injected_kwargs[arg_name] = resource_instance

                    except Exception as e:
                        log.error("resource_injection_failed", resource=arg_name, error=str(e))

                        # 重新抛出异常，外层的 async with stack 会自动清理之前成功的资源
                        raise RuntimeError(f"Failed to inject resource '{arg_name}': {e}") from e

                # 调用任务处理函数，传入 ctx、scheduled_time 和所有注入的资源
                await handler(ctx, scheduled_time, **injected_kwargs)

                # 刷新持久化缓冲区
                await persistence_writer.flush()

                # 任务执行成功，更新状态
                status = "SUCCESS"

            except Exception as e:
                status = "FAILED"
                error_msg = str(e)
                error_type = type(e).__name__
                log.error("job_failed", error=error_msg, exc_info=True)

            finally:
                # 无论成功还是失败，都执行以下清理和记录操作

                # 计算执行耗时（毫秒）
                end_ts = asyncio.get_running_loop().time()
                duration_ms = int((end_ts - start_ts) * 1000)

                # 记录 Prometheus 指标
                JOB_RUN_TOTAL.labels(job_id=job_id, status=status).inc()
                JOB_DURATION_SECONDS.labels(job_id=job_id).observe(end_ts - start_ts)

                # 更新执行记录（job_run 表）
                try:
                    await self._finish_run_record(run_id, status, duration_ms, error_type, error_msg)
                except Exception as e:
                    log.critical("runner_finish_record_failed", error=str(e))

        return status == "SUCCESS"

    async def _get_job_state(self, job_id: str) -> Optional[ScheduledJob]:
        """从数据库获取任务的状态信息

        Args:
            job_id (str): 任务的唯一标识符

        Returns:
            Optional[ScheduledJob]: 任务状态对象（包含 last_data_time、schedule_expr 等），如果任务不存在，返回 None

        Note:
            - 使用 async for 获取会话，自动处理会话的创建和关闭
            - scalar_one_or_none() 确保查询结果最多一条（如果有多条会抛出异常）
        """
        stmt = select(ScheduledJob).where(ScheduledJob.job_id == job_id)
        async for session in get_session():
            result = await session.execute(stmt)
            return result.scalar_one_or_none()
        return None

    async def _update_watermark(self, job_id: str, new_watermark: datetime.datetime):
        """更新任务的数据水位线（last_data_time）

        Args:
            job_id (str): 任务的唯一标识符
            new_watermark (datetime.datetime): 新的水位线时间

        Note:
            - 水位线表示任务已成功处理到的数据截止时间
            - 更新后立即提交事务（commit），确保水位线持久化
        """
        stmt = (
            update(ScheduledJob)
            .where(ScheduledJob.job_id == job_id)
            .values(last_data_time=new_watermark)
        )
        async for session in get_session():
            await session.execute(stmt)
            await session.commit()

    async def _acquire_lock(self, job_id: str, scheduled_time: datetime.datetime) -> bool:
        """尝试获取幂等锁（分布式锁）

        Args:
            job_id (str): 任务的唯一标识符
            scheduled_time (datetime.datetime): 逻辑触发时间（锁键的一部分）

        Returns:
            bool: True（获取成功）或 False（锁已存在）

        设计原理：
            - 锁键为 (job_id, scheduled_time)，利用数据库的唯一索引实现分布式锁
            - 如果插入成功，说明锁获取成功；如果插入失败（IntegrityError），说明锁已存在
            - 锁无超时机制（假设任务最终会完成或被强制终止）

        Note:
            - IntegrityError 是预期的异常（锁已存在），不应记录错误日志
            - 其他异常（如数据库连接失败）返回 False，避免任务重复执行
        """
        stmt = insert(JobLock).values(
            job_id=job_id,
            scheduled_time=scheduled_time,
            owner=self.host_id,
            acquired_at=utcnow()
        )
        try:
            async for session in get_session():
                await session.execute(stmt)
                await session.commit()
                # 锁获取成功
                return True
        except IntegrityError:
            # 锁已存在（唯一索引冲突）
            return False
        except Exception:
            # 其他异常（如数据库连接失败）返回 False，避免任务在不确定的状态下执行
            return False

    async def _create_run_record(
            self, job_id: str, scheduled_time: datetime.datetime, cfg_ver: Optional[int],
            interval: Optional[DataInterval]
    ) -> Optional[int]:
        """创建任务执行记录（job_run 表）

        Args:
            job_id (str): 任务的唯一标识符
            scheduled_time (datetime.datetime): 逻辑触发时间
            cfg_ver (Optional[int]): 配置版本号（用于追踪配置变更）
            interval (Optional[DataInterval]): 数据时间窗口（有状态任务）或 None（无状态任务）

        Returns:
            Optional[int]: 执行记录的主键（run_id）
                如果创建失败，返回 None

        Note:
            - 执行记录的初始状态为 RUNNING（假设任务正在执行）
            - 如果是有状态任务，会记录 data_time_start 和 data_time_end（时间窗口）
            - 记录创建失败会记录错误日志，但不抛出异常（避免任务重复执行）
        """
        # 构建插入值
        values = {
            "job_id": job_id,
            "scheduled_time": scheduled_time,
            "start_time": utcnow(),
            "status": "RUNNING",
            "config_version": cfg_ver,
            "host": self.host_id,
            "pid": os.getpid(),
            # 当前不支持重试，固定为 1
            "attempt": 1
        }
        # 如果是有状态任务，添加数据时间窗口
        if interval:
            values["data_time_start"] = interval.start
            values["data_time_end"] = interval.end

        stmt = insert(JobRun).values(**values)
        try:
            async for session in get_session():
                result = await session.execute(stmt)
                # 获取插入记录的主键（run_id）
                new_id = cast(CursorResult, result).inserted_primary_key[0]
                await session.commit()
                return new_id
        except Exception as e:
            # 记录创建失败（如数据库连接失败）
            logger.error("create_run_record_error", error=str(e))
            return None

    async def _finish_run_record(
            self, run_id: int, status: str, duration_ms: int,
            error_type: str = None, error_msg: str = None
    ):
        """更新任务执行记录的最终状态

        Args:
            run_id (int): 执行记录的主键
            status (str): 最终状态（SUCCESS 或 FAILED）
            duration_ms (int): 执行耗时（毫秒）
            error_type (str): 错误类型（如 ValueError、ConnectionError）
            error_msg (str): 错误消息（截断到 500 字符）

        Note:
            - 错误消息会被截断到 500 字符，防止数据库字段溢出
            - 实际生产中应有专门的错误日志收集系统（如 ELK、Sentry），数据库中的错误信息仅供快速定位问题
        """
        # 构建更新值
        values = {
            "end_time": utcnow(),
            "status": status,
            "duration_ms": duration_ms,
        }
        if error_type:
            values["error_type"] = error_type
        if error_msg:
            # 此处截断是为了防止 DB 字段溢出
            values["error_msg"] = error_msg[:500]

        stmt = update(JobRun).where(JobRun.run_id == run_id).values(**values)
        async for session in get_session():
            await session.execute(stmt)
            await session.commit()


# 创建全局的任务执行引擎实例
job_runner = JobRunner()
