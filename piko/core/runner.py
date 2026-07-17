import asyncio
import datetime
import json
import os
import socket
import uuid
from contextlib import AsyncExitStack
from typing import Any, cast

from croniter import croniter
from sqlalchemy import delete
from sqlalchemy import CursorResult, exists, func, insert, or_, update, select
from sqlalchemy.exc import IntegrityError

from piko.config import settings
from piko.core.cache import ConfigCache
from piko.core.registry import JobHandler, JobRegistry, JobOptions
from piko.core.resource import Resource
from piko.core.types import DataInterval, BackfillPolicy
from piko.infra.db import get_session_context, JobLock, JobRun, ScheduledJob, utcnow
from piko.infra.leader import get_leader_mutex
from piko.infra.logging import LazyLoggerProxy, get_logger
from piko.infra.observability import JOB_RUN_TOTAL, JOB_DURATION_SECONDS
from piko.persistence.writer import PersistenceWriter

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
           e. 刷新持久化缓冲区（Writer）
           f. 更新执行记录（状态、耗时、错误信息）
           g. 记录 Prometheus 指标
           h. 释放资源（AsyncExitStack 自动清理）
        4. 后处理：
           - 有状态任务：成功后更新水位线（last_data_time），失败则中断回填
           - 无状态任务：无后处理

    Attributes:
        host_id (str): 主机标识符，格式为 "hostname:pid"，用于记录任务的执行节点
        registry (JobRegistry): 任务注册中心实例
        config_cache (ConfigCache): 配置缓存实例
        writer (PersistenceWriter): 持久化写入器实例

    Example:
        ```python
        # 在 PikoApp 中初始化
        runner = JobRunner(registry=app.registry, config_cache=app.cache, writer=app.writer)

        # 触发任务
        await runner.run_job("my_task", scheduled_time=datetime(2025, 12, 30, 10, 0, 0))
        ```

    Note:
        - 任务函数必须是协程函数（async def）
        - 资源的生命周期完全由 AsyncExitStack 管理，任务函数无需手动关闭资源
    """

    def __init__(
        self, registry: JobRegistry, config_cache: ConfigCache, writer: PersistenceWriter
    ) -> None:
        """初始化任务执行引擎

        Args:
            registry (JobRegistry): 任务注册中心，用于查找任务处理函数和配置
            config_cache (ConfigCache): 配置缓存，用于获取任务的运行时配置
            writer (PersistenceWriter): 持久化写入器，用于写入任务产生的数据
        """
        # 构建主机标识符：hostname:pid
        self.host_id = f"{socket.gethostname()}:{os.getpid()}"
        self.owner_token = uuid.uuid4().hex

        # 依赖注入
        self.registry = registry
        self.config_cache = config_cache
        self.writer = writer
        self.handler_timeout_s = float(getattr(settings, "job_handler_timeout_s", 300))
        if self.handler_timeout_s <= 0:
            raise ValueError("job_handler_timeout_s must be greater than zero")

    async def run_job(self, job_id: str, scheduled_time: datetime.datetime | None = None) -> None:
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
                opts["resources"],
            )

            # 4. 后处理：失败时中断回填
            if is_stateful:
                if not success:
                    log.warning("backfill_interrupted_by_failure", failed_interval=str(interval))
                    break

    def _validate_and_get_handler(
        self, job_id: str, log: LazyLoggerProxy
    ) -> tuple[JobHandler, JobOptions] | None:
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

        # 白名单检查：任务是否已注册，使用注入的 registry 实例
        handler = self.registry.get_job(job_id)
        if not handler:
            log.warning("runner_skip_unregistered")
            return None

        # 获取任务元数据
        opts = self.registry.get_options(job_id)

        # 返回验证结果
        return handler, opts

    async def _resolve_intervals(
        self,
        job_id: str,
        scheduled_time: datetime.datetime,
        opts: JobOptions,
        log: LazyLoggerProxy,
    ) -> list[DataInterval]:
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

        log.info(
            "stateful_job_backfill_plan",
            count=len(intervals),
            intervals=[str(i) for i in intervals],
        )
        return intervals

    async def _calculate_backfill_intervals(
        self, job_id: str, trigger_time: datetime.datetime, policy: BackfillPolicy
    ) -> list[DataInterval]:
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
        intervals: list[DataInterval] = []
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
        data_interval: DataInterval | None,
        resource_defs: dict[str, type[Resource]],
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
            6. 刷新持久化缓冲区（Writer.flush）
            7. 更新执行记录（状态、耗时、错误信息）
            8. 记录 Prometheus 指标
            9. 释放资源（AsyncExitStack 自动清理）
        """
        # 从缓存获取配置，使用注入的 config_cache 实例
        cached_conf = self.config_cache.get(job_id)
        config_version = cached_conf.version if cached_conf else None

        if settings.leader_enabled and not await get_leader_mutex().verify_fencing_token():
            logger.warning("runner_skip_invalid_leader_fencing", job_id=job_id)
            return False

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
            await self._release_lock(job_id, scheduled_time)
            return False

        heartbeat_task = asyncio.create_task(self._heartbeat_lock(job_id, scheduled_time))

        # 构建任务上下文（ctx）
        # 设计考量：ctx 是字典，包含任务执行所需的所有元数据
        ctx: dict[str, object] = {
            "run_id": run_id,
            "job_id": job_id,
            "config": cached_conf.config_json if cached_conf else {},
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
                # 获取任务处理函数，使用注入的 registry 实例
                handler = self.registry.get_job(job_id)
                if not handler:
                    raise ValueError(f"Job {job_id} handler missing")

                if settings.leader_enabled and not await get_leader_mutex().verify_fencing_token():
                    raise RuntimeError("Leader fencing token is no longer valid")

                # 无论缓存是否命中，都通过 Registry 验证配置；缺失配置按空对象处理，
                # 这样有必填字段的 Schema 会明确失败，有默认值的 Schema 仍能生效。
                config_data = cached_conf.config_json if cached_conf else {}
                typed_config = self.registry.validate_config(job_id, config_data)
                ctx["config"] = typed_config

                # 遍历资源定义字典，逐个实例化并注入资源
                injected_kwargs: dict[str, object] = {}
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
                await asyncio.wait_for(
                    handler(ctx, scheduled_time, **injected_kwargs),
                    timeout=self.handler_timeout_s,
                )

                # 刷新持久化缓冲区，使用注入的 writer 实例
                await self.writer.flush()

                # 任务执行成功，更新状态
                status = "SUCCESS"

            except Exception as e:
                status = "FAILED"
                error_msg = str(e)
                error_type = type(e).__name__
                log.error("job_failed", error=error_msg, exc_info=True)

            finally:
                heartbeat_task.cancel()
                await asyncio.gather(heartbeat_task, return_exceptions=True)

                # 计算执行耗时（毫秒）
                end_ts = asyncio.get_running_loop().time()
                duration_ms = int((end_ts - start_ts) * 1000)

                try:
                    await self._finish_run_record(
                        run_id,
                        job_id,
                        status,
                        duration_ms,
                        error_type,
                        error_msg,
                        data_interval.end if status == "SUCCESS" and data_interval else None,
                    )
                except Exception as e:
                    log.critical("runner_finish_record_failed", error=str(e))
                    status = "FAILED"

                # 记录 Prometheus 指标
                JOB_RUN_TOTAL.labels(job_id=job_id, status=status).inc()
                JOB_DURATION_SECONDS.labels(job_id=job_id).observe(end_ts - start_ts)

                # 释放幂等锁
                await self._release_lock(job_id, scheduled_time, log)

        return status == "SUCCESS"

    async def _get_job_state(self, job_id: str) -> ScheduledJob | None:
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
        async with get_session_context() as session:
            result = await session.execute(stmt)
            return result.scalar_one_or_none()
        return None

    async def _acquire_lock(self, job_id: str, scheduled_time: datetime.datetime) -> bool:
        """尝试获取幂等锁（分布式锁）

        Args:
            job_id (str): 任务的唯一标识符
            scheduled_time (datetime.datetime): 逻辑触发时间（锁键的一部分）

        Returns:
            bool: True（获取成功）或 False（锁已存在）

        设计原理：
            - 锁键为 (job_id, scheduled_time)，利用数据库的唯一索引实现分布式锁
            - 过期锁通过条件更新回收，当前 Worker 使用 owner_token 续租和释放

        Note:
            - IntegrityError 是预期的异常（锁已存在），不应记录错误日志
            - 其他异常（如数据库连接失败）返回 False，避免任务重复执行
        """
        now = utcnow()
        expires_at = now + datetime.timedelta(seconds=settings.job_lock_lease_s)
        update_stmt = (
            update(JobLock)
            .where(
                JobLock.job_id == job_id,
                JobLock.scheduled_time == scheduled_time,
                JobLock.expires_at <= now,
            )
            .values(
                owner=self.host_id,
                owner_token=self.owner_token,
                acquired_at=now,
                expires_at=expires_at,
            )
        )
        insert_stmt = insert(JobLock).values(
            job_id=job_id,
            scheduled_time=scheduled_time,
            owner=self.host_id,
            owner_token=self.owner_token,
            acquired_at=now,
            expires_at=expires_at,
        )
        try:
            async with get_session_context() as session:
                completed = await session.execute(
                    select(JobRun.run_id)
                    .where(
                        JobRun.job_id == job_id,
                        JobRun.scheduled_time == scheduled_time,
                        JobRun.status == "SUCCESS",
                    )
                    .limit(1)
                )
                if completed.scalar_one_or_none() is not None:
                    await session.rollback()
                    return False
                updated = cast(CursorResult[Any], await session.execute(update_stmt))
                if updated.rowcount == 1:
                    await session.commit()
                    return True
                try:
                    await session.execute(insert_stmt)
                    await session.commit()
                    return True
                except IntegrityError:
                    await session.rollback()
                    return False
        except Exception as error:
            logger.warning("job_lock_acquire_error", job_id=job_id, error=str(error))
            return False
        return False

    async def _heartbeat_lock(self, job_id: str, scheduled_time: datetime.datetime) -> None:
        """按固定间隔续租任务锁"""
        interval = max(1, min(settings.job_lock_heartbeat_s, settings.job_lock_lease_s // 2))
        while True:
            await asyncio.sleep(interval)
            now = utcnow()
            expires_at = now + datetime.timedelta(seconds=settings.job_lock_lease_s)
            stmt = (
                update(JobLock)
                .where(
                    JobLock.job_id == job_id,
                    JobLock.scheduled_time == scheduled_time,
                    JobLock.owner == self.host_id,
                    JobLock.owner_token == self.owner_token,
                    JobLock.expires_at > now,
                )
                .values(expires_at=expires_at)
            )
            try:
                async with get_session_context() as session:
                    result = cast(CursorResult[Any], await session.execute(stmt))
                    await session.commit()
                    if result.rowcount != 1:
                        logger.error("job_lock_lease_lost", job_id=job_id)
                        return
            except asyncio.CancelledError:
                raise
            except Exception as error:
                logger.warning("job_lock_heartbeat_error", job_id=job_id, error=str(error))

    async def _release_lock(
        self,
        job_id: str,
        scheduled_time: datetime.datetime,
        log: LazyLoggerProxy | None = None,
    ) -> None:
        """只释放当前 Worker 持有的任务锁"""
        stmt = delete(JobLock).where(
            JobLock.job_id == job_id,
            JobLock.scheduled_time == scheduled_time,
            JobLock.owner == self.host_id,
            JobLock.owner_token == self.owner_token,
        )
        try:
            async with get_session_context() as session:
                await session.execute(stmt)
                await session.commit()
        except Exception as error:
            (log or logger).error("release_lock_failed", error=str(error))

    async def recover_expired_locks(self) -> int:
        """删除已过期的任务锁并返回回收数量"""
        stmt = delete(JobLock).where(JobLock.expires_at <= utcnow())
        try:
            async with get_session_context() as session:
                result = cast(CursorResult[Any], await session.execute(stmt))
                await session.commit()
                return int(result.rowcount or 0)
        except Exception as error:
            logger.warning("job_lock_recovery_error", error=str(error))
            return 0
        return 0

    async def recover_orphaned_runs(self) -> int:
        """将超时未结束的 RUNNING 记录标记为孤儿运行"""
        now = utcnow()
        cutoff = now - datetime.timedelta(seconds=settings.job_run_orphan_timeout_s)
        stmt = (
            update(JobRun)
            .where(
                JobRun.status == "RUNNING",
                JobRun.start_time < cutoff,
                ~exists().where(
                    JobLock.job_id == JobRun.job_id,
                    JobLock.scheduled_time == JobRun.scheduled_time,
                    JobLock.expires_at > now,
                ),
            )
            .values(
                status="ABANDONED",
                end_time=now,
                error_type="OrphanedRun",
                error_msg="运行记录超过租约恢复窗口仍未结束",
            )
        )
        try:
            async with get_session_context() as session:
                result = cast(CursorResult[Any], await session.execute(stmt))
                await session.commit()
                return int(result.rowcount or 0)
        except Exception as error:
            logger.warning("job_run_recovery_error", error=str(error))
            return 0
        return 0

    async def _create_run_record(
        self,
        job_id: str,
        scheduled_time: datetime.datetime,
        cfg_ver: int | None,
        interval: DataInterval | None,
    ) -> int | None:
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
        }
        # 如果是有状态任务，添加数据时间窗口
        if interval:
            values["data_time_start"] = interval.start
            values["data_time_end"] = interval.end

        try:
            async with get_session_context() as session:
                attempt_result = await session.execute(
                    select(func.max(JobRun.attempt)).where(
                        JobRun.job_id == job_id,
                        JobRun.scheduled_time == scheduled_time,
                    )
                )
                max_attempt = attempt_result.scalar_one()
                values["attempt"] = int(max_attempt or 0) + 1
                stmt = insert(JobRun).values(**values)
                result = await session.execute(stmt)
                # 获取插入记录的主键（run_id）
                primary_key = cast(Any, result).inserted_primary_key
                if not primary_key:
                    await session.rollback()
                    return None
                new_id = primary_key[0]
                await session.commit()
                return int(new_id) if new_id is not None else None
        except Exception as e:
            # 记录创建失败（如数据库连接失败）
            logger.error("create_run_record_error", error=str(e))
            return None

    async def _finish_run_record(
        self,
        run_id: int,
        job_id: str,
        status: str,
        duration_ms: int,
        error_type: str | None = None,
        error_msg: str | None = None,
        watermark: datetime.datetime | None = None,
    ) -> None:
        """更新任务执行记录的最终状态

        Args:
            run_id (int): 执行记录的主键
            job_id (str): 任务的唯一标识符
            status (str): 最终状态（SUCCESS 或 FAILED）
            duration_ms (int): 执行耗时（毫秒）
            error_type (str): 错误类型（如 ValueError、ConnectionError）
            error_msg (str): 错误消息（截断到 500 字符）
            watermark (datetime.datetime | None): 成功状态任务要提交的新水位线

        Note:
            - 错误消息会被截断到 500 字符，防止数据库字段溢出
            - 成功记录、水位线 CAS 和一次性任务完成标记在同一事务中提交
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
        async with get_session_context() as session:
            await session.execute(stmt)
            if status == "SUCCESS":
                if watermark is not None:
                    watermark_stmt = (
                        update(ScheduledJob)
                        .where(
                            ScheduledJob.job_id == job_id,
                            or_(
                                ScheduledJob.last_data_time.is_(None),
                                ScheduledJob.last_data_time < watermark,
                            ),
                        )
                        .values(last_data_time=watermark)
                    )
                    watermark_result = cast(
                        CursorResult[Any], await session.execute(watermark_stmt)
                    )
                    if watermark_result.rowcount != 1:
                        current = await session.execute(
                            select(ScheduledJob.last_data_time).where(ScheduledJob.job_id == job_id)
                        )
                        current_watermark = current.scalar_one_or_none()
                        if current_watermark is None or current_watermark < watermark:
                            raise RuntimeError("watermark CAS rejected")

                date_stmt = (
                    update(ScheduledJob)
                    .where(
                        ScheduledJob.job_id == job_id,
                        ScheduledJob.schedule_type == "date",
                        ScheduledJob.enabled.is_(True),
                    )
                    .values(enabled=False, completed_at=utcnow())
                )
                await session.execute(date_stmt)
            await session.commit()
