import asyncio
import json
import math
import secrets
from datetime import datetime, timezone
from typing import TypeAlias, cast

from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from piko.config import settings
from piko.core.cache import ConfigCache, CachedConfig
from piko.core.registry import JobRegistry
from piko.core.runner import JobRunner
from piko.core.scheduler import SchedulerManager
from piko.infra.db import get_session_context, JobConfig, ScheduledJob
from piko.infra.leader import get_leader_mutex
from piko.infra.logging import get_logger
from piko.infra.observability import CONFIG_RECONCILE_TOTAL

logger = get_logger(__name__)
_jitter_source = secrets.SystemRandom()

Trigger: TypeAlias = CronTrigger | DateTrigger | IntervalTrigger
SUPPORTED_EXECUTORS = frozenset({"default", "cpu", "io"})


class ConfigWatcher:
    """配置监视器与调度器协调器（Reconcile 模式）

    本类负责监听数据库中的任务配置变更，并将变更同步到 APScheduler 和内存缓存中
    采用"协调循环（Reconciliation Loop）"设计模式，定期比对数据库与内存状态，自动处理任务的增删改

    Attributes:
        _scheduler_manager (SchedulerManager): 调度器管理器实例
        _config_cache (ConfigCache): 配置缓存实例
        _registry (JobRegistry): 任务注册中心实例
        _runner (JobRunner): 任务执行引擎实例
        _running (bool): 协调循环是否正在运行
        _task (asyncio.Task | None): 协调循环的异步任务
        _dynamic_interval (float): 动态轮询间隔，可由系统配置覆盖
    """

    def __init__(
        self,
        scheduler_manager: SchedulerManager,
        config_cache: ConfigCache,
        registry: JobRegistry,
        runner: JobRunner,
    ) -> None:
        """初始化配置监视器

        Args:
            scheduler_manager (SchedulerManager): 负责操作 APScheduler
            config_cache (ConfigCache): 负责更新内存配置
            registry (JobRegistry): 负责白名单校验
            runner (JobRunner): 负责提供任务执行入口
        """
        self._scheduler_manager = scheduler_manager
        self._config_cache = config_cache
        self._registry = registry
        self._runner = runner

        self._running = False
        self._task: asyncio.Task[None] | None = None

        # 动态轮询间隔从静态配置初始化，并在后续协调中受控更新。
        self._dynamic_interval = settings.poll_interval_s
        self._sync_initialized = False
        self._last_sync_at: datetime | None = None
        self._known_jobs: dict[str, ScheduledJob] = {}
        self._known_configs: dict[str, JobConfig] = {}

    async def start(self) -> None:
        """启动配置监视器的协调循环"""
        if self._running:
            return

        self._running = True
        self._task = asyncio.create_task(self._watch_loop())
        await asyncio.sleep(0)
        logger.info("config_watcher_started", interval=self._dynamic_interval)

    async def stop(self) -> None:
        """停止配置监视器的协调循环"""
        self._running = False
        if self._task:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
        logger.info("config_watcher_stopped")

    @property
    def is_running(self) -> bool:
        """返回配置协调循环是否正在运行。"""
        return self._running

    @property
    def dynamic_interval(self) -> float:
        """返回当前生效的配置轮询间隔。"""
        return float(self._dynamic_interval)

    async def _watch_loop(self) -> None:
        """协调循环的主逻辑"""
        while self._running:
            try:
                await self._reconcile()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error("watcher_reconcile_error", error=str(e))
                CONFIG_RECONCILE_TOTAL.labels(result="failed").inc()
                # 异常时退避到默认间隔，防止数据库故障导致死循环风暴
                await asyncio.sleep(settings.poll_interval_s)
                continue

            # 使用最后一次通过校验的动态间隔，避免读取失败时中断协调循环。
            jitter = _jitter_source.uniform(-settings.poll_jitter_s, settings.poll_jitter_s)
            sleep_time = max(1, self._dynamic_interval + jitter)
            await asyncio.sleep(sleep_time)

    async def _reconcile(self) -> None:
        """执行一次协调操作（同步数据库状态到内存和调度器）"""
        if settings.leader_enabled and not get_leader_mutex().is_leader:
            self._clear_scheduler_jobs()
            CONFIG_RECONCILE_TOTAL.labels(result="no_change").inc()
            return

        async with get_session_context() as session:
            sync_anchor = await self._database_now(session)
            if self._sync_initialized and self._last_sync_at is not None:
                job_filter = ScheduledJob.updated_at >= self._last_sync_at
                config_filter = JobConfig.updated_at >= self._last_sync_at
                stmt_job = select(ScheduledJob).where(job_filter)
                stmt_cfg = select(JobConfig).where(config_filter)
            else:
                stmt_job = select(ScheduledJob)
                stmt_cfg = select(JobConfig)

            # 1. 增量读取任务调度与配置；首次协调执行全量初始化。
            result_job = await session.execute(stmt_job)
            for row in result_job.scalars():
                if row.enabled and self._registry.get_job(row.job_id):
                    self._known_jobs[row.job_id] = row
                elif not row.enabled:
                    self._known_jobs.pop(row.job_id, None)
                else:
                    logger.warning("watcher_skip_unregistered", job_id=row.job_id)

            result_cfg = await session.execute(stmt_cfg)
            for row in result_cfg.scalars():
                self._known_configs[row.job_id] = row

            self._last_sync_at = sync_anchor
            self._sync_initialized = True

        self._sync_system_config()
        self._sync_config_cache(self._known_configs)
        self._config_cache.prune(set(self._known_configs.keys()))
        self._sync_scheduler(self._known_jobs)
        CONFIG_RECONCILE_TOTAL.labels(result="success").inc()

    async def _database_now(self, session: AsyncSession) -> datetime:
        """读取增量同步边界时间

        与 ``ScheduledJob.updated_at``（由 ``piko.infra.db.utcnow`` 即 Python 端
        UTC 时间写入）使用同一时钟源，避免跨主机时钟偏差导致增量过滤
        （``updated_at >= last_sync_at``）漏掉刚写入的变更。
        """
        from piko.infra.db import utcnow

        return utcnow()

    def _sync_system_config(self) -> None:
        """应用系统级动态配置中唯一受支持的轮询间隔。"""
        row = self._known_configs.get("piko_system_settings")
        if row is None:
            return
        self.apply_system_config(row.config_json)

    def apply_system_config(self, config_data: object) -> None:
        """校验并应用受支持的系统级动态配置。"""
        if isinstance(config_data, (str, bytes)):
            config_data = json.loads(config_data)
        if not isinstance(config_data, dict):
            logger.warning("system_config_invalid", field="poll_interval_s")
            return

        typed_config = cast(dict[str, object], config_data)
        new_interval = typed_config.get("poll_interval_s")
        if (
            isinstance(new_interval, (int, float))
            and not isinstance(new_interval, bool)
            and math.isfinite(new_interval)
            and 1 <= new_interval <= 3600
        ):
            if new_interval != self._dynamic_interval:
                logger.info("system_poll_interval_changed", interval_s=new_interval)
                self._dynamic_interval = float(new_interval)
        elif new_interval is not None:
            logger.warning("system_config_invalid", field="poll_interval_s")

    def _clear_scheduler_jobs(self) -> None:
        """Follower 失去调度资格时清除内存定时器。"""
        raw_scheduler = self._scheduler_manager.raw_scheduler
        for job in raw_scheduler.get_jobs():
            raw_scheduler.remove_job(job.id)
        if raw_scheduler.get_jobs():
            logger.warning("watcher_follower_scheduler_clear_incomplete")

    async def reconcile_once(self) -> None:
        """执行一轮配置协调

        该方法用于需要显式推进配置状态的管理操作。
        """
        try:
            await self._reconcile()
        except Exception:
            CONFIG_RECONCILE_TOTAL.labels(result="failed").inc()
            raise

    def _sync_config_cache(self, db_configs: dict[str, JobConfig]) -> None:
        """更新内存中的配置缓存"""
        for job_id, row in db_configs.items():
            now_utc = datetime.now(timezone.utc).replace(tzinfo=None)
            effective = row.effective_from
            if effective and effective > now_utc:
                continue

            cached = CachedConfig(
                config_json=row.config_json, version=row.version, schema_version=row.schema_version
            )
            self._config_cache.set(job_id, cached)

    def _sync_scheduler(self, db_jobs: dict[str, ScheduledJob]) -> None:
        """同步任务到 APScheduler（增删改）"""
        raw_scheduler = self._scheduler_manager.raw_scheduler

        ap_job_ids = {job.id for job in raw_scheduler.get_jobs()}
        db_job_ids = set(db_jobs)

        to_add = db_job_ids - ap_job_ids
        for job_id in to_add:
            self._add_job_to_scheduler(db_jobs[job_id])

        to_remove = ap_job_ids - db_job_ids
        for job_id in to_remove:
            raw_scheduler.remove_job(job_id)
            logger.info("watcher_job_removed", job_id=job_id)

        to_update = db_job_ids & ap_job_ids
        for job_id in to_update:
            row = db_jobs[job_id]
            existing_job = raw_scheduler.get_job(job_id)
            current_version_tag = f"v{row.version}"

            if existing_job is not None and existing_job.name != current_version_tag:
                new_trigger = self._build_trigger(row)
                job_options = self._job_options(row)
                if new_trigger and job_options is not None:
                    raw_scheduler.reschedule_job(job_id, trigger=new_trigger)
                    raw_scheduler.modify_job(
                        job_id,
                        name=current_version_tag,
                        **job_options,
                    )
                    logger.info("watcher_job_rescheduled", job_id=job_id, version=row.version)

    def _add_job_to_scheduler(self, row: ScheduledJob) -> None:
        """添加任务到 APScheduler"""
        trigger = self._build_trigger(row)
        if not trigger:
            return
        job_options = self._job_options(row)
        if job_options is None:
            return

        self._scheduler_manager.raw_scheduler.add_job(
            func=self._runner.run_job,
            trigger=trigger,
            id=row.job_id,
            args=[row.job_id],
            name=f"v{row.version}",
            **job_options,
        )

        logger.info("watcher_job_added", job_id=row.job_id, trigger=str(trigger))

    def _build_trigger(self, row: ScheduledJob) -> Trigger | None:
        """构建 APScheduler 触发器"""
        try:
            raw_kwargs = json.loads(row.schedule_expr)
            if not isinstance(raw_kwargs, dict):
                raise ValueError("schedule_expr must be a JSON object")
            kwargs = cast(dict[str, object], raw_kwargs)
            kwargs["timezone"] = row.timezone
            if row.schedule_type != "date":
                kwargs["jitter"] = row.jitter_s or None
            elif row.jitter_s:
                raise ValueError("date trigger does not support jitter_s")
            if row.schedule_type == "cron":
                return CronTrigger(**kwargs)
            elif row.schedule_type == "interval":
                return IntervalTrigger(**kwargs)
            elif row.schedule_type == "date":
                return DateTrigger(**kwargs)
            else:
                logger.error("unknown_schedule_type", job_id=row.job_id, type=row.schedule_type)
                return None
        except Exception as e:
            logger.error("trigger_build_failed", job_id=row.job_id, error=str(e))
            return None

    def _job_options(self, row: ScheduledJob) -> dict[str, object] | None:
        """校验并生成 APScheduler 的逐任务参数"""
        if row.executor not in SUPPORTED_EXECUTORS:
            logger.error("unsupported_job_executor", job_id=row.job_id, executor=row.executor)
            return None
        if row.misfire_grace_s < 0 or row.max_instances < 1 or row.jitter_s < 0:
            logger.error("invalid_job_scheduler_options", job_id=row.job_id)
            return None
        return {
            "executor": row.executor,
            "misfire_grace_time": row.misfire_grace_s,
            "coalesce": row.coalesce,
            "max_instances": row.max_instances,
        }
