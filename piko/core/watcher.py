import asyncio
import json
import random
from datetime import datetime, timezone
from typing import Dict

from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger
from sqlalchemy import select

from piko.config import settings
from piko.core.cache import ConfigCache, CachedConfig
from piko.core.registry import JobRegistry
from piko.core.runner import JobRunner
from piko.core.scheduler import SchedulerManager
from piko.infra.db import get_session, ScheduledJob, JobConfig
from piko.infra.logging import get_logger

logger = get_logger(__name__)


class ConfigWatcher:
    """配置监视器与调度器协调器（Reconcile 模式）

    本类负责监听数据库中的任务配置变更，并将变更同步到 APScheduler 和内存缓存中
    采用"协调循环（Reconciliation Loop）"设计模式，定期比对数据库与内存状态，自动处理任务的增删改，确保调度器始终运行最新的配置

    设计模式：
        - 协调循环：类似于 Kubernetes 的 Controller 模式，定期轮询数据库并计算差异（diff）
        - 最终一致性：配置变更不会立即生效，而是在下一个轮询周期后同步
        - 依赖注入：所有外部组件（Scheduler, Cache, Registry, Runner）均通过构造函数注入

    Attributes:
        _scheduler_manager (SchedulerManager): 调度器管理器实例
        _config_cache (ConfigCache): 配置缓存实例
        _registry (JobRegistry): 任务注册中心实例
        _runner (JobRunner): 任务执行引擎实例（用于注册回调）
        _running (bool): 协调循环是否正在运行
        _task (asyncio.Task | None): 协调循环的异步任务

    Note:
        - 本类由 PikoApp 实例化，不再作为全局单例
    """

    def __init__(
            self,
            scheduler_manager: SchedulerManager,
            config_cache: ConfigCache,
            registry: JobRegistry,
            runner: JobRunner
    ):
        """初始化配置监视器

        Args:
            scheduler_manager (SchedulerManager): 负责操作 APScheduler
            config_cache (ConfigCache): 负责更新内存配置
            registry (JobRegistry): 负责白名单校验
            runner (JobRunner): 负责提供任务执行入口（run_job）
        """
        self._scheduler_manager = scheduler_manager
        self._config_cache = config_cache
        self._registry = registry
        self._runner = runner

        # 协调循环的运行标志
        self._running = False
        self._task: asyncio.Task | None = None

    async def start(self):
        """启动配置监视器的协调循环

        创建一个后台异步任务，定期执行协调逻辑（`_watch_loop`）
        """
        if self._running:
            return

        self._running = True
        self._task = asyncio.create_task(self._watch_loop())
        await asyncio.sleep(0)
        logger.info("config_watcher_started")

    async def stop(self):
        """停止配置监视器的协调循环（优雅关闭）

        取消后台协调任务并等待其完全退出
        """
        self._running = False
        if self._task:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
        logger.info("config_watcher_stopped")

    async def _watch_loop(self):
        """协调循环的主逻辑"""
        while self._running:
            try:
                await self._reconcile()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error("watcher_reconcile_error", error=str(e))
                await asyncio.sleep(settings.poll_interval_s)
                continue

            jitter = random.uniform(-settings.poll_jitter_s, settings.poll_jitter_s)
            sleep_time = max(1, settings.poll_interval_s + jitter)
            await asyncio.sleep(sleep_time)

    async def _reconcile(self):
        """执行一次协调操作（同步数据库状态到内存和调度器）"""
        db_jobs: Dict[str, ScheduledJob] = {}
        db_configs: Dict[str, JobConfig] = {}

        async for session in get_session():
            stmt_job = select(ScheduledJob).where(ScheduledJob.enabled.is_(True))
            result_job = await session.execute(stmt_job)

            for row in result_job.scalars():
                # 使用注入的 registry 实例进行白名单检查
                if self._registry.get_job(row.job_id):
                    db_jobs[row.job_id] = row
                else:
                    logger.warning("watcher_skip_unregistered", job_id=row.job_id)

            stmt_cfg = select(JobConfig)
            result_cfg = await session.execute(stmt_cfg)
            for row in result_cfg.scalars():
                db_configs[row.job_id] = row

        self._sync_config_cache(db_configs)

        # 使用注入的 config_cache 实例
        self._config_cache.prune(set(db_configs.keys()))

        self._sync_scheduler(db_jobs)

    def _sync_config_cache(self, db_configs: Dict[str, JobConfig]):
        """更新内存中的配置缓存"""
        for job_id, row in db_configs.items():
            now_utc = datetime.now(timezone.utc).replace(tzinfo=None)
            effective = row.effective_from
            if effective and effective > now_utc:
                continue

            cached = CachedConfig(
                config_json=row.config_json,
                version=row.version,
                schema_version=row.schema_version
            )
            # 使用注入的 config_cache 实例
            self._config_cache.set(job_id, cached)

    def _sync_scheduler(self, db_jobs: Dict[str, ScheduledJob]):
        """同步任务到 APScheduler（增删改）"""
        # 使用注入的 scheduler_manager 实例
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

            if existing_job.name != current_version_tag:
                new_trigger = self._build_trigger(row)
                if new_trigger:
                    raw_scheduler.reschedule_job(job_id, trigger=new_trigger)
                    raw_scheduler.modify_job(job_id, name=current_version_tag)
                    logger.info("watcher_job_rescheduled", job_id=job_id, version=row.version)

    def _add_job_to_scheduler(self, row: ScheduledJob):
        """添加任务到 APScheduler"""
        trigger = self._build_trigger(row)
        if not trigger:
            return

        # 使用注入的 runner 实例的 run_job 方法
        self._scheduler_manager.raw_scheduler.add_job(
            func=self._runner.run_job,
            trigger=trigger,
            id=row.job_id,
            args=[row.job_id],
            name=f"v{row.version}"
        )

        logger.info("watcher_job_added", job_id=row.job_id, trigger=str(trigger))

    def _build_trigger(self, row: ScheduledJob):
        """构建 APScheduler 触发器"""
        try:
            kwargs = json.loads(row.schedule_expr)
            if row.schedule_type == 'cron':
                return CronTrigger(**kwargs, timezone=settings.timezone)
            elif row.schedule_type == 'interval':
                return IntervalTrigger(**kwargs, timezone=settings.timezone)
            elif row.schedule_type == 'date':
                return DateTrigger(**kwargs, timezone=settings.timezone)
            else:
                logger.error("unknown_schedule_type", job_id=row.job_id, type=row.schedule_type)
                return None
        except Exception as e:
            logger.error("trigger_build_failed", job_id=row.job_id, error=str(e))
            return None
