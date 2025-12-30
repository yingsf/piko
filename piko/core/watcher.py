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
from piko.core.cache import config_cache, CachedConfig
from piko.core.registry import registry
from piko.core.runner import job_runner
from piko.core.scheduler import scheduler_manager
from piko.infra.db import get_session, ScheduledJob, JobConfig
from piko.infra.logging import get_logger

logger = get_logger(__name__)


class ConfigWatcher:
    """配置监视器与调度器协调器（Reconcile 模式）

    本类负责监听数据库中的任务配置变更，并将变更同步到 APScheduler 和内存缓存中
    采用"协调循环（Reconciliation Loop）"设计模式，定期比对数据库与内存状态，自动处理任务的增删改，确保调度器始终运行最新的配置

    设计模式：
        - 协调循环：类似于 Kubernetes 的 Controller 模式，不依赖数据库触发器或消息队列，而是定期轮询数据库并计算差异（diff）
        - 最终一致性：配置变更不会立即生效，而是在下一个轮询周期（默认 5 秒）后同步，适用于任务调度这种非实时场景
        - 幂等性：每次协调操作都是幂等的，即使多次执行也不会产生副作用

    核心职责：
        1. 配置同步：将数据库中的 JobConfig 同步到内存缓存（ConfigCache）
        2. 调度器同步：将数据库中的 ScheduledJob 同步到 APScheduler（增删改调度任务）
        3. 内存管理：定期清理不活跃任务的缓存，防止内存泄漏
        4. 异常恢复：轮询模式天然支持从异常中恢复，无需额外的重试机制

    工作流程：
        1. 启动协调循环（`start`）
        2. 每隔 `poll_interval_s` 秒执行一次 `_reconcile`：
           a. 从数据库加载所有 enabled=True 的任务和配置
           b. 对比内存状态，计算增删改集合
           c. 更新 ConfigCache 和 APScheduler
        3. 如果发生异常，记录日志并继续下一轮循环（容错）
        4. 优雅关闭时（`stop`）取消协调任务

    性能优化：
        - 轮询抖动：在轮询间隔上增加随机偏移（±jitter_s），避免多实例同时查询数据库造成峰值负载
        - 集合运算优化：使用集合的差集、交集运算高效计算增删改列表（O(n) 时间复杂度）
        - 批量操作：一次轮询处理所有变更，减少数据库查询次数

    容错设计：
        - 协调循环中的异常不会导致进程退出，而是记录日志并等待下一轮重试
        - CancelledError 会立即中断循环，避免优雅关闭时被捕获
        - 数据库查询失败不会影响已加载的任务继续运行（调度器独立于数据库）

    Attributes:
        _running (bool): 协调循环是否正在运行
        _task (asyncio.Task | None): 协调循环的异步任务
        _scheduler (AsyncIOScheduler): APScheduler 的原始调度器实例

    Example:
        ```python
        from piko.core.watcher import config_watcher

        # 启动监视器
        await config_watcher.start()

        # 监视器会自动同步数据库变更到 APScheduler
        # 例如：在数据库中新增一个任务，5 秒后会自动添加到调度器

        # 优雅关闭
        await config_watcher.stop()
        ```

    Note:
        - 本类应作为全局单例使用（见模块底部的 `config_watcher = ConfigWatcher()`）
        - 轮询间隔（poll_interval_s）应根据配置变更频率调整：
          频繁变更时可设为 5-10 秒，稳定后可增加到 30-60 秒
        - 不推荐轮询间隔小于 1 秒，会对数据库造成不必要的压力
    """

    def __init__(self):
        """初始化配置监视器

        创建协调循环所需的状态标志和调度器引用
        """
        # 协调循环的运行标志
        self._running = False

        # 协调循环的异步任务
        self._task: asyncio.Task | None = None

        # APScheduler 的原始调度器实例
        self._scheduler = scheduler_manager.raw_scheduler

    async def start(self):
        """启动配置监视器的协调循环

        创建一个后台异步任务，定期执行协调逻辑（`_watch_loop`）
        """
        # 幂等性检查：如果已经启动，直接返回
        if self._running:
            return

        # 设置运行标志
        self._running = True

        # 创建后台协调任务
        self._task = asyncio.create_task(self._watch_loop())

        # 让出控制权给事件循环，确保任务被正确调度
        await asyncio.sleep(0)

        # 记录启动日志
        logger.info("config_watcher_started")

    async def stop(self):
        """停止配置监视器的协调循环（优雅关闭）

        取消后台协调任务并等待其完全退出，确保不会留下僵尸任务

        """
        # 设置停止标志
        self._running = False

        # 如果存在协调任务，则取消它
        if self._task:
            # 请求取消任务
            self._task.cancel()

            # 等待任务取消完成
            await asyncio.gather(self._task, return_exceptions=True)

        # 记录停止日志
        logger.info("config_watcher_stopped")

    async def _watch_loop(self):
        """协调循环的主逻辑（无限循环，直到 `_running` 为 False）

        定期执行协调操作（`_reconcile`），并在两次协调之间休眠一段时间，如果协调过程中发生异常，会记录日志并继续下一轮（容错）

        工作流程：
            1. 执行一次协调（`_reconcile`）
            2. 如果发生异常：
               - CancelledError: 立即退出（优雅关闭）
               - 其他异常: 记录日志，休眠后继续
            3. 计算休眠时间（基础间隔 + 抖动）
            4. 休眠，然后回到步骤 1

        Note:
            - 协调循环是无限的，只有在 `_running` 变为 False 或收到 CancelledError 时才会退出
            - 异常恢复机制确保单次协调失败不会导致整个监视器停止工作
        """
        # 无限循环，直到 _running 变为 False
        while self._running:
            try:
                # 执行一次协调操作
                await self._reconcile()

            except asyncio.CancelledError:
                # 任务被取消时直接退出
                raise

            except Exception as e:
                logger.error("watcher_reconcile_error", error=str(e))

                # 发生异常后立即休眠，避免错误快速重试造成资源浪费
                await asyncio.sleep(settings.poll_interval_s)
                continue

            # 计算下一次协调前的休眠时间
            jitter = random.uniform(-settings.poll_jitter_s, settings.poll_jitter_s)

            # 最终休眠时间
            sleep_time = max(1, settings.poll_interval_s + jitter)

            # 休眠（让出控制权给事件循环）
            await asyncio.sleep(sleep_time)

    async def _reconcile(self):
        """执行一次协调操作（同步数据库状态到内存和调度器）

        工作流程：
            1. 从数据库加载所有 enabled=True 的 ScheduledJob 和 JobConfig
            2. 过滤出已注册的任务（白名单检查）
            3. 同步配置到 ConfigCache
            4. 清理不活跃任务的缓存（内存管理）
            5. 同步任务到 APScheduler（增删改）

        Note:
            - 本方法是协调循环的核心，所有数据库到内存的同步都在此完成
            - 如果数据库查询失败，会抛出异常并被 `_watch_loop` 捕获
        """
        # 初始化字典用于存储数据库结果
        db_jobs: Dict[str, ScheduledJob] = {}
        db_configs: Dict[str, JobConfig] = {}

        # 从数据库加载数据
        async for session in get_session():
            # 查询所有启用的任务
            stmt_job = select(ScheduledJob).where(ScheduledJob.enabled.is_(True))
            result_job = await session.execute(stmt_job)

            # 遍历查询结果，执行白名单检查
            for row in result_job.scalars():
                # 检查任务是否已在 Registry 中注册（白名单检查）
                if registry.get_job(row.job_id):
                    db_jobs[row.job_id] = row
                else:
                    logger.warning("watcher_skip_unregistered", job_id=row.job_id)

            # 查询所有任务的配置（不过滤 enabled 状态）
            stmt_cfg = select(JobConfig)
            result_cfg = await session.execute(stmt_cfg)
            for row in result_cfg.scalars():
                db_configs[row.job_id] = row

        # 同步配置到内存缓存
        self._sync_config_cache(db_configs)

        # 清理不活跃任务的缓存（内存管理）
        config_cache.prune(set(db_configs.keys()))

        # 同步任务到 APScheduler
        self._sync_scheduler(db_jobs)

    def _sync_config_cache(self, db_configs: Dict[str, JobConfig]):
        """更新内存中的配置缓存

        遍历数据库中的所有配置，检查生效时间后写入 ConfigCache

        Args:
            db_configs (Dict[str, JobConfig]): job_id -> 配置对象的映射（来自数据库）

        生效时间逻辑：
            - 如果配置有 `effective_from` 字段且时间未到，则跳过（延迟生效）
            - 否则，立即写入缓存
        """
        # 遍历数据库中的所有配置
        for job_id, row in db_configs.items():
            # 获取当前 UTC 时间（Naive，无时区信息）
            now_utc = datetime.now(timezone.utc).replace(tzinfo=None)

            # 检查配置的生效时间
            effective = row.effective_from
            if effective and effective > now_utc:
                # 配置未到生效时间，跳过（延迟生效）
                continue

            # 将配置封装为 CachedConfig（不可变对象）
            cached = CachedConfig(
                config_json=row.config_json,
                version=row.version,
                schema_version=row.schema_version
            )

            # 写入缓存
            config_cache.set(job_id, cached)

    def _sync_scheduler(self, db_jobs: Dict[str, ScheduledJob]):
        """同步任务到 APScheduler（增删改）

        比对数据库中的任务与 APScheduler 中的任务，计算增删改集合，
        并执行相应的操作（添加、删除、重新调度）

        Args:
            db_jobs (Dict[str, ScheduledJob]): job_id -> 任务对象的映射（来自数据库）

        同步策略（三态同步）：
            1. to_add：数据库有但调度器没有的任务 → 添加到调度器
            2. to_remove：调度器有但数据库没有的任务 → 从调度器删除
            3. to_update：数据库和调度器都有的任务 → 检查版本号，如有变更则重新调度
        """
        # 获取 APScheduler 中所有任务的 ID
        ap_job_ids = {job.id for job in self._scheduler.get_jobs()}

        # 获取数据库中所有任务的 ID
        db_job_ids = set(db_jobs)

        # 计算需要添加的任务（差集：db - ap）
        to_add = db_job_ids - ap_job_ids
        for job_id in to_add:
            # 添加任务到调度器
            self._add_job_to_scheduler(db_jobs[job_id])

        # 计算需要删除的任务（差集：ap - db）
        to_remove = ap_job_ids - db_job_ids
        for job_id in to_remove:
            # 从调度器删除任务
            self._scheduler.remove_job(job_id)
            logger.info("watcher_job_removed", job_id=job_id)

        # 计算需要更新的任务（交集：db ∩ ap）
        to_update = db_job_ids & ap_job_ids
        for job_id in to_update:
            # 获取数据库中的任务对象
            row = db_jobs[job_id]

            # 获取调度器中的任务对象
            existing_job = self._scheduler.get_job(job_id)

            # 构建当前版本标签（如 "v5"）
            current_version_tag = f"v{row.version}"

            # 如果版本不同，说明配置已变更，需要重新调度
            if existing_job.name != current_version_tag:
                # 构建新的触发器（基于新的调度表达式）
                new_trigger = self._build_trigger(row)

                # 重新调度任务（更新触发器）
                self._scheduler.reschedule_job(job_id, trigger=new_trigger)

                # 更新任务的版本标签
                self._scheduler.modify_job(job_id, name=current_version_tag)

                # 记录重新调度日志
                logger.info("watcher_job_rescheduled", job_id=job_id, version=row.version)

    def _add_job_to_scheduler(self, row: ScheduledJob):
        """添加任务到 APScheduler

        根据数据库中的任务配置，构建触发器并添加到调度器

        Args:
            row (ScheduledJob): 数据库中的任务对象

        Note:
            - 如果触发器构建失败（如调度表达式无效），则跳过添加并记录错误日志
            - 任务的执行函数固定为 `job_runner.run_job`，参数为 `job_id`
            - 任务的版本标签存储在 `name` 字段中，用于版本比对
        """
        # 构建触发器（根据调度类型和表达式）
        trigger = self._build_trigger(row)
        if not trigger:
            # 触发器构建失败，跳过添加
            return

        # 添加任务到调度器
        self._scheduler.add_job(
            func=job_runner.run_job,
            trigger=trigger,
            id=row.job_id,
            args=[row.job_id],
            name=f"v{row.version}"
        )

        # 记录添加日志
        logger.info("watcher_job_added", job_id=row.job_id, trigger=str(trigger))

    def _build_trigger(self, row: ScheduledJob):
        """根据数据库中的调度表达式构建 APScheduler 触发器

        Args:
            row (ScheduledJob): 数据库中的任务对象

        Returns:
            Trigger | None: APScheduler 的触发器对象（CronTrigger、IntervalTrigger、DateTrigger），如果构建失败，返回 None

        调度类型：
            - cron: 使用 Cron 表达式（如 "0 0 * * *" 表示每天午夜）
            - interval: 使用固定间隔（如 {"seconds": 30} 表示每 30 秒）
            - date: 使用固定日期时间（如 {"run_date": "2025-12-31 23:59:59"}）

        Note:
            - 调度表达式存储为 JSON 字符串，需要解析为字典后传递给触发器
            - 如果表达式格式错误或类型未知，会记录错误日志并返回 None
        """
        try:
            # 解析调度表达式（JSON -> Dict）
            kwargs = json.loads(row.schedule_expr)

            # 根据调度类型构建相应的触发器
            if row.schedule_type == 'cron':
                # Cron 触发器：支持标准 Cron 表达式
                return CronTrigger(**kwargs, timezone=settings.timezone)
            elif row.schedule_type == 'interval':
                # Interval 触发器：固定时间间隔重复执行
                return IntervalTrigger(**kwargs, timezone=settings.timezone)
            elif row.schedule_type == 'date':
                # Date 触发器：在指定的日期时间执行一次
                return DateTrigger(**kwargs, timezone=settings.timezone)
            else:
                # 未知的调度类型，记录错误日志
                logger.error("unknown_schedule_type", job_id=row.job_id, type=row.schedule_type)
                return None

        except Exception as e:
            logger.error("trigger_build_failed", job_id=row.job_id, error=str(e))
            return None


# 创建全局的配置监视器实例
config_watcher = ConfigWatcher()
