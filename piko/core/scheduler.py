import datetime

from apscheduler.executors.asyncio import AsyncIOExecutor
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from piko.config import settings
from piko.infra.logging import get_logger

logger = get_logger(__name__)


class PikoExecutor(AsyncIOExecutor):
    """自定义 APScheduler 执行器（解决调度时间漂移问题）

    本类继承自 APScheduler 的 AsyncIOExecutor，通过"劫持"任务提交逻辑，
    将 APScheduler 计算的精准触发时间（scheduled_time）注入到任务参数中

    解决方案：
        通过覆盖 `_do_submit_job` 方法，在任务提交前将 `run_times[0]`（APScheduler 计算的精准触发时间）
        注入到任务的 kwargs 中，确保任务函数能访问到"神圣不可侵犯"的计划时间

    Attributes:
        无新增属性，完全继承自 AsyncIOExecutor

    Example:
        ```python
        # 在 SchedulerManager 中使用自定义执行器
        executors = {
            'default': PikoExecutor()
        }
        scheduler = AsyncIOScheduler(executors=executors)

        # 任务函数会自动接收 scheduled_time 参数
        @scheduler.scheduled_job('cron', hour=10, minute=0)
        async def my_task(scheduled_time=None):
            print(f"计划触发时间: {scheduled_time}")
            print(f"实际执行时间: {datetime.utcnow()}")
            # 即使任务延迟执行，scheduled_time 仍是 10:00
        ```

    Note:
        - `scheduled_time` 参数会自动注入到所有通过此执行器运行的任务中
        - 任务函数可以选择性地接收此参数（如果不需要可以不声明）
        - 时区转换统一为 UTC Naive，与 Piko 的数据库约定一致
    """

    def _do_submit_job(self, job, run_times):
        """提交任务到事件循环前的预处理（注入 scheduled_time）

        Args:
            job: APScheduler 的 Job 对象，包含任务的所有信息（func、args、kwargs 等）
            run_times (list[datetime]): 本次触发的计划时间列表（通常只有一个元素）

        工作流程：
            1. 从 run_times 中提取第一个时间（精准触发时间）
            2. 转换为 UTC Naive 时间（去除时区信息）
            3. 注入到 job.kwargs 中（键名为 "scheduled_time"）
            4. 调用父类方法继续执行标准逻辑（提交到事件循环）

        Note:
            - run_times 通常只有一个元素，因为 `coalesce=True` 会合并积压的任务
            - 如果 run_times 为空（极端情况），scheduled_time 会被设为 None
        """
        # 1. 获取 APScheduler 计算出的精准触发时间
        scheduled_time: datetime.datetime | None = run_times[0] if run_times else None

        # 2. 转换为 Naive UTC 以匹配 Piko DB 规范
        if scheduled_time and scheduled_time.tzinfo:
            # 先转换为 UTC 时区，再去除时区信息（replace(tzinfo=None)）
            scheduled_time = scheduled_time.astimezone(datetime.timezone.utc).replace(tzinfo=None)

        # 3. 注入到 kwargs 中
        job.kwargs["scheduled_time"] = scheduled_time

        # 4. 调用父类继续执行标准逻辑
        super()._do_submit_job(job, run_times)


class SchedulerManager:
    """APScheduler 适配层（调度器管理器）

    本类封装了 APScheduler 的初始化和生命周期管理，将 APScheduler 作为纯内存的"时间轮触发器"使用，
    所有持久化状态由 MySQL（scheduled_job 表）和 ConfigWatcher 维护

    配置参数说明：
        - jobstores：任务存储后端（使用 MemoryJobStore，重启即空）
        - executors：任务执行器（使用自定义 PikoExecutor，注入 scheduled_time）
        - job_defaults：任务的默认配置：
          - `coalesce=True`：积压合并（多次触发合并为一次执行）
          - `max_instances=1`：默认单实例（具体并发控制由 JobRunner 的锁管理）
          - `misfire_grace_time`：Misfire 宽限时间（任务延迟多久后不再执行）
        - timezone：调度器的时区（如 Asia/Shanghai），影响 Cron 表达式的解析

    Attributes:
        _scheduler (AsyncIOScheduler): APScheduler 的原始调度器实例

    Example:
        ```python
        from piko.core.scheduler import scheduler_manager

        # 启动调度器
        scheduler_manager.startup()

        # 获取原始调度器（供 ConfigWatcher 使用）
        raw = scheduler_manager.raw_scheduler
        raw.add_job(func=my_task, trigger='cron', hour=10)

        # 优雅关闭
        scheduler_manager.shutdown()
        ```

    Note:
        - 本类应作为全局单例使用（见模块底部的 `scheduler_manager = SchedulerManager()`）
        - 调度器启动后会自动创建后台线程（事件循环），无需手动管理
        - 优雅关闭时（`shutdown(wait=True)`）会等待在途任务执行完毕
    """

    def __init__(self):
        """初始化调度器管理器

        配置并创建 APScheduler 实例，但不启动（需手动调用 `startup`）
        """
        # 配置任务存储：使用纯内存存储，重启即空
        jobstores = {
            "default": MemoryJobStore()
        }

        # 配置任务执行器：使用自定义 PikoExecutor
        executors = {
            "default": PikoExecutor()
        }

        # 配置任务的默认参数
        job_defaults = {
            # coalesce=True: 积压合并
            "coalesce": True,

            # max_instances=1: 默认单实例
            "max_instances": 1,

            # misfire_grace_time: Misfire 宽限时间（单位：秒）
            "misfire_grace_time": settings.ap_misfire_grace_s_default,
        }

        # 创建 APScheduler 实例
        self._scheduler = AsyncIOScheduler(
            jobstores=jobstores,
            executors=executors,
            job_defaults=job_defaults,
            timezone=settings.timezone,
        )

    def startup(self):
        """启动调度器

        Note:
            - 启动后调度器会立即开始工作，但由于 JobStore 是空的（MemoryJobStore），
              不会触发任何任务，直到 ConfigWatcher 从数据库同步任务进来
            - 应在应用启动时调用（如 FastAPI 的 `@app.on_event("startup")`）
        """
        # 幂等性检查：如果已启动，直接返回
        if not self._scheduler.running:
            self._scheduler.start()
            logger.info("scheduler_started")

    def shutdown(self):
        """优雅关闭调度器

        停止 APScheduler 的事件循环，等待所有在途任务执行完毕

        Note:
            - `wait=True` 确保在途任务有机会执行完（如正在写入数据库的任务）
            - 应在应用关闭时调用（如 FastAPI 的 `@app.on_event("shutdown")`）
            - 如果在途任务长时间不结束，可能导致关闭阻塞（应设置超时保护）
        """
        # 检查调度器是否正在运行
        if self._scheduler.running:
            self._scheduler.shutdown(wait=True)
            logger.info("scheduler_shutdown")

    @property
    def raw_scheduler(self) -> AsyncIOScheduler:
        """获取 APScheduler 的原始调度器实例

        供 ConfigWatcher 使用，直接操作调度器（增删改任务）

        Returns:
            AsyncIOScheduler: APScheduler 的调度器实例

        Note:
            - 不建议直接使用此属性，应通过 SchedulerManager 的封装方法操作调度器
            - ConfigWatcher 需要直接访问调度器来同步任务，因此暴露此属性
        """
        return self._scheduler


# 创建全局的调度器管理器实例
scheduler_manager = SchedulerManager()
