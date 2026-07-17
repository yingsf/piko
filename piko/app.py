import asyncio
import importlib
import pkgutil
import signal
from collections.abc import Callable
from contextlib import asynccontextmanager
from types import ModuleType
from typing import Type, Dict, List, Set

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from sqlalchemy import select

from piko.compute.manager import CpuManager
from piko.config import settings
from piko.core.cache import ConfigCache
from piko.core.registry import JobHandler, JobRegistry
from piko.core.resource import Resource
from piko.core.runner import JobRunner
from piko.core.scheduler import SchedulerManager
from piko.core.types import BackfillPolicy
from piko.core.watcher import ConfigWatcher
from piko.infra.db import (
    check_database_connection,
    get_session_context,
    init_db,
    reset_db,
    ScheduledJob,
    verify_schema,
)
from piko.infra.leader import get_leader_mutex, get_leader_watchdog
from piko.infra.logging import get_logger, setup_logging
from piko.infra.observability import metrics_endpoint
from piko.persistence.writer import PersistenceWriter

logger = get_logger(__name__)


class PikoApp:
    """Piko 应用程序生命周期管理器（App 实例模式）

    负责组装所有子系统（Registry, Runner, Scheduler等），协调启动、运行和关闭流程。采用依赖注入模式管理组件

    Attributes:
        name (str): 应用名称，用于日志标识。
        registry (JobRegistry): 任务注册中心，存储代码中定义的 Job。
        config_cache (ConfigCache): 配置缓存，同步 DB 中的任务配置。
        writer (PersistenceWriter): 持久化写入器，负责 JobRun 等数据的落库。
        cpu_manager (CpuManager): CPU 密集型任务计算池。
        runner (JobRunner): 任务执行引擎。
        scheduler (SchedulerManager): 调度管理器。
        watcher (ConfigWatcher): 配置监听器，负责感知 DB 变更。
        api_app (FastAPI): 内置的运维 API 实例。
    """

    def __init__(self, name: str = "piko", modules: List[str] | None = None):
        """初始化 Piko 应用程序

        Args:
            name (str): 应用程序名称，将用于 API 文档标题和日志。
            modules (List[str] | None): 需要自动加载的模块路径列表（可选）。建议使用 auto_discover_jobs 替代此参数
        """
        self.name = name
        self._shutdown_event = asyncio.Event()
        self._started = False

        # ==========================================================
        # 1. 实例化核心组件
        # ==========================================================
        self.registry = JobRegistry()
        self.config_cache = ConfigCache()
        self.writer = PersistenceWriter()

        # CPU 计算池由应用实例持有
        self.cpu_manager = CpuManager()

        # ==========================================================
        # 2. 组装组件 (依赖注入)
        # ==========================================================
        self.runner = JobRunner(
            registry=self.registry, config_cache=self.config_cache, writer=self.writer
        )

        self.scheduler = SchedulerManager()

        self.watcher = ConfigWatcher(
            scheduler_manager=self.scheduler,
            config_cache=self.config_cache,
            registry=self.registry,
            runner=self.runner,
        )

        # ==========================================================
        # 3. 初始化运维 API
        # ==========================================================
        docs_url = "/docs" if settings.api_docs_enabled else None
        redoc_url = "/redoc" if settings.api_docs_enabled else None
        openapi_url = "/openapi.json" if settings.api_docs_enabled else None
        self.api_app = FastAPI(
            lifespan=self._lifespan_context,
            title=f"{name} Worker",
            docs_url=docs_url,
            redoc_url=redoc_url,
            openapi_url=openapi_url,
        )
        self._register_api_routes()

        # ==========================================================
        # 4. 加载模块 (如果有)
        # ==========================================================
        if modules:
            self.load_modules(modules)

    def load_modules(self, modules: List[str]):
        """动态加载模块以触发任务注册

        Args:
            modules (List[str]): 模块路径列表，例如 ["my_project.jobs.etl"]

        Raises:
            ImportError: 当模块路径不存在或导入失败时抛出。
        """
        for module_path in modules:
            try:
                importlib.import_module(module_path)
                logger.info("module_loaded", module=module_path)
            except ImportError as e:
                logger.error("module_load_failed", module=module_path, error=str(e))
                raise

    def auto_discover_jobs(self, base_package: str | ModuleType, pattern: str = "jobs"):
        """自动发现并加载任务模块

        递归扫描指定包下的所有子模块，如果模块名匹配 pattern (默认 'jobs')，则自动导入它，从而触发 @app.job 装饰器注册

        Args:
            base_package (str | ModuleType): 根包名 (e.g. 'iop_session_archiver')
            pattern (str): 模块匹配后缀 (默认 'jobs'，即匹配 xxxx.jobs.py)
        """
        if isinstance(base_package, str):
            try:
                package = importlib.import_module(base_package)
            except ImportError as e:
                logger.error("auto_discover_failed", package=base_package, error=str(e))
                raise e
        else:
            package = base_package

        if not hasattr(package, "__path__"):
            logger.warning(f"Skipping auto-discover: '{package.__name__}' is not a package.")
            return

        logger.info(f"Auto-discovering jobs in '{package.__name__}' (pattern='*{pattern}')...")

        count = 0
        prefix = package.__name__ + "."

        for _, name, is_pkg in pkgutil.walk_packages(package.__path__, prefix):
            if is_pkg:
                continue

            if name.endswith("." + pattern) or name == pattern:
                try:
                    importlib.import_module(name)
                    logger.debug(f"   -> Loaded: {name}")
                    count += 1
                except Exception as e:
                    logger.error(f"❌ Failed to load module '{name}': {e}")

        logger.info(f"Auto-discovered {count} job modules.")

    def _register_api_routes(self):
        """注册内置的运维 API 路由"""

        async def healthz():
            """健康检查端点 (Liveness Probe)"""
            status = "shutting_down" if self.is_shutdown_initiated else "ok"
            return JSONResponse(content={"status": status, "shutdown": self.is_shutdown_initiated})

        self.api_app.add_api_route("/healthz", healthz, methods=["GET"])

        async def readyz():
            """就绪检查端点 (Readiness Probe)

            检查启动完成、数据库、Writer、Watcher、Scheduler 和 Leader 状态。
            """
            leader = get_leader_mutex()
            checks = {
                "started": self._started,
                "database": await check_database_connection(),
                "writer": self.writer.is_running,
                "watcher": self.watcher.is_running,
                "scheduler": self.scheduler.is_running,
                "leader": not settings.leader_enabled or leader.is_leader,
            }
            ready = all(checks.values()) and not self.is_shutdown_initiated
            status = "ready" if ready else "not_ready"
            if settings.leader_enabled and not leader.is_leader:
                status = "standby"
            return JSONResponse(
                status_code=200 if ready else 503,
                content={"status": status, "ready": ready, "checks": checks},
            )

        self.api_app.add_api_route("/readyz", readyz, methods=["GET"])

        async def metrics():
            """Prometheus 指标端点"""
            if not settings.metrics_enabled:
                return JSONResponse(status_code=404, content={"detail": "metrics disabled"})
            return metrics_endpoint()

        self.api_app.add_api_route("/metrics", metrics, methods=["GET"])

    @property
    def is_shutdown_initiated(self) -> bool:
        """检查是否已触发关闭流程"""
        return self._shutdown_event.is_set()

    def job(
        self,
        job_id: str,
        schema: Type[BaseModel] | None = None,
        stateful: bool = False,
        backfill_policy: BackfillPolicy = BackfillPolicy.SKIP,
        resources: Dict[str, Type[Resource]] | None = None,
    ) -> Callable[[JobHandler], JobHandler]:
        """装饰器：注册任务到当前 App 实例

        Args:
            job_id (str): 任务唯一标识，必须与 scheduled_job 表中的 job_id 一致
            schema (Type[BaseModel] | None): 任务配置的 Pydantic Schema，用于验证 config json
            stateful (bool): 是否为有状态任务（需要维护 last_data_time）
            backfill_policy (BackfillPolicy): 补跑策略 (SKIP 或 RUN)
            resources (Dict[str, Type[Resource]] | None): 资源依赖注入声明

        Returns:
            Callable: 装饰器函数
        """
        return self.registry.register(
            job_id=job_id,
            schema=schema,
            stateful=stateful,
            backfill_policy=backfill_policy,
            resources=resources,
        )

    async def startup(self):
        """执行应用启动流程（六阶段）

        1. 初始化 DB 和 Table
        2. 启动 CPU 计算池
        3. 启动持久化写入器
        4. (可选) 选举 Leader
        5. 启动 ConfigWatcher 和 Scheduler
        6. 检查配置完整性 (Integrity Check)
        """
        setup_logging()
        logger.info("piko_app_startup", app=self.name, version=settings.version)

        try:
            init_db()
            await verify_schema()
            recovered_locks = await self.runner.recover_expired_locks()
            if recovered_locks:
                logger.warning("expired_job_locks_recovered", count=recovered_locks)
            recovered_runs = await self.runner.recover_orphaned_runs()
            if recovered_runs:
                logger.warning("orphaned_job_runs_recovered", count=recovered_runs)

            if settings.leader_enabled:
                await get_leader_mutex().ensure_seed()

            self.cpu_manager.startup()
            await self.writer.start()

            if settings.leader_enabled:
                is_leader = await get_leader_mutex().try_acquire()
                logger.info("leader_election", is_leader=is_leader)
                await get_leader_watchdog().start()

            await self.watcher.start()
            self.scheduler.startup()

            # 启动时检查：代码里的 Job 是否在 DB 里配置了
            await self._check_scheduler_integrity()

            self._started = True
            logger.info("piko_app_started")
        except Exception as e:
            logger.critical("piko_startup_unexpected_error", error=str(e))
            raise e

    async def _check_scheduler_integrity(self):
        """检查任务配置完整性（防呆）

        对比代码中注册的任务 (Registry) 和数据库中调度的任务 (DB)。如果发现代码里写了 Job 但数据库里没配，输出醒目地警告日志
        """
        # 1. 获取代码中定义的所有 Job ID (使用 Public API)
        registered_jobs = set(self.registry.get_all_job_ids())

        if not registered_jobs:
            logger.warning(
                "⚠️ No jobs registered in code. Did you forget @app.job or auto_discover_jobs?"
            )
            return

        # 2. 获取数据库中配置的所有 Job ID
        db_jobs: Set[str] = set()
        try:
            async with get_session_context() as session:
                # 查询所有启用的任务
                stmt = select(ScheduledJob.job_id).where(ScheduledJob.enabled.is_(True))
                result = await session.execute(stmt)
                db_jobs = set(result.scalars().all())
        except Exception as e:
            logger.warning(f"⚠️ Failed to check DB integrity: {e}")
            return

        # 3. 对比分析
        # 场景 A: 代码有任务，但数据库完全没配置 (最常见的错误)
        if not db_jobs:
            logger.warning(
                "\n" + "=" * 60 + "\n"
                "🚨 严重警告：没有配置任何任务！ 🚨\n"
                f"   在代码中发现了 {len(registered_jobs)} 个任务 ({', '.join(list(registered_jobs)[:3])}...), \n"
                "   但是 'scheduled_job' 表为空或所有任务都被禁用。\n"
                "   👉 操作：您必须在 'scheduled_job' 和 'job_config' 表中插入记录。\n"
                "   (您的代码没有问题，但 Piko 是配置驱动的。没有数据库记录 = 不会执行)\n"
                + "="
                * 60
            )

            return

        # 场景 B: 某些任务代码里写了，但没配置数据库
        missing_in_db = registered_jobs - db_jobs
        if missing_in_db:
            logger.warning(
                f"⚠️ 配置缺失：任务 {missing_in_db} 在代码中已定义但在数据库中未配置调度。\n"
                "   在您在 'scheduled_job' 表中配置它们之前，它们将不会运行。"
            )
        # 场景 C: 数据库配了任务，但代码里没加载 (可能是僵尸任务，或者是别的 Worker 的任务)
        missing_in_code = db_jobs - registered_jobs
        if missing_in_code:
            logger.info(
                f"ℹ️ 孤儿配置：任务 {missing_in_code} 在数据库中存在但在当前工作进程代码中未找到。\n"
                "   如果它们属于其他工作进程服务，则没有问题。"
            )

    async def shutdown(self):
        """执行应用关闭流程（逆序关闭）

        1. 停止调度器 (不再触发新任务)
        2. 停止 ConfigWatcher
        3. 停止 Leader 选举
        4. 停止持久化写入 (确保缓冲数据落盘)
        5. 停止 CPU 计算池
        """
        self._shutdown_event.set()
        self._started = False
        timeout_s = float(getattr(settings, "shutdown_timeout_s", 30))
        logger.info("piko_app_shutdown_begin", timeout_s=timeout_s)
        try:
            await asyncio.wait_for(self._shutdown_components(), timeout=timeout_s)
        except asyncio.TimeoutError:
            logger.critical("piko_shutdown_timeout", timeout_s=timeout_s)
        else:
            logger.info("piko_app_shutdown_complete")

    async def _shutdown_components(self) -> None:
        """按逆序关闭组件，供总停机预算统一约束。"""
        await self.watcher.stop()
        await asyncio.to_thread(self.scheduler.shutdown)

        if settings.leader_enabled:
            await get_leader_watchdog().stop()
            await get_leader_mutex().release()

        await self.writer.stop()
        await asyncio.to_thread(self.cpu_manager.shutdown)
        await reset_db()

    @asynccontextmanager
    async def _lifespan_context(self, _app: FastAPI):
        """FastAPI Lifespan 上下文管理器"""
        await self.startup()
        try:
            yield
        finally:
            await self.shutdown()

    @property
    def lifespan(self):
        return self._lifespan_context

    async def run_forever(self):
        """CLI 运行主入口（阻塞直到收到信号）"""
        await self.startup()

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda: self._shutdown_event.set())

        logger.info("piko_running_wait_for_signal")
        await self._shutdown_event.wait()
        await self.shutdown()

    def run(self):
        """同步运行入口（开发调试便利方法）"""
        asyncio.run(self.run_forever())
