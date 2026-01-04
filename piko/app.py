import asyncio
import importlib
import signal
from contextlib import asynccontextmanager
from typing import Type, Dict, List

from fastapi import FastAPI
from fastapi.responses import Response
from pydantic import BaseModel

from piko.compute.manager import CpuManager
from piko.config import settings
from piko.core.cache import ConfigCache
from piko.core.registry import JobRegistry
from piko.core.resource import Resource
from piko.core.runner import JobRunner
from piko.core.scheduler import SchedulerManager
from piko.core.types import BackfillPolicy
from piko.core.watcher import ConfigWatcher
from piko.infra.db import init_db, create_all_tables
from piko.infra.leader import get_leader_mutex, get_leader_watchdog
from piko.infra.logging import get_logger, setup_logging
from piko.infra.observability import metrics_endpoint, CONTENT_TYPE_LATEST
from piko.persistence.writer import PersistenceWriter

logger = get_logger(__name__)


class PikoApp:
    """Piko 应用程序生命周期管理器（App 实例模式）

    负责组装所有子系统（Registry, Runner, Scheduler等），协调启动、运行和关闭流程，并提供统一的编程入口

    不再依赖全局隐式状态，所有核心组件由 App 实例持有并进行依赖注入（Dependency Injection）

    支持两种运行模式：
    1. CLI 模式：通过 run_forever() 运行，使用信号处理优雅关闭
    2. FastAPI 模式：通过 lifespan 上下文管理器集成到 FastAPI

    Attributes:
        name (str): 应用名称，用于日志标识
        registry (JobRegistry): 任务注册中心
        config_cache (ConfigCache): 配置缓存
        writer (PersistenceWriter): 持久化写入器
        runner (JobRunner): 任务执行引擎
        scheduler (SchedulerManager): 调度器管理器
        watcher (ConfigWatcher): 配置监听器
        cpu_manager (CpuManager): CPU 计算池管理器
        api_app (FastAPI): 内置的 FastAPI 实例（用于监控和运维）
        _shutdown_event (asyncio.Event): 关闭信号事件
    """

    def __init__(self, name: str = "piko", modules: List[str] | None = None):
        """初始化 Piko 应用程序

        Args:
            name (str): 应用程序名称，将用于日志和 API 文档标题
            modules (List[str] | None): 需要自动加载的模块路径列表（用于触发任务注册）
        """
        self.name = name
        self._shutdown_event = asyncio.Event()

        # ==========================================================
        # 1. 实例化核心组件
        # ==========================================================
        self.registry = JobRegistry()
        self.config_cache = ConfigCache()

        # 持久化写入器（负责写库）
        self.writer = PersistenceWriter()

        # CPU 计算池（负责密集型计算）
        self.cpu_manager = CpuManager()

        # ==========================================================
        # 2. 组装组件 (依赖注入)
        # ==========================================================
        # JobRunner 需要访问 Registry(查任务), Cache(查配置), Writer(写结果)
        self.runner = JobRunner(
            registry=self.registry,
            config_cache=self.config_cache,
            writer=self.writer
        )

        # 调度器管理器（负责时间触发），SchedulerManager 现在是纯实例，无全局状态
        self.scheduler = SchedulerManager()

        # 配置监视器（负责同步 DB -> Cache/Scheduler），ConfigWatcher 接收所有依赖，彻底消除全局 import
        self.watcher = ConfigWatcher(
            scheduler_manager=self.scheduler,
            config_cache=self.config_cache,
            registry=self.registry,
            runner=self.runner
        )

        # ==========================================================
        # 3. 初始化运维 API
        # ==========================================================
        self.api_app = FastAPI(lifespan=self._lifespan_context, title=f"{name} Worker")
        self._register_api_routes()

        # ==========================================================
        # 4. 加载模块 (如果有)
        # ==========================================================
        if modules:
            self.load_modules(modules)

    def load_modules(self, modules: List[str]):
        """动态加载模块以触发任务注册

        Args:
            modules (List[str]): 模块路径列表，例如 ["my_project.jobs.etl", "my_project.jobs.crawler"]

        Raises:
            ImportError: 当模块路径不存在或导入失败时抛出
        """
        for module_path in modules:
            try:
                importlib.import_module(module_path)
                logger.info("module_loaded", module=module_path)
            except ImportError as e:
                logger.error("module_load_failed", module=module_path, error=str(e))
                # 在启动阶段，任何配置错误都应该立即报错退出
                raise

    def _register_api_routes(self):
        """注册内置的运维 API 路由"""

        @self.api_app.get("/healthz")
        async def healthz():
            """健康检查端点（Liveness Probe）"""
            return {"status": "ok", "shutdown": self.is_shutdown_initiated}

        @self.api_app.get("/readyz")
        async def readyz():
            """就绪检查端点（Readiness Probe）

            用于 K8s 判断流量是否可以转发到此 Pod
            如果当前节点是 Standby，返回非 Ready 状态（视业务需求而定）
            """
            leader = get_leader_mutex()
            if settings.leader_enabled and not leader.is_leader:
                return {"status": "standby", "ready": False}
            return {"status": "leader", "ready": True}

        @self.api_app.get("/metrics")
        async def metrics():
            """Prometheus 指标端点"""
            data = metrics_endpoint()
            return Response(content=data, media_type=CONTENT_TYPE_LATEST)

    @property
    def is_shutdown_initiated(self) -> bool:
        """检查是否已触发关闭流程

        Returns:
            bool: True 表示已收到关闭信号
        """
        return self._shutdown_event.is_set()

    def job(
            self,
            job_id: str,
            schema: Type[BaseModel] | None = None,
            stateful: bool = False,
            backfill_policy: BackfillPolicy = BackfillPolicy.SKIP,
            resources: Dict[str, Type[Resource]] | None = None
    ):
        """装饰器：注册任务到当前 App 实例

        Args:
            job_id (str): 任务唯一标识
            schema (Type[BaseModel] | None): 配置验证 Schema
            stateful (bool): 是否为有状态任务
            backfill_policy (BackfillPolicy): 补跑策略
            resources (Dict[str, Type[Resource]] | None): 资源依赖声明

        Returns:
            Callable: 装饰器函数
        """
        return self.registry.register(
            job_id=job_id,
            schema=schema,
            stateful=stateful,
            backfill_policy=backfill_policy,
            resources=resources
        )

    async def startup(self):
        """执行应用启动流程（六阶段）"""
        # 0. 初始化日志（最先执行）
        setup_logging()
        logger.info("piko_app_startup", app=self.name, version=settings.version)

        try:
            # 1. 基础设施层
            init_db()
            await create_all_tables()

            # 初始化 Leader 种子数据
            if settings.leader_enabled:
                await get_leader_mutex().ensure_seed()

            # 2. Worker 层
            self.cpu_manager.startup()
            await self.writer.start()

            # 3. Leader 层
            if settings.leader_enabled:
                is_leader = await get_leader_mutex().try_acquire()
                logger.info("leader_election", is_leader=is_leader)
                await get_leader_watchdog().start()

            # 4. Watcher 层
            await self.watcher.start()

            # 5. Scheduler 层
            self.scheduler.startup()

            logger.info("piko_app_started")
        except Exception as e:
            logger.critical("piko_startup_unexpected_error", error=str(e))
            raise e

    async def shutdown(self):
        """执行应用关闭流程（逆序关闭）"""
        logger.info("piko_app_shutdown_begin")

        # 1. 停止调度器
        self.scheduler.shutdown()

        # 2. 停止 Watcher
        await self.watcher.stop()

        # 3. 停止 Leader 机制
        if settings.leader_enabled:
            await get_leader_watchdog().stop()
            await get_leader_mutex().release()

        # 4. 停止持久化（关键：刷盘）
        await self.writer.stop()

        # 5. 停止计算资源
        self.cpu_manager.shutdown()

        logger.info("piko_app_shutdown_complete")

    @asynccontextmanager
    async def _lifespan_context(self, _app: FastAPI):
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
