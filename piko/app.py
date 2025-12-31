import asyncio
import signal
from contextlib import asynccontextmanager
from typing import Type, Dict

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

    Methods:
        startup: 六阶段启动流程（日志 -> 数据库 -> Workers -> Leader -> Watcher -> 调度器）
        shutdown: 五阶段关闭流程（调度器 -> Watcher -> Leader -> 持久化 -> Workers）
        run_forever: CLI 入口点，启动后等待关闭信号
        job: 任务注册装饰器

    Note:
        核心设计原则（启动/关闭顺序）：
        1. 严格顺序：启动和关闭流程有严格的依赖顺序，错误的顺序会导致数据丢失或报错
           例如：必须先停止调度器再停止持久化，否则调度器产生的新数据无法写入
        2. 优雅关闭：收到信号后，会等待所有正在进行的任务完成后再退出
        3. 依赖注入：核心组件在 __init__ 中组装，避免全局变量污染
    """

    def __init__(self, name: str = "piko"):
        """初始化 Piko 应用程序

        Args:
            name (str): 应用程序名称，将用于日志和 API 文档标题
        """
        self.name = name
        self._shutdown_event = asyncio.Event()

        # ==========================================================
        # 1. 实例化核心组件 (无副作用)
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
        """执行应用启动流程（六阶段）

        启动顺序：
        1. 初始化日志系统（确保最早捕获日志）
        2. 初始化数据库和表结构（基础设施就绪）
        3. 启动工作组件（计算池、持久化写入器）
        4. 启动 Leader 选举（决定当前节点角色）
        5. 启动配置监视器（同步任务配置）
        6. 启动调度器（开始触发任务）

        Note:
            - 必须按此顺序执行，否则会导致依赖错误
            - 例如：ConfigWatcher 依赖数据库，Scheduler 依赖 ConfigWatcher 同步的任务
        """
        # 0. 初始化日志（最先执行）
        setup_logging()
        logger.info("piko_app_startup", app=self.name, version=settings.version)

        # 1. 基础设施层
        init_db()
        # 自动创建表（开发模式便利性，生产环境建议用 Alembic）
        await create_all_tables()

        # 初始化 Leader 种子数据
        if settings.leader_enabled:
            await get_leader_mutex().ensure_seed()

        # 2. Worker 层
        self.cpu_manager.startup()
        await self.writer.start()

        # 3. Leader 层
        if settings.leader_enabled:
            # 尝试抢占 Leader
            is_leader = await get_leader_mutex().try_acquire()
            logger.info("leader_election", is_leader=is_leader)
            # 启动看门dog（自动续约/抢占）
            await get_leader_watchdog().start()

        # 4. Watcher 层
        await self.watcher.start()

        # 5. Scheduler 层
        self.scheduler.startup()

        logger.info("piko_app_started")

    async def shutdown(self):
        """执行应用关闭流程（逆序关闭）

        关闭顺序（与启动相反）：
        1. 停止调度器（不再产生新任务）
        2. 停止配置监视器（不再同步变更）
        3. 停止 Leader 看门doc并释放锁（主动让位）
        4. 停止持久化写入器（刷盘残留数据）
        5. 关闭计算池（等待计算任务完成）

        Note:
            - 逆序关闭是为了防止数据丢失和报错
            - 重点关注 persistence_writer.stop()，它会等待队列清空
        """
        logger.info("piko_app_shutdown_begin")

        # 1. 停止调度器
        self.scheduler.shutdown()

        # 2. 停止 Watcher
        await self.watcher.stop()

        # 3. 停止 Leader 机制
        if settings.leader_enabled:
            await get_leader_watchdog().stop()
            # 主动释放锁，让其他节点立即接管
            await get_leader_mutex().release()

        # 4. 停止持久化（关键：刷盘）
        await self.writer.stop()

        # 5. 停止计算资源
        self.cpu_manager.shutdown()

        logger.info("piko_app_shutdown_complete")

    @asynccontextmanager
    async def _lifespan_context(self, _app: FastAPI):
        """FastAPI Lifespan 上下文管理器适配器

        将 Piko 的启动/关闭流程集成到 FastAPI 应用中，
        使得 PikoApp 可以被 uvicorn 等 ASGI 服务器托管。
        """
        await self.startup()
        try:
            yield
        finally:
            await self.shutdown()

    @property
    def lifespan(self):
        """公开的 lifespan 属性，供外部 FastAPI 使用"""
        return self._lifespan_context

    async def run_forever(self):
        """CLI 运行主入口（阻塞直到收到信号）

        注册信号处理器（SIGINT/SIGTERM），启动应用，然后挂起等待关闭信号

        Note:
            - 适用于 Docker 容器或 Systemd 服务
            - 收到信号后会触发优雅关闭流程
        """
        # 启动应用
        await self.startup()

        # 获取当前事件循环
        loop = asyncio.get_running_loop()

        # 注册信号处理器
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda: self._shutdown_event.set())

        logger.info("piko_running_wait_for_signal")

        # 挂起主协程，直到收到信号
        await self._shutdown_event.wait()

        # 收到信号后，执行关闭
        await self.shutdown()

    def run(self):
        """同步运行入口（开发调试便利方法）

        封装了 asyncio.run(run_forever())，方便在 main.py 中直接调用
        """
        asyncio.run(self.run_forever())
