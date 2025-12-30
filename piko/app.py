import asyncio
import signal
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import Response

from piko.compute.manager import cpu_manager
from piko.config import settings
from piko.core.scheduler import scheduler_manager
from piko.core.watcher import config_watcher
from piko.infra.db import init_db, create_all_tables
from piko.infra.leader import get_leader_mutex, get_leader_watchdog
from piko.infra.logging import get_logger, setup_logging
from piko.infra.observability import metrics_endpoint, CONTENT_TYPE_LATEST
from piko.persistence.writer import persistence_writer

logger = get_logger(__name__)


class PikoApp:
    """Piko 应用程序生命周期管理器

    协调所有子系统的启动、运行和关闭流程，确保严格的顺序依赖关系
    支持两种运行模式：
    1. CLI 模式：通过 run_forever() 运行，使用信号处理优雅关闭
    2. FastAPI 模式：通过 lifespan 上下文管理器集成到 FastAPI

    Attributes:
        self._shutdown_event (asyncio.Event): 关闭信号事件（信号处理器设置，主循环等待）

    Methods:
        startup: 六阶段启动流程（日志 -> 数据库 -> Workers -> Leader -> Watcher -> 调度器）
        shutdown: 五阶段关闭流程（调度器 -> Watcher -> Leader -> 持久化 -> Workers）
        run_forever: CLI 入口点，启动后等待关闭信号
        is_shutdown_initiated: 公共属性，检查是否已触发关闭

    Note:
        核心设计原则：
        1. 严格顺序：启动和关闭流程有明确的依赖关系，必须按顺序执行
        2. 容错兜底：每个阶段失败不会阻塞后续阶段（记录错误后继续）
        3. 优雅关闭：关闭时先停止新任务触发，再等待队列清空，最后释放资源
        4. 高可用：支持 Leader 选举，Standby 节点保持热备份状态

        启动顺序依赖分析：
        - 日志系统 -> 所有子系统（日志是基础设施）
        - 数据库 -> Leader/Watcher/Scheduler（所有业务逻辑依赖持久化）
        - Workers -> Scheduler（调度器需要 Worker 处理任务）
        - Leader 选举 -> Scheduler（只有 Leader 才执行调度）
        - Watcher -> Scheduler（调度器需要读取任务配置）

        关闭顺序依赖分析：
        - Scheduler -> Watcher（先停止触发新任务，再停止配置更新）
        - Watcher -> Leader（释放 Leader 前确保没有配置变更）
        - Leader -> Persistence（释放 Leader 后仍需保证数据不丢失）
        - Persistence -> Workers（持久化依赖 Worker 完成剩余任务）

    Warning:
        - 必须先调用 startup() 再调用 shutdown()
        - 不要直接访问 _shutdown_event，使用 is_shutdown_initiated 属性
        - Leader 选举失败时应用仍可启动（进入 Standby 模式）
    """

    def __init__(self):
        """初始化应用程序生命周期管理器"""
        # 关闭信号事件：信号处理器设置，主循环等待
        self._shutdown_event = asyncio.Event()

    @property
    def is_shutdown_initiated(self) -> bool:
        """检查是否已触发关闭流程

        Returns:
            bool: True 表示已触发关闭（收到 SIGINT/SIGTERM），False 表示正常运行
        """
        return self._shutdown_event.is_set()

    async def startup(self):
        """六阶段启动流程（严格顺序执行）

        执行步骤：
        0. 日志系统初始化：最先启动，所有后续日志依赖此步骤
        1. 数据库初始化：连接池创建、建表（开发环境自动建表）
        2. Workers 启动：计算引擎（CPU Manager）和持久化引擎（Persistence Writer）
        3. Leader 选举：若启用，尝试抢占 Leader 并启动看门狗
        4. 配置监听器启动：加载任务配置并监听变更（Standby 节点也启动，为切换做准备）
        5. 调度器启动：开始触发任务（仅 Leader 节点实际执行）

        Raises:
            Exception: 启动阶段失败会抛出异常，应用需要重启

        Note:
            阶段 0: 日志系统
            - setup_logging() 配置日志格式、级别、输出目标
            - 最先执行，确保后续所有步骤都能正常记录日志

            阶段 1: 数据库
            - init_db() 初始化连接池（同步或异步取决于配置）
            - create_all_tables() 在开发环境自动建表（生产环境由 migration 工具管理）
            - ensure_seed() 确保 Leader 表有初始记录（防止首次启动竞争失败）

            阶段 2: Workers
            - cpu_manager.startup() 启动计算线程池（执行 Python 函数任务）
            - persistence_writer.start() 启动持久化消费者（批量写入结果）
            - 这两个组件独立运行，调度器通过队列与它们通信

            阶段 3: Leader 选举
            - ensure_seed() 双重检查（虽冗余但安全，防止并发建表导致种子记录丢失）
            - try_acquire() 快速路径（Fast Path）：启动时立即尝试抢占 Leader
            - 若抢占失败，节点进入 Standby 模式（仍可切换为 Leader）
            - get_leader_watchdog().start() 启动后台看门狗：
              * Leader 节点：定期续约（防止被其他节点抢占）
              * Standby 节点：定期尝试抢占（Leader 故障时自动切换）

            阶段 4: 配置监听器
            - config_watcher.start() 加载任务配置到内存缓存
            - Design 5.1 要求：Standby 节点也启动 Watcher（原因如下）
              * 缓存预热：切换为 Leader 时无需重新加载配置，减少切换延迟
              * 一致性保证：Standby 节点与 Leader 节点保持相同的配置视图
              * 监控需求：Standby 节点可以暴露配置状态（用于调试和监控）

            阶段 5: 调度器
            - scheduler_manager.startup() 启动调度循环
            - 调度器内部检查 Leader 状态，只有 Leader 节点才触发任务
            - Standby 节点的调度器空转（等待 Leader 切换）

            启动顺序依赖关系：
            - 日志 -> 所有子系统（基础设施）
            - 数据库 -> Leader/Watcher/Scheduler（持久化依赖）
            - Workers -> Scheduler（任务执行依赖）
            - Leader -> Scheduler（调度授权依赖）
            - Watcher -> Scheduler（配置依赖）
        """
        # 阶段 0: 日志系统初始化（最先执行）
        setup_logging()

        logger.info("piko_startup_begin", version=settings.version)

        # 阶段 1: 数据库初始化
        init_db()

        # create_all_tables: 开发环境自动建表（生产环境由 Alembic 等工具管理）
        # 这里调用 SQLAlchemy 的 metadata.create_all()
        await create_all_tables()

        # ensure_seed: 确保 Leader 表有种子记录（防止首次启动时 try_acquire 失败）
        # 多个节点并发启动时，第一个节点需要创建种子记录
        if settings.leader_enabled:
            await get_leader_mutex().ensure_seed()

        # 阶段 2: Workers 启动
        # cpu_manager: 管理计算线程池（执行 Python 函数任务）
        # startup() 创建线程池并启动后台监控任务
        cpu_manager.startup()

        # persistence_writer: 管理持久化队列和消费者
        # start() 创建后台消费任务并恢复磁盘兜底数据
        await persistence_writer.start()

        # 阶段 3: Leader 选举（若启用）
        if settings.leader_enabled:
            # 3.1 双重检查种子数据（虽冗余但安全）
            # 防止并发建表导致种子记录丢失（极端情况）
            await get_leader_mutex().ensure_seed()

            # 3.2 快速路径（Fast Path）：启动时立即尝试抢占 Leader
            # try_acquire() 执行 UPDATE ... WHERE owner_id IS NULL 的原子操作
            leader = get_leader_mutex()
            is_leader = await leader.try_acquire()

            # 记录选举结果（owner_id 是当前节点标识，用于调试）
            logger.info("leader_election_result", is_leader=is_leader, owner_id=leader.owner_id)

            # 3.3 启动后台看门狗（Leader 续约 & Standby 抢占）
            # Leader 节点：定期执行 UPDATE ... WHERE owner_id = self 续约
            # Standby 节点：定期执行 try_acquire() 尝试抢占
            await get_leader_watchdog().start()

        # 阶段 4: 配置监听器启动
        await config_watcher.start()

        # 阶段 5: 调度器启动
        # scheduler_manager.startup() 启动调度循环
        # 调度器内部检查 get_leader_mutex().is_leader，只有 Leader 才触发任务
        scheduler_manager.startup()

        logger.info("piko_startup_complete")

    async def shutdown(self):
        """五阶段优雅关闭流程

        执行步骤：
        1. 停止调度器：不再触发新任务（但已触发的任务继续执行）
        2. 停止配置监听器：不再处理配置变更（减少干扰）
        3. 释放 Leader 锁：停止watchdog并释放数据库锁（允许其他节点接管）
        4. 刷新持久化队列（关键）：等待队列清空并 dump 残留数据
        5. 停止计算引擎：等待运行中的任务完成并关闭线程池

        Note:
            阶段 1: 停止调度器
            - scheduler_manager.shutdown() 设置停止标志
            - 调度循环立即退出（不再触发新任务）
            - 已触发的任务继续执行（由 Worker 处理）

            阶段 2: 停止配置监听器
            - config_watcher.stop() 停止后台监听任务
            - 防止停机期间配置变更导致状态不一致

            阶段 3: 释放 Leader 锁
            - get_leader_watchdog().stop() 停止续约/抢占任务
            - get_leader_mutex().release() 执行 UPDATE ... SET owner_id = NULL
            - 释放后其他 Standby 节点可以立即接管 Leader

            阶段 4: 刷新持久化队列（最关键）
            - persistence_writer.stop() 执行三阶段关闭：
              1. 等待队列清空（带超时）
              2. 取消消费任务
              3. dump 残留数据到磁盘（兜底）
            - 这确保数据不丢失（即使停机超时）

            阶段 5: 停止计算引擎
            - cpu_manager.shutdown() 等待运行中的任务完成
            - 关闭线程池（释放资源）

            顺序依赖关系分析：
            - Scheduler -> Watcher：先停止触发，再停止配置更新
            - Watcher -> Leader：配置稳定后再释放 Leader（避免切换时配置不一致）
            - Leader -> Persistence：释放 Leader 不影响持久化（数据安全优先）
            - Persistence -> Workers：持久化依赖 Worker 完成剩余任务
        """
        logger.info("piko_shutdown_begin")

        # 阶段 1: 停止调度器（不再触发新任务）
        # shutdown() 设置 _running = False，调度循环立即退出
        scheduler_manager.shutdown()

        # 阶段 2: 停止配置监听器（不再处理配置变更）
        # stop() 停止后台监听任务和缓存更新
        await config_watcher.stop()

        # 阶段 3: 释放 Leader 锁（允许其他节点接管）
        if settings.leader_enabled:
            # 停止看门狗（Leader 停止续约，Standby 停止抢占）
            await get_leader_watchdog().stop()

            # 释放数据库锁（UPDATE ... SET owner_id = NULL）
            # 其他 Standby 节点的看门狗会立即检测到并尝试抢占
            await get_leader_mutex().release()

        # 阶段 4: 刷新持久化队列（最关键，确保数据不丢失）
        # stop() 执行：等待队列 -> 取消消费者 -> dump 残留数据
        await persistence_writer.stop()

        # 阶段 5: 停止计算引擎（等待任务完成并释放资源）
        # shutdown() 等待运行中的任务完成并关闭线程池
        cpu_manager.shutdown()

        logger.info("piko_shutdown_complete")

    async def run_forever(self):
        """CLI 模式入口点（启动 -> 等待信号 -> 关闭）

        执行流程：
        1. 调用 startup() 启动所有子系统
        2. 注册信号处理器（SIGINT/SIGTERM -> 设置关闭事件）
        3. 等待关闭事件（阻塞在此）
        4. 收到信号后调用 shutdown() 优雅关闭
        """
        # 阶段 1: 启动所有子系统
        await self.startup()

        # 阶段 2: 注册信号处理器
        # 获取当前事件循环（必须在 async 上下文中调用）
        loop = asyncio.get_running_loop()

        # 为 SIGINT 和 SIGTERM 注册处理器
        # lambda: self._shutdown_event.set() 设置关闭事件（非阻塞）
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda: self._shutdown_event.set())

        # 阶段 3: 等待关闭信号（阻塞在此）
        logger.info("piko_running_wait_for_signal")
        await self._shutdown_event.wait()  # 阻塞直到 set() 被调用

        # 阶段 4: 优雅关闭
        await self.shutdown()


# 全局应用实例
app_instance = PikoApp()


# FastAPI 集成（运维 API）

@asynccontextmanager
async def lifespan(_app: FastAPI):
    """FastAPI 生命周期管理器（通过 uvicorn 启动时使用）

    Args:
        _app (FastAPI): FastAPI 应用实例（必须存在但不被使用，故命名为 _app）

    Yields:
        None: 上下文管理器，启动阶段在 yield 前执行，关闭阶段在 yield 后执行

    Note:
        使用场景：
        - 通过 uvicorn 启动：uvicorn piko.app:api_app
        - FastAPI 会在启动时调用 lifespan 上下文管理器
        - 这确保 PikoApp 的生命周期与 FastAPI 同步

        设计细节：
        - 参数命名为 _app 表示"必须存在但不使用"（遵循 PEP 8 约定）
        - FastAPI 要求 lifespan 函数接收 app 参数（即使不使用）
        - yield 前执行 startup，yield 后执行 shutdown（标准上下文管理器模式）
    """
    # 启动阶段：初始化所有子系统
    await app_instance.startup()

    # yield: FastAPI 应用正常运行（处理请求）
    yield

    # 关闭阶段：优雅关闭所有子系统
    await app_instance.shutdown()


# 创建 FastAPI 应用实例
api_app = FastAPI(lifespan=lifespan, title="Piko Worker")


@api_app.get("/healthz")
async def healthz():
    """存活探针

    Returns:
        dict: 健康状态对象，包含状态和关闭标志

    Note:
        设计要点：
        - 始终返回 200 OK（除非应用完全崩溃）
        - K8s 存活探针失败会重启 Pod（不应轻易失败）
        - is_shutdown_initiated 表示是否正在关闭（用于调试）

        与 readyz 的区别：
        - healthz：检查应用是否存活（进程是否崩溃）
        - readyz：检查应用是否就绪（是否可以处理流量）
    """
    # 使用公共属性访问关闭状态（封装性）
    return {"status": "ok", "host": app_instance.is_shutdown_initiated}


@api_app.get("/readyz")
async def readyz():
    """就绪探针

    Returns:
        dict: 就绪状态对象，包含角色和就绪标志

    Note:
        - Leader 节点：返回 {"status": "leader", "ready": True}
        - Standby 节点：返回 {"status": "standby", "ready": False}

        为什么 Standby 返回 ready=False：
        - K8s Service 只会将流量转发到 ready=true 的 Pod
        - Standby 节点不执行任务调度（Design 5.1 要求）
        - 避免误将流量打到 Standby 节点（虽然它没有对外服务）

        替代设计：
        - 若 Standby 节点也需要对外提供 API（如监控查询），可以返回 ready=true
        - 但需要在 API 层面区分 Leader 和 Standby 的权限

        监控建议：
        - 监控 readyz 端点，当集群中没有 ready=true 的节点时触发告警
        - 结合 Leader 选举日志，判断是否发生了脑裂（多个节点都认为自己是 Leader）
    """
    # 获取 Leader 互斥锁实例（检查当前节点是否为 Leader）
    leader = get_leader_mutex()

    if settings.leader_enabled and not leader.is_leader:
        # 当前节点是 Standby：返回 ready=false
        # K8s Service 不会将流量转发到此节点
        return {"status": "standby", "ready": False}

    # 当前节点是 Leader 或未启用 Leader 选举：返回 ready=true
    return {"status": "leader", "ready": True}


@api_app.get("/metrics")
async def metrics():
    """Prometheus 指标端点

    Returns:
        Response: Prometheus 格式的指标数据（text/plain; version=0.0.4）

    Note:
        - 使用 Prometheus Client Library 生成标准格式指标
        - 返回 CONTENT_TYPE_LATEST（Prometheus 官方推荐的 MIME 类型）

        暴露的指标（示例）：
        - piko_persistence_queue_size: 持久化队列当前大小
        - piko_scheduler_jobs_total: 调度的任务总数
        - piko_cpu_tasks_running: 正在运行的任务数

        监控告警建议：
        - 队列堆积：piko_persistence_queue_size > 阈值
        - 任务失败率：rate(piko_task_failures_total[5m]) > 阈值
        - Leader 切换频率：changes(piko_leader_is_leader[1h]) > 阈值
    """
    # metrics_endpoint() 调用 prometheus_client.generate_latest()，返回 Prometheus 格式的指标数据（bytes）
    data = metrics_endpoint()

    # 返回 Response 对象，设置正确的 MIME 类型，CONTENT_TYPE_LATEST = "text/plain; version=0.0.4; charset=utf-8"
    return Response(content=data, media_type=CONTENT_TYPE_LATEST)
