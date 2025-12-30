import datetime
import sys
from typing import Any, AsyncGenerator

from sqlalchemy import (
    BIGINT,
    JSON,
    String,
    DateTime,
    Integer,
    Boolean,
    Index,
)
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    create_async_engine,
    async_sessionmaker,
    AsyncEngine,
    AsyncAttrs,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from piko.config import settings
from piko.infra.logging import get_logger

logger = get_logger(__name__)


class Base(AsyncAttrs, DeclarativeBase):
    """SQLAlchemy ORM 基类

    所有业务表模型均继承此基类，自动获得：
    - 异步属性加载能力（AsyncAttrs）
    - 声明式映射支持（DeclarativeBase）

    Note:
        该基类不包含任何业务字段，仅作为 ORM 框架的统一入口
    """
    pass


_engine: AsyncEngine | None = None
_session_maker: async_sessionmaker[AsyncSession] | None = None


def init_db() -> None:
    """初始化数据库引擎和会话工厂

    基于配置文件中的 mysql_dsn 创建异步数据库引擎，并设置连接池参数
    该函数应在应用启动时调用一次，后续通过 get_session() 获取会话

    Raises:
        SystemExit: 当 mysql_dsn 配置缺失或为空时，记录严重错误并退出进程

    Note:
        连接池配置考量：
        - pool_size: 常驻连接数，根据并发度调整
        - max_overflow: 突发流量时的额外连接数
        - pool_recycle: 定期回收连接，防止 MySQL 服务端超时断开
        - pool_pre_ping: 每次取出连接前先 ping，确保连接有效（容错性 vs 性能权衡）
    """
    global _engine, _session_maker
    if _engine:
        return

    # 严格校验配置：DSN 是数据库连接的唯一入口，缺失则无法工作
    dsn = settings.get("mysql_dsn", "")
    if not dsn or str(dsn).strip() == "":
        logger.critical("startup_config_missing", field="mysql_dsn")
        sys.exit(1)

    _engine = create_async_engine(
        settings.mysql_dsn,
        pool_size=settings.mysql_pool_size,
        max_overflow=settings.mysql_max_overflow,
        pool_recycle=settings.mysql_pool_recycle_s,
        echo=settings.debug,
        pool_pre_ping=True
    )
    _session_maker = async_sessionmaker(
        bind=_engine,
        expire_on_commit=False,
        autoflush=False
    )


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """获取数据库会话的异步生成器（依赖注入模式）

    典型用法：
        async for session in get_session():
            result = await session.execute(stmt)
            await session.commit()

    Yields:
        AsyncSession: 已绑定引擎的会话对象，由上下文管理器自动管理生命周期

    Raises:
        RuntimeError: 当数据库未初始化时（需先调用 init_db()）

    Note:
        使用 async with 上下文管理器确保：
        - 会话在使用后自动关闭
        - 异常发生时自动回滚
        - 连接正确归还连接池
    """
    if _session_maker is None:
        raise RuntimeError("Database not initialized. Call init_db() first.")

    async with _session_maker() as session:
        yield session


async def create_all_tables() -> None:
    """创建所有已定义的数据库表（基于 Base.metadata）

    该函数会读取所有继承自 Base 的模型类定义，并在数据库中创建对应的表结构

    Raises:
        RuntimeError: 当数据库引擎未初始化时

    Warning:
        该操作是幂等的（已存在的表不会被修改），但不会自动处理字段变更
    """
    # 前置条件检查：必须先初始化引擎才能操作 metadata
    if _engine is None:
        raise RuntimeError("Database not initialized.")

    async with _engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


def utcnow() -> datetime.datetime:
    """获取当前 UTC 时间（无时区标记的 naive datetime）

    Returns:
        datetime.datetime: UTC 时间，tzinfo 为 None

    Note:
        使用 naive datetime 而非 aware datetime，原因如下：
        - MySQL 的 DATETIME 类型不存储时区信息
        - 统一使用 UTC 存储，应用层负责时区转换
        - 避免 SQLAlchemy 在序列化时的时区混淆
    """
    return datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)


class ScheduledJob(Base):
    """计划任务调度配置表

    存储所有需要定期执行的任务的调度规则、执行策略和状态信息
    支持 Cron 表达式、固定间隔等多种调度类型，以及有状态任务的增量处理

    Attributes:
        job_id (str): 任务唯一标识，主键，最大 128 字符
        schedule_type (str): 调度类型，如 'cron'、'interval' 等
        schedule_expr (str): 调度表达式，具体格式由 schedule_type 决定
        timezone (str): 任务执行的时区，默认 'Asia/Shanghai'
        enabled (bool): 任务是否启用，禁用后不会触发新的执行
        misfire_grace_s (int): 容忍的延迟执行时间（秒），超过则视为过期
        coalesce (bool): 是否合并多次过期的执行为一次
        max_instances (int): 最大并发执行实例数，默认 1（串行执行）
        jitter_s (int): 随机抖动时间（秒），用于分散集群负载
        executor (str): 执行器类型，如 'cpu'、'io'、'gpu' 等
        concurrency_group (str): 并发控制分组，相同组内任务共享资源配额
        is_stateful (bool): 是否为有状态任务（支持增量处理）
        last_data_time (datetime | None): 上次处理的数据时间点，用于增量计算
        max_lookback_window (int): 最大回溯窗口（秒），用于补偿处理
        version (int): 版本号，用于乐观锁控制并发修改
        updated_at (datetime): 最后更新时间，用于变更检测

    Indexes:
        - idx_sjob_enabled: 快速过滤启用/禁用任务
        - idx_sjob_version: 支持基于版本号的增量同步
        - idx_sjob_updated: 支持按更新时间排序的变更订阅

    Note:
        有状态任务设计：
        - is_stateful=True 时，调度器会根据 last_data_time 计算下次处理的数据窗口
        - max_lookback_window 限制回溯范围，防止长时间停机后产生过大的补偿任务
    """
    __tablename__ = "scheduled_job"

    job_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    schedule_type: Mapped[str] = mapped_column(String(16), nullable=False)
    schedule_expr: Mapped[str] = mapped_column(String(512), nullable=False)
    timezone: Mapped[str] = mapped_column(String(64), default="Asia/Shanghai", nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    misfire_grace_s: Mapped[int] = mapped_column(Integer, default=300, nullable=False)
    coalesce: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    max_instances: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    jitter_s: Mapped[int] = mapped_column(Integer, default=0, nullable=False)

    executor: Mapped[str] = mapped_column(String(16), default="cpu", nullable=False)
    concurrency_group: Mapped[str] = mapped_column(String(64), default="default", nullable=False)

    # 有状态任务支持
    is_stateful: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    last_data_time: Mapped[datetime.datetime | None] = mapped_column(DateTime(6), nullable=True)
    max_lookback_window: Mapped[int] = mapped_column(Integer, default=0, nullable=False)

    version: Mapped[int] = mapped_column(BIGINT, default=1, nullable=False)

    updated_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(6),
        default=utcnow,
        onupdate=utcnow,
        nullable=False
    )

    __table_args__ = (
        Index("idx_sjob_enabled", "enabled"),
        Index("idx_sjob_version", "version"),
        Index("idx_sjob_updated", "updated_at"),
    )


class JobConfig(Base):
    """任务配置表（支持版本化和灰度发布）

    存储每个任务的详细配置参数（JSON 格式），支持配置的版本管理和生效时间控制，与 ScheduledJob 表分离，便于独立更新配置而不影响调度规则

    Attributes:
        job_id (str): 任务 ID，主键，关联 ScheduledJob.job_id
        schema_version (int): 配置 schema 版本号，用于配置格式演进
        config_json (dict): 任务配置的 JSON 内容，具体结构由 schema_version 定义
        effective_from (datetime | None): 配置生效时间，支持定时灰度发布
        version (int): 版本号，用于乐观锁控制并发修改
        updated_at (datetime): 最后更新时间

    Indexes:
        - idx_jcfg_version: 支持版本号过滤
        - idx_jcfg_updated: 支持按更新时间排序的变更检测

    Note:
        配置生效策略：
        - effective_from 为 None 时立即生效
        - 非 None 时，调度器在该时间点后才会使用新配置
    """
    __tablename__ = "job_config"
    job_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    schema_version: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    config_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    effective_from: Mapped[datetime.datetime | None] = mapped_column(DateTime(6), nullable=True)

    version: Mapped[int] = mapped_column(BIGINT, default=1, nullable=False)
    updated_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(6),
        default=utcnow,
        onupdate=utcnow,
        nullable=False
    )

    __table_args__ = (
        Index("idx_jcfg_version", "version"),
        Index("idx_jcfg_updated", "updated_at"),
    )


class JobRun(Base):
    """任务执行记录表

    记录每次任务执行的完整生命周期，包括调度时间、实际执行时间、状态、性能指标、错误信息等，用于监控、审计和故障排查

    Attributes:
        run_id (int): 执行记录 ID，主键，自增
        job_id (str): 关联的任务 ID
        scheduled_time (datetime): 计划执行时间（调度器触发时间）
        start_time (datetime): 实际开始执行时间
        end_time (datetime | None): 执行结束时间，运行中为 None
        status (str): 执行状态，如 'RUNNING'、'SUCCESS'、'FAILED' 等
        config_version (int | None): 使用的配置版本号，用于关联 JobConfig
        schedule_version (int | None): 使用的调度版本号，用于关联 ScheduledJob
        data_time_start (datetime | None): 本次处理的数据窗口起始时间（有状态任务）
        data_time_end (datetime | None): 本次处理的数据窗口结束时间（有状态任务）
        compute_ms (int | None): 计算阶段耗时（毫秒）
        persist_ms (int | None): 持久化阶段耗时（毫秒）
        duration_ms (int | None): 总执行时长（毫秒）
        attempt (int): 重试次数，首次执行为 1
        error_type (str | None): 错误类型（异常类名）
        error_hash (str | None): 错误堆栈的哈希值，用于聚合相似错误
        error_msg (str | None): 错误消息摘要（截断到 512 字符）
        host (str | None): 执行主机名
        pid (int | None): 执行进程 PID
        created_at (datetime): 记录创建时间

    Indexes:
        - idx_run_job_time: 组合索引，支持按任务和时间查询历史记录（最常见查询）
        - idx_run_status: 支持按状态过滤（如查询所有失败任务）
        - idx_run_created: 支持按创建时间排序的时序查询

    Note:
        性能指标拆分设计：
        - compute_ms: 纯计算逻辑耗时，用于算法性能分析
        - persist_ms: 数据持久化耗时，用于 I/O 瓶颈分析
        - duration_ms: 端到端总耗时，用于 SLA 监控
    """
    __tablename__ = "job_run"
    run_id: Mapped[int] = mapped_column(BIGINT, primary_key=True, autoincrement=True)
    job_id: Mapped[str] = mapped_column(String(128), nullable=False)

    scheduled_time: Mapped[datetime.datetime] = mapped_column(DateTime(6), nullable=False)
    start_time: Mapped[datetime.datetime] = mapped_column(DateTime(6), nullable=False)
    end_time: Mapped[datetime.datetime | None] = mapped_column(DateTime(6), nullable=True)

    status: Mapped[str] = mapped_column(String(32), nullable=False)
    config_version: Mapped[int | None] = mapped_column(BIGINT, nullable=True)
    schedule_version: Mapped[int | None] = mapped_column(BIGINT, nullable=True)

    # 记录本次 Run 处理的数据窗口
    data_time_start: Mapped[datetime.datetime | None] = mapped_column(DateTime(6), nullable=True)
    data_time_end: Mapped[datetime.datetime | None] = mapped_column(DateTime(6), nullable=True)

    compute_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    persist_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    duration_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    attempt: Mapped[int] = mapped_column(Integer, default=1, nullable=False)

    error_type: Mapped[str | None] = mapped_column(String(64), nullable=True)
    error_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    error_msg: Mapped[str | None] = mapped_column(String(512), nullable=True)

    host: Mapped[str | None] = mapped_column(String(128), nullable=True)
    pid: Mapped[int | None] = mapped_column(Integer, nullable=True)

    created_at: Mapped[datetime.datetime] = mapped_column(DateTime(6), default=utcnow, nullable=False)

    __table_args__ = (
        Index("idx_run_job_time", "job_id", "scheduled_time"),
        Index("idx_run_status", "status"),
        Index("idx_run_created", "created_at"),
    )


class JobLock(Base):
    """任务执行锁表（防止重复执行）

    用于保证同一任务在同一调度时间点只被执行一次，即使在多实例部署下也能避免重复
    采用数据库唯一约束实现分布式锁，无需额外的分布式锁服务（如 Redis）

    Attributes:
        job_id (str): 任务 ID，联合主键之一
        scheduled_time (datetime): 调度时间，联合主键之一
        owner (str): 锁持有者标识（通常为 hostname:pid）
        acquired_at (datetime): 锁获取时间

    Indexes:
        - idx_lock_owner: 支持按持有者查询所有锁（如节点下线时清理锁）

    Note:
        锁机制设计：
        - 联合主键 (job_id, scheduled_time) 天然保证唯一性
        - 通过 INSERT 操作抢占锁（唯一约束冲突表示抢占失败）
        - 执行完成后删除锁记录，释放资源
        - 异常情况下锁可能残留，需要定期清理过期锁（通过 acquired_at 判断）
    """
    __tablename__ = "job_lock"
    job_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    scheduled_time: Mapped[datetime.datetime] = mapped_column(DateTime(6), primary_key=True)

    owner: Mapped[str] = mapped_column(String(128), nullable=False)
    acquired_at: Mapped[datetime.datetime] = mapped_column(DateTime(6), default=utcnow, nullable=False)

    __table_args__ = (
        Index("idx_lock_owner", "owner"),
    )


class SchedulerLeader(Base):
    """调度器 Leader 选举表

    用于在多实例调度器集群中选举出唯一的 Leader 节点，Leader 负责触发任务调度
    采用基于数据库的租约（Lease）机制 + 版本号乐观锁实现分布式选举

    Attributes:
        name (str): 租约名称，主键，通常为固定值（如 'scheduler-leader'）
        owner (str): 当前 Leader 标识（hostname:pid）
        lease_until (datetime): 租约到期时间，Leader 需在到期前续约
        version (int): 版本号，用于 CAS（Compare-And-Swap）操作防止并发抢占
        updated_at (datetime): 最后更新时间

    Note:
        选举机制核心要点：
        1. 租约过期判断：lease_until < 当前时间，则锁可被抢占
        2. CAS 更新：通过 WHERE version = old_version 确保原子性
        3. 版本号递增：每次成功抢占或续约时 version += 1，防止 ABA 问题
        4. 心跳续约：Leader 定期更新 lease_until，失败则自动降级

        该表通常只有一行记录，不需要复杂索引
    """
    __tablename__ = "scheduler_leader"
    name: Mapped[str] = mapped_column(String(64), primary_key=True)
    owner: Mapped[str] = mapped_column(String(128), nullable=False)
    lease_until: Mapped[datetime.datetime] = mapped_column(DateTime(6), nullable=False)
    version: Mapped[int] = mapped_column(Integer, default=0, nullable=False)

    updated_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(6),
        default=utcnow,
        onupdate=utcnow,
        nullable=False
    )


class SinkDedupe(Base):
    """数据输出去重表（幂等性保证）

    记录已成功写入下游系统的数据标识，用于防止任务重试或故障恢复时重复写入
    支持多种下游系统（Sink），如数据库、消息队列、API 等

    Attributes:
        sink_name (str): 下游系统标识（如 'mysql_orders'、'kafka_events'），联合主键
        idempotency_key (str): 幂等键（通常为业务 ID 或数据指纹），联合主键
        run_id (int): 关联的执行记录 ID（JobRun.run_id）
        status (str): 写入状态，如 'SUCCESS'、'PENDING' 等
        updated_at (datetime): 最后更新时间
        created_at (datetime): 记录创建时间

    Indexes:
        - idx_dedupe_run: 支持按 run_id 反查该次执行写入了哪些数据
        - idx_dedupe_status_updated: 组合索引，支持查询特定状态的记录并按时间排序
        - idx_dedupe_created: 支持按创建时间的时序查询

    Note:
        幂等性设计：
        1. 写入前先查询 (sink_name, idempotency_key) 是否存在
        2. 已存在且 status='SUCCESS' 则跳过写入
        3. 不存在则执行写入 + 插入去重记录
        4. 异常情况可能导致 status='PENDING'，需要定期清理或重试

        清理策略：
        - 定期删除过期的去重记录（通过 created_at 判断）
        - 平衡存储成本和幂等性保证窗口
    """
    __tablename__ = "sink_dedupe"
    sink_name: Mapped[str] = mapped_column(String(64), primary_key=True)
    idempotency_key: Mapped[str] = mapped_column(String(128), primary_key=True)

    run_id: Mapped[int] = mapped_column(BIGINT, nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False)

    updated_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(6),
        default=utcnow,
        onupdate=utcnow,
        nullable=False
    )
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime(6), default=utcnow, nullable=False)

    __table_args__ = (
        Index("idx_dedupe_run", "run_id"),
        Index("idx_dedupe_status_updated", "status", "updated_at"),
        Index("idx_dedupe_created", "created_at"),
    )
    