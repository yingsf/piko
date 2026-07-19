import datetime
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator

from sqlalchemy import (
    BIGINT,
    JSON,
    String,
    Integer,
    Boolean,
    ForeignKey,
    Index,
    UniqueConstraint,
    text,
)
from sqlalchemy.engine import URL, make_url
from sqlalchemy.dialects.mysql import DATETIME
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
CURRENT_SCHEMA_REVISION = "0006_workflow_control_plane"
DATABASE_NOT_INITIALIZED_MESSAGE = "Database not initialized. Call init_db() first."
WORKFLOW_RUN_FOREIGN_KEY = "workflow_run.run_id"
WORKFLOW_TASK_FOREIGN_KEY = "workflow_task.task_id"


def normalize_mysql_dsn(dsn: str) -> URL:
    """将 MySQL 异步 DSN 解析为 aiomysql URL

    项目接受 ``mysql+asyncmy`` 作为配置别名，实际连接统一使用 aiomysql
    驱动，以保持已有部署变量可用并统一异步 MySQL 驱动边界。

    Args:
        dsn: 配置中的 SQLAlchemy 数据库 URL。

    Returns:
        已解析的 SQLAlchemy URL。

    Raises:
        ValueError: 当 DSN 不是支持的异步 MySQL 驱动时。
    """
    url = make_url(dsn)
    if url.drivername == "mysql+asyncmy":
        logger.warning(
            "deprecated_mysql_driver_alias",
            configured_driver="asyncmy",
            effective_driver="aiomysql",
        )
        return url.set(drivername="mysql+aiomysql")
    if url.drivername != "mysql+aiomysql":
        raise ValueError(
            "mysql_dsn must use the async mysql driver "
            "mysql+aiomysql:// (mysql+asyncmy:// is accepted as a compatibility alias)."
        )
    return url


class Base(AsyncAttrs, DeclarativeBase):
    """SQLAlchemy ORM 基类

    所有业务表模型均继承此基类，自动获得：
    - 异步属性加载能力（AsyncAttrs）
    - 声明式映射支持（DeclarativeBase）
    """

    pass


# 全局数据库对象
_engine: AsyncEngine | None = None
_session_maker: async_sessionmaker[AsyncSession] | None = None


def init_db() -> None:
    """初始化数据库引擎和会话工厂

    基于配置文件中的 mysql_dsn 创建异步数据库引擎，并设置连接池参数。
    该函数具有幂等性，重复调用不会重新创建 Engine。

    Raises:
        ValueError: 当 mysql_dsn 配置缺失或为空时
    """
    global _engine, _session_maker
    if _engine:
        return

    dsn = settings.get("mysql_dsn", "")
    if not dsn or str(dsn).strip() == "":
        logger.critical("startup_config_missing", field="mysql_dsn")
        raise ValueError(
            "CRITICAL: MySQL DSN is missing. Please set env var or add mysql_dsn to settings.toml."
        )

    _engine = create_async_engine(
        normalize_mysql_dsn(str(settings.mysql_dsn)),
        pool_size=settings.mysql_pool_size,
        max_overflow=settings.mysql_max_overflow,
        pool_recycle=settings.mysql_pool_recycle_s,
        echo=settings.debug,
        pool_pre_ping=True,
    )
    _session_maker = async_sessionmaker(bind=_engine, expire_on_commit=False, autoflush=False)


async def reset_db() -> None:
    """重置数据库状态 (用于多进程 Worker 场景)

    在进程复用场景下（如 ProcessPoolExecutor），旧的 Engine 会绑定到已关闭的 Loop。
    此方法用于强制销毁旧 Engine 并清空全局引用，允许后续重新调用 init_db。
    """
    global _engine, _session_maker
    if _engine:
        # 尝试优雅关闭连接池
        try:
            await _engine.dispose()
        except Exception as e:
            logger.warning("db_engine_dispose_error", error=str(e))

    _engine = None
    _session_maker = None


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """兼容旧依赖注入调用的数据库会话生成器。

    新代码应使用 :func:`get_session_context`，避免把单次会话误用为迭代器。

    Yields:
        AsyncSession: 已绑定引擎的会话对象
    """
    async with get_session_context() as session:
        yield session


@asynccontextmanager
async def get_session_context() -> AsyncGenerator[AsyncSession, None]:
    """以异步上下文管理器提供一个数据库会话。"""
    if _session_maker is None:
        raise RuntimeError(DATABASE_NOT_INITIALIZED_MESSAGE)

    async with _session_maker() as session:
        yield session


def get_session_maker() -> async_sessionmaker[AsyncSession]:
    """Return the initialized session factory for injected repositories."""
    if _session_maker is None:
        raise RuntimeError(DATABASE_NOT_INITIALIZED_MESSAGE)
    return _session_maker


async def create_all_tables() -> None:
    """创建所有已定义的数据库表

    此函数只供隔离测试准备临时数据库使用。生产环境必须通过 Alembic
    迁移入口管理 schema，应用启动不会自动执行 DDL。
    """
    if _engine is None:
        raise RuntimeError("Database not initialized.")

    async with _engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def verify_schema() -> None:
    """验证数据库已应用当前迁移版本

    Raises:
        RuntimeError: 当迁移版本缺失、过旧或数据库尚未初始化时。
    """
    if _engine is None:
        raise RuntimeError(DATABASE_NOT_INITIALIZED_MESSAGE)

    async with _engine.connect() as connection:
        try:
            result = await connection.execute(text("SELECT version_num FROM alembic_version"))
        except Exception as error:
            raise RuntimeError(
                "Database schema is not initialized; run 'piko db upgrade' first."
            ) from error
        revision = result.scalar_one_or_none()
    if revision != CURRENT_SCHEMA_REVISION:
        raise RuntimeError(
            f"Database schema revision {revision!r} does not match "
            f"application revision {CURRENT_SCHEMA_REVISION!r}."
        )


async def check_database_connection() -> bool:
    """执行轻量查询检查数据库连接是否可用。"""
    if _engine is None:
        return False
    try:
        async with _engine.connect() as connection:
            await connection.execute(text("SELECT 1"))
        return True
    except Exception as error:
        logger.warning("database_health_check_failed", error=str(error))
        return False


def utcnow() -> datetime.datetime:
    """获取当前 UTC 时间（无时区标记的 naive datetime）"""
    return datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)


# =============================================================================
# Piko 核心模型定义
# =============================================================================


class ScheduledJob(Base):
    """计划任务调度配置表"""

    __tablename__ = "scheduled_job"

    job_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    schedule_type: Mapped[str] = mapped_column(String(16), nullable=False)
    schedule_expr: Mapped[str] = mapped_column(String(512), nullable=False)
    timezone: Mapped[str] = mapped_column(String(64), default="Asia/Shanghai", nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    completed_at: Mapped[datetime.datetime | None] = mapped_column(DATETIME(fsp=6), nullable=True)

    misfire_grace_s: Mapped[int] = mapped_column(Integer, default=300, nullable=False)
    coalesce: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    max_instances: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    jitter_s: Mapped[int] = mapped_column(Integer, default=0, nullable=False)

    executor: Mapped[str] = mapped_column(String(16), default="cpu", nullable=False)
    concurrency_group: Mapped[str] = mapped_column(String(64), default="default", nullable=False)

    is_stateful: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    last_data_time: Mapped[datetime.datetime | None] = mapped_column(DATETIME(fsp=6), nullable=True)
    max_lookback_window: Mapped[int] = mapped_column(Integer, default=0, nullable=False)

    version: Mapped[int] = mapped_column(BIGINT, default=1, nullable=False)

    updated_at: Mapped[datetime.datetime] = mapped_column(
        DATETIME(fsp=6), default=utcnow, onupdate=utcnow, nullable=False
    )

    __table_args__ = (
        Index("idx_sjob_enabled", "enabled"),
        Index("idx_sjob_version", "version"),
        Index("idx_sjob_updated", "updated_at"),
    )


class JobConfig(Base):
    """任务配置表（支持版本化和灰度发布）"""

    __tablename__ = "job_config"
    job_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    schema_version: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    config_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    effective_from: Mapped[datetime.datetime | None] = mapped_column(DATETIME(fsp=6), nullable=True)

    version: Mapped[int] = mapped_column(BIGINT, default=1, nullable=False)
    updated_at: Mapped[datetime.datetime] = mapped_column(
        DATETIME(fsp=6), default=utcnow, onupdate=utcnow, nullable=False
    )

    __table_args__ = (
        Index("idx_jcfg_version", "version"),
        Index("idx_jcfg_updated", "updated_at"),
    )


class JobRun(Base):
    """任务执行记录表"""

    __tablename__ = "job_run"
    run_id: Mapped[int] = mapped_column(BIGINT, primary_key=True, autoincrement=True)
    job_id: Mapped[str] = mapped_column(String(128), nullable=False)

    scheduled_time: Mapped[datetime.datetime] = mapped_column(DATETIME(fsp=6), nullable=False)
    start_time: Mapped[datetime.datetime] = mapped_column(DATETIME(fsp=6), nullable=False)
    end_time: Mapped[datetime.datetime | None] = mapped_column(DATETIME(fsp=6), nullable=True)

    status: Mapped[str] = mapped_column(String(32), nullable=False)
    config_version: Mapped[int | None] = mapped_column(BIGINT, nullable=True)
    schedule_version: Mapped[int | None] = mapped_column(BIGINT, nullable=True)

    data_time_start: Mapped[datetime.datetime | None] = mapped_column(
        DATETIME(fsp=6), nullable=True
    )
    data_time_end: Mapped[datetime.datetime | None] = mapped_column(DATETIME(fsp=6), nullable=True)

    compute_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    persist_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    duration_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    attempt: Mapped[int] = mapped_column(Integer, default=1, nullable=False)

    error_type: Mapped[str | None] = mapped_column(String(64), nullable=True)
    error_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    error_msg: Mapped[str | None] = mapped_column(String(512), nullable=True)

    host: Mapped[str | None] = mapped_column(String(128), nullable=True)
    pid: Mapped[int | None] = mapped_column(Integer, nullable=True)

    created_at: Mapped[datetime.datetime] = mapped_column(
        DATETIME(fsp=6), default=utcnow, nullable=False
    )

    __table_args__ = (
        Index("idx_run_job_time", "job_id", "scheduled_time"),
        Index("idx_run_status", "status"),
        Index("idx_run_created", "created_at"),
        UniqueConstraint("job_id", "scheduled_time", "attempt", name="uq_run_job_time_attempt"),
    )


class JobLock(Base):
    """任务执行锁表（防止重复执行）"""

    __tablename__ = "job_lock"
    job_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    scheduled_time: Mapped[datetime.datetime] = mapped_column(DATETIME(fsp=6), primary_key=True)

    owner: Mapped[str] = mapped_column(String(128), nullable=False)
    owner_token: Mapped[str] = mapped_column(String(64), nullable=False)
    acquired_at: Mapped[datetime.datetime] = mapped_column(
        DATETIME(fsp=6), default=utcnow, nullable=False
    )
    expires_at: Mapped[datetime.datetime] = mapped_column(DATETIME(fsp=6), nullable=False)

    __table_args__ = (Index("idx_lock_owner", "owner"),)


class SchedulerLeader(Base):
    """调度器 Leader 选举表"""

    __tablename__ = "scheduler_leader"
    name: Mapped[str] = mapped_column(String(64), primary_key=True)
    owner: Mapped[str] = mapped_column(String(128), nullable=False)
    lease_until: Mapped[datetime.datetime] = mapped_column(DATETIME(fsp=6), nullable=False)
    version: Mapped[int] = mapped_column(Integer, default=0, nullable=False)

    updated_at: Mapped[datetime.datetime] = mapped_column(
        DATETIME(fsp=6), default=utcnow, onupdate=utcnow, nullable=False
    )


class WorkflowRun(Base):
    """Durable workflow instance; independent from the legacy job_run table."""

    __tablename__ = "workflow_run"

    run_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    workflow_id: Mapped[str] = mapped_column(String(128), nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String(255), nullable=False)
    config_snapshot_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    config_digest: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    business_result_status: Mapped[str] = mapped_column(String(32), nullable=False)
    started_at: Mapped[datetime.datetime | None] = mapped_column(DATETIME(fsp=6), nullable=True)
    finished_at: Mapped[datetime.datetime | None] = mapped_column(DATETIME(fsp=6), nullable=True)
    created_at: Mapped[datetime.datetime] = mapped_column(
        DATETIME(fsp=6), default=utcnow, nullable=False
    )
    updated_at: Mapped[datetime.datetime] = mapped_column(
        DATETIME(fsp=6), default=utcnow, onupdate=utcnow, nullable=False
    )

    __table_args__ = (
        UniqueConstraint("workflow_id", "idempotency_key", name="uq_workflow_run_idempotency"),
        Index("idx_workflow_run_status", "status"),
        Index("idx_workflow_run_updated", "updated_at"),
    )


class WorkflowTask(Base):
    """One technical task in one workflow run."""

    __tablename__ = "workflow_task"

    task_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    run_id: Mapped[str] = mapped_column(
        String(64), ForeignKey(WORKFLOW_RUN_FOREIGN_KEY, ondelete="CASCADE"), nullable=False
    )
    workflow_id: Mapped[str] = mapped_column(String(128), nullable=False)
    stage: Mapped[str] = mapped_column(String(128), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    attempt: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    max_attempts: Mapped[int] = mapped_column(Integer, default=3, nullable=False)
    available_at: Mapped[datetime.datetime | None] = mapped_column(DATETIME(fsp=6), nullable=True)
    owner: Mapped[str | None] = mapped_column(String(128), nullable=True)
    lock_token: Mapped[str | None] = mapped_column(String(128), nullable=True)
    lease_until: Mapped[datetime.datetime | None] = mapped_column(DATETIME(fsp=6), nullable=True)
    heartbeat_at: Mapped[datetime.datetime | None] = mapped_column(DATETIME(fsp=6), nullable=True)
    idempotency_key: Mapped[str] = mapped_column(String(255), nullable=False)
    error_code: Mapped[str | None] = mapped_column(String(128), nullable=True)
    error_message: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    started_at: Mapped[datetime.datetime | None] = mapped_column(DATETIME(fsp=6), nullable=True)
    finished_at: Mapped[datetime.datetime | None] = mapped_column(DATETIME(fsp=6), nullable=True)
    created_at: Mapped[datetime.datetime] = mapped_column(
        DATETIME(fsp=6), default=utcnow, nullable=False
    )
    updated_at: Mapped[datetime.datetime] = mapped_column(
        DATETIME(fsp=6), default=utcnow, onupdate=utcnow, nullable=False
    )

    __table_args__ = (
        UniqueConstraint("run_id", "stage", name="uq_workflow_task_run_stage"),
        UniqueConstraint("idempotency_key", name="uq_workflow_task_idempotency"),
        Index("idx_workflow_task_claim", "status", "available_at", "stage"),
        Index("idx_workflow_task_lease", "status", "lease_until"),
        Index("idx_workflow_task_run", "run_id", "status"),
    )


class WorkflowTaskDependency(Base):
    """Same-run DAG edge with explicit technical/business activation rules."""

    __tablename__ = "workflow_task_dependency"

    dependency_id: Mapped[int] = mapped_column(BIGINT, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(
        String(64), ForeignKey(WORKFLOW_RUN_FOREIGN_KEY, ondelete="CASCADE"), nullable=False
    )
    task_id: Mapped[str] = mapped_column(
        String(64), ForeignKey(WORKFLOW_TASK_FOREIGN_KEY, ondelete="CASCADE"), nullable=False
    )
    depends_on_task_id: Mapped[str] = mapped_column(
        String(64), ForeignKey(WORKFLOW_TASK_FOREIGN_KEY, ondelete="CASCADE"), nullable=False
    )
    condition_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)

    __table_args__ = (
        UniqueConstraint(
            "run_id", "task_id", "depends_on_task_id", name="uq_workflow_task_dependency"
        ),
        Index("idx_workflow_dependency_task", "run_id", "task_id"),
        Index("idx_workflow_dependency_upstream", "run_id", "depends_on_task_id"),
    )


class WorkflowTaskEvent(Base):
    """Append-only lifecycle audit event."""

    __tablename__ = "workflow_task_event"

    event_id: Mapped[int] = mapped_column(BIGINT, primary_key=True, autoincrement=True)
    task_id: Mapped[str] = mapped_column(
        String(64), ForeignKey(WORKFLOW_TASK_FOREIGN_KEY, ondelete="CASCADE"), nullable=False
    )
    run_id: Mapped[str] = mapped_column(
        String(64), ForeignKey(WORKFLOW_RUN_FOREIGN_KEY, ondelete="CASCADE"), nullable=False
    )
    stage: Mapped[str] = mapped_column(String(128), nullable=False)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    payload_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    created_at: Mapped[datetime.datetime] = mapped_column(
        DATETIME(fsp=6), default=utcnow, nullable=False
    )

    __table_args__ = (
        Index("idx_workflow_event_task_time", "task_id", "created_at"),
        Index("idx_workflow_event_run_stage", "run_id", "stage", "created_at"),
    )


class WorkflowTaskManifest(Base):
    """Business result manifest committed with technical finalization."""

    __tablename__ = "workflow_task_manifest"

    manifest_id: Mapped[int] = mapped_column(BIGINT, primary_key=True, autoincrement=True)
    task_id: Mapped[str] = mapped_column(
        String(64), ForeignKey(WORKFLOW_TASK_FOREIGN_KEY, ondelete="CASCADE"), nullable=False
    )
    run_id: Mapped[str] = mapped_column(
        String(64), ForeignKey(WORKFLOW_RUN_FOREIGN_KEY, ondelete="CASCADE"), nullable=False
    )
    idempotency_key: Mapped[str] = mapped_column(String(255), nullable=False)
    result_status: Mapped[str] = mapped_column(String(32), nullable=False)
    result_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    output_digest: Mapped[str | None] = mapped_column(String(128), nullable=True)
    created_at: Mapped[datetime.datetime] = mapped_column(
        DATETIME(fsp=6), default=utcnow, nullable=False
    )
    updated_at: Mapped[datetime.datetime] = mapped_column(
        DATETIME(fsp=6), default=utcnow, onupdate=utcnow, nullable=False
    )

    __table_args__ = (
        UniqueConstraint("task_id", name="uq_workflow_manifest_task"),
        UniqueConstraint("idempotency_key", name="uq_workflow_manifest_idempotency"),
        Index("idx_workflow_manifest_run", "run_id"),
    )
