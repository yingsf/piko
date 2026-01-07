import datetime
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
    """获取数据库会话的异步生成器（依赖注入模式）

    Yields:
        AsyncSession: 已绑定引擎的会话对象
    """
    if _session_maker is None:
        raise RuntimeError("Database not initialized. Call init_db() first.")

    async with _session_maker() as session:
        yield session


async def create_all_tables() -> None:
    """创建所有已定义的数据库表"""
    if _engine is None:
        raise RuntimeError("Database not initialized.")

    async with _engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


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

    misfire_grace_s: Mapped[int] = mapped_column(Integer, default=300, nullable=False)
    coalesce: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    max_instances: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    jitter_s: Mapped[int] = mapped_column(Integer, default=0, nullable=False)

    executor: Mapped[str] = mapped_column(String(16), default="cpu", nullable=False)
    concurrency_group: Mapped[str] = mapped_column(String(64), default="default", nullable=False)

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
    """任务配置表（支持版本化和灰度发布）"""
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
    """任务执行记录表"""
    __tablename__ = "job_run"
    run_id: Mapped[int] = mapped_column(BIGINT, primary_key=True, autoincrement=True)
    job_id: Mapped[str] = mapped_column(String(128), nullable=False)

    scheduled_time: Mapped[datetime.datetime] = mapped_column(DateTime(6), nullable=False)
    start_time: Mapped[datetime.datetime] = mapped_column(DateTime(6), nullable=False)
    end_time: Mapped[datetime.datetime | None] = mapped_column(DateTime(6), nullable=True)

    status: Mapped[str] = mapped_column(String(32), nullable=False)
    config_version: Mapped[int | None] = mapped_column(BIGINT, nullable=True)
    schedule_version: Mapped[int | None] = mapped_column(BIGINT, nullable=True)

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
    """任务执行锁表（防止重复执行）"""
    __tablename__ = "job_lock"
    job_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    scheduled_time: Mapped[datetime.datetime] = mapped_column(DateTime(6), primary_key=True)

    owner: Mapped[str] = mapped_column(String(128), nullable=False)
    acquired_at: Mapped[datetime.datetime] = mapped_column(DateTime(6), default=utcnow, nullable=False)

    __table_args__ = (
        Index("idx_lock_owner", "owner"),
    )


class SchedulerLeader(Base):
    """调度器 Leader 选举表"""
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
    """数据输出去重表（幂等性保证）"""
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
