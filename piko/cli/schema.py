"""Piko 数据库 schema 收敛 CLI。

schema 由当前代码中的 ``Base.metadata`` 定义。命令只执行幂等的结构收敛，
不创建版本表，也不提供自动降级；无法安全推导的破坏性变更必须由发布者
提供显式 SQL 或数据迁移代码。
"""

from __future__ import annotations

import asyncio
import os

from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from piko.config import settings
from piko.infra.db import normalize_mysql_dsn
from piko.infra.schema import SchemaReport, check_schema, ensure_schema


def _resolve_timeout(lock_timeout_s: int | None) -> int:
    """解析 schema advisory lock 等待秒数。"""
    if lock_timeout_s is not None:
        return lock_timeout_s
    raw = os.environ.get(
        "PIKO_SCHEMA_LOCK_TIMEOUT_S",
        "30",
    )
    timeout = int(raw)
    if timeout < 0:
        raise ValueError("schema lock timeout must be non-negative")
    return timeout


def _database_dsn() -> str:
    dsn = str(settings.get("mysql_dsn", ""))
    if not dsn.strip():
        raise RuntimeError("未配置 mysql_dsn，请设置 PIKO_MYSQL_DSN")
    return dsn


async def _reconcile_database(lock_timeout_s: int) -> SchemaReport:
    engine = create_async_engine(
        normalize_mysql_dsn(_database_dsn()),
        poolclass=NullPool,
        pool_pre_ping=True,
    )
    try:
        return await ensure_schema(engine, lock_timeout_s=lock_timeout_s)
    finally:
        await engine.dispose()


async def _check_database() -> SchemaReport:
    engine = create_async_engine(
        normalize_mysql_dsn(_database_dsn()),
        poolclass=NullPool,
        pool_pre_ping=True,
    )
    try:
        return await check_schema(engine)
    finally:
        await engine.dispose()


def run_upgrade(*, lock_timeout_s: int | None = None) -> int:
    """将数据库收敛到当前目标结构。"""
    try:
        report = asyncio.run(_reconcile_database(_resolve_timeout(lock_timeout_s)))
    except Exception as error:
        print(f"schema 收敛失败: {error}")
        return 1
    print(report.summary())
    return 0


def run_init(*, lock_timeout_s: int | None = None) -> int:
    """初始化空库或补齐已有数据库的 Piko schema。"""
    return run_upgrade(lock_timeout_s=lock_timeout_s)


def run_repair(*, lock_timeout_s: int | None = None) -> int:
    """重试之前失败的幂等 schema 收敛步骤。"""
    return run_upgrade(lock_timeout_s=lock_timeout_s)


def run_check() -> int:
    """检查数据库是否已经满足当前目标结构。"""
    try:
        report = asyncio.run(_check_database())
    except Exception as error:
        print(f"schema 检查失败: {error}")
        return 1
    print(report.summary())
    return 0 if report.is_synchronized else 1
