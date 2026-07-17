#!/usr/bin/env python3
"""使用 MySQL advisory lock 执行单副本数据库迁移

用法：``python scripts/migrate.py upgrade``，默认目标为 ``head``。
迁移连接会持有数据库级锁，直到 Alembic 子进程结束，避免多个应用副本
同时执行 DDL。
"""

from __future__ import annotations

import argparse
import asyncio
import os
import subprocess
import sys
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine
from sqlalchemy.pool import NullPool

from piko.config import settings
from piko.infra.db import normalize_mysql_dsn

ROOT = Path(__file__).resolve().parents[1]
LOCK_NAME = "piko-schema-migration"


async def _acquire_lock(connection: AsyncConnection, timeout_s: int) -> bool:
    """获取数据库级迁移锁"""
    value = await connection.scalar(
        text("SELECT GET_LOCK(:lock_name, :timeout_s)"),
        {"lock_name": LOCK_NAME, "timeout_s": timeout_s},
    )
    return value == 1


async def _release_lock(connection: AsyncConnection) -> None:
    """释放数据库级迁移锁"""
    await connection.scalar(text("SELECT RELEASE_LOCK(:lock_name)"), {"lock_name": LOCK_NAME})


async def _run_migration(alembic_args: list[str], timeout_s: int) -> None:
    """在持有数据库锁的连接上运行 Alembic"""
    engine = create_async_engine(
        normalize_mysql_dsn(str(settings.mysql_dsn)),
        poolclass=NullPool,
        pool_pre_ping=True,
    )
    async with engine.connect() as connection:
        if not await _acquire_lock(connection, timeout_s):
            raise RuntimeError("无法获取数据库迁移锁，已有其他迁移执行者")
        try:
            subprocess.run(
                [sys.executable, "-m", "alembic", *alembic_args],
                cwd=ROOT,
                check=True,
            )
        finally:
            await _release_lock(connection)
    await engine.dispose()


def main() -> int:
    """解析迁移参数并运行单副本迁移"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("upgrade", "downgrade", "current"))
    parser.add_argument("revision", nargs="?", default="head")
    parser.add_argument(
        "--lock-timeout-s",
        type=int,
        default=int(os.environ.get("PIKO_MIGRATION_LOCK_TIMEOUT_S", "30")),
    )
    args = parser.parse_args()
    if args.lock_timeout_s < 0:
        parser.error("--lock-timeout-s must be non-negative")

    alembic_args = [args.command]
    if args.command != "current":
        alembic_args.append(args.revision)
    try:
        asyncio.run(_run_migration(alembic_args, args.lock_timeout_s))
    except (RuntimeError, subprocess.CalledProcessError) as error:
        print(f"迁移失败: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
