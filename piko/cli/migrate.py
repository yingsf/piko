"""带 MySQL advisory lock 的单副本数据库迁移执行器

迁移入口持有数据库级锁直到 Alembic 子进程结束，避免多个应用副本（例如
Kubernetes 多副本）同时执行 DDL。迁移脚本与 alembic 配置随 ``piko`` 包发布，
本模块通过 ``importlib.resources`` 在安装环境中定位它们，不依赖仓库布局。

数据库连接统一从 ``PIKO_MYSQL_DSN`` 读取（与应用启动使用同一配置源）。
"""

from __future__ import annotations

import asyncio
import os
import subprocess  # noqa: B404  # nosec B404  受控调用固定 alembic 子进程，参数固定不来自外部输入
import sys
from importlib.resources import files
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine
from sqlalchemy.pool import NullPool

from piko.config import settings
from piko.infra.db import normalize_mysql_dsn

LOCK_NAME = "piko-schema-migration"


def migrations_dir() -> Path:
    """返回包内 migrations 目录的文件系统路径

    Returns:
        指向已安装 ``piko.migrations`` 资源的路径。
    """
    return Path(str(files("piko.migrations")))


def alembic_ini_path() -> Path:
    """返回包内 alembic.ini 的文件系统路径"""
    return migrations_dir() / "alembic.ini"


async def _acquire_lock(connection: AsyncConnection, timeout_s: int) -> bool:
    """获取数据库级迁移锁

    Args:
        connection: 持有锁的异步连接。
        timeout_s: 等待锁的最长秒数。

    Returns:
        是否成功获取锁。
    """
    value = await connection.scalar(
        text("SELECT GET_LOCK(:lock_name, :timeout_s)"),
        {"lock_name": LOCK_NAME, "timeout_s": timeout_s},
    )
    return value == 1


async def _release_lock(connection: AsyncConnection) -> None:
    """释放数据库级迁移锁"""
    await connection.scalar(text("SELECT RELEASE_LOCK(:lock_name)"), {"lock_name": LOCK_NAME})


async def _current_revision_locked(connection: AsyncConnection) -> str | None:
    """读取当前 alembic 版本号

    Args:
        connection: 已建立异步连接。

    Returns:
        当前 ``alembic_version.version_num``，表不存在时返回 None。
    """
    try:
        result = await connection.execute(text("SELECT version_num FROM alembic_version"))
    except Exception:
        return None
    row = result.fetchone()
    return None if row is None else str(row[0])


async def detect_current_revision() -> str | None:
    """连接数据库并返回当前 schema 版本

    Returns:
        当前 ``alembic_version.version_num``，未初始化（表不存在）时返回 None。

    Raises:
        RuntimeError: 当未配置 ``mysql_dsn`` 时。
    """
    dsn = str(settings.get("mysql_dsn", ""))
    if not dsn:
        raise RuntimeError("未配置 mysql_dsn，请设置 PIKO_MYSQL_DSN")
    engine = create_async_engine(
        normalize_mysql_dsn(dsn),
        poolclass=NullPool,
        pool_pre_ping=True,
    )
    try:
        async with engine.connect() as connection:
            return await _current_revision_locked(connection)
    finally:
        await engine.dispose()


async def _run_migration(alembic_args: list[str], timeout_s: int) -> None:
    """在持有数据库锁的连接上运行 Alembic 子进程

    Args:
        alembic_args: 传递给 ``alembic`` 的位置参数（如 ``["upgrade", "head"]``）。
        timeout_s: advisory lock 等待秒数。

    Raises:
        RuntimeError: 当无法获取迁移锁时。
        subprocess.CalledProcessError: 当 Alembic 子进程失败时。
    """
    dsn = str(settings.get("mysql_dsn", ""))
    if not dsn:
        raise RuntimeError("未配置 mysql_dsn，请设置 PIKO_MYSQL_DSN")
    engine = create_async_engine(
        normalize_mysql_dsn(dsn),
        poolclass=NullPool,
        pool_pre_ping=True,
    )
    async with engine.connect() as connection:
        if not await _acquire_lock(connection, timeout_s):
            raise RuntimeError("无法获取数据库迁移锁，已有其他迁移执行者")
        try:
            await _run_alembic_async(alembic_args)
        finally:
            await _release_lock(connection)
    await engine.dispose()


async def _run_alembic_async(alembic_args: list[str]) -> None:
    """以异步子进程方式运行 Alembic，避免阻塞事件循环

    命令与参数均由代码控制（固定调用当前解释器的 alembic 模块，
    ``alembic_args`` 来源于 argparse 的受限 choices），不接受外部输入。

    Args:
        alembic_args: 传递给 ``alembic`` 的位置参数。

    Raises:
        subprocess.CalledProcessError: 当 Alembic 子进程返回非零退出码时。
    """
    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "alembic",
        "-c",
        str(alembic_ini_path()),
        *alembic_args,
        cwd=str(migrations_dir()),
    )
    returncode = await proc.wait()
    if returncode != 0:
        raise subprocess.CalledProcessError(returncode, ["alembic", *alembic_args])


def run_upgrade(revision: str = "head", *, lock_timeout_s: int | None = None) -> int:
    """升级数据库到指定版本

    Args:
        revision: 目标版本，默认 ``head``。
        lock_timeout_s: advisory lock 等待秒数，None 则读取环境变量默认值。

    Returns:
        进程退出码（0 表示成功）。
    """
    timeout = _resolve_timeout(lock_timeout_s)
    try:
        asyncio.run(_run_migration(["upgrade", revision], timeout))
    except (RuntimeError, subprocess.CalledProcessError) as error:
        print(f"升级失败: {error}", file=sys.stderr)
        return 1
    return 0


def run_downgrade(revision: str = "-1", *, lock_timeout_s: int | None = None) -> int:
    """降级数据库到指定版本

    Args:
        revision: 目标版本，默认 ``-1``（回退一步）。
        lock_timeout_s: advisory lock 等待秒数，None 则读取环境变量默认值。

    Returns:
        进程退出码（0 表示成功）。
    """
    timeout = _resolve_timeout(lock_timeout_s)
    try:
        asyncio.run(_run_migration(["downgrade", revision], timeout))
    except (RuntimeError, subprocess.CalledProcessError) as error:
        print(f"降级失败: {error}", file=sys.stderr)
        return 1
    return 0


def show_current() -> int:
    """打印当前数据库 schema 版本

    Returns:
        进程退出码（0 表示成功）。
    """
    try:
        revision = asyncio.run(detect_current_revision())
    except RuntimeError as error:
        print(f"查询当前版本失败: {error}", file=sys.stderr)
        return 1
    if revision is None:
        print("数据库尚未初始化，请先运行: piko db upgrade")
        return 0
    print(revision)
    return 0


def show_history() -> int:
    """打印迁移版本链（不连接数据库，仅读取包内迁移文件）

    Returns:
        进程退出码（0 表示成功）。
    """
    try:
        # 固定调用 alembic history，参数完全由代码控制。
        subprocess.run(  # noqa: B603  # nosec B603  受控调用，参数固定
            [
                sys.executable,
                "-m",
                "alembic",
                "-c",
                str(alembic_ini_path()),
                "history",
            ],
            cwd=str(migrations_dir()),
            check=True,
        )
    except subprocess.CalledProcessError as error:
        print(f"查询迁移历史失败: {error}", file=sys.stderr)
        return 1
    return 0


def _resolve_timeout(lock_timeout_s: int | None) -> int:
    """解析 advisory lock 等待秒数，默认从环境变量读取"""
    if lock_timeout_s is not None:
        return lock_timeout_s
    return int(os.environ.get("PIKO_MIGRATION_LOCK_TIMEOUT_S", "30"))
