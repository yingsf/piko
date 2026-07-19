"""Piko CLI 的单元与集成测试

单元部分验证命令解析、迁移资源定位；集成部分（需 PIKO_TEST_MYSQL_DSN）在隔离
测试库上验证 ``piko db upgrade/current`` 端到端可用。
"""

import os
from pathlib import Path

import pytest

from piko.cli import migrate
from piko.cli.main import _build_parser
from piko.infra.db import CURRENT_SCHEMA_REVISION

# _build_parser 仅用于测试构造解析器，pyright 的私有访问告警在此抑制。
# pyright: reportPrivateUsage=false


# --------------------------------------------------------------------------- #
# 单元测试：不依赖数据库
# --------------------------------------------------------------------------- #


def test_parser_db_upgrade_defaults_to_head() -> None:
    """验证 ``piko db upgrade`` 不带 revision 时默认 head"""
    parser = _build_parser()
    args = parser.parse_args(["db", "upgrade"])
    assert args.command == "db"
    assert args.db_command == "upgrade"
    assert args.revision == "head"
    assert args.lock_timeout_s is None


def test_parser_db_downgrade_defaults_to_minus_one() -> None:
    """验证 ``piko db downgrade`` 默认回退一步"""
    parser = _build_parser()
    args = parser.parse_args(["db", "downgrade"])
    assert args.db_command == "downgrade"
    assert args.revision == "-1"


def test_parser_db_current_and_history_take_no_revision() -> None:
    """验证 current/history 不接受额外 revision 位置参数"""
    parser = _build_parser()
    assert parser.parse_args(["db", "current"]).db_command == "current"
    assert parser.parse_args(["db", "history"]).db_command == "history"


def test_parser_db_upgrade_accepts_explicit_revision_and_timeout() -> None:
    """验证 ``piko db upgrade schema_v1 --lock-timeout-s 5`` 解析正确"""
    parser = _build_parser()
    args = parser.parse_args(["db", "upgrade", "schema_v1", "--lock-timeout-s", "5"])
    assert args.revision == "schema_v1"
    assert args.lock_timeout_s == 5


def test_parser_rejects_unknown_db_command() -> None:
    """验证未知子命令被拒绝"""
    parser = _build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["db", "frobnicate"])


def test_migrations_dir_points_into_installed_package() -> None:
    """验证迁移目录定位到已安装 piko 包内的 piko/migrations"""
    migrations_dir = migrate.migrations_dir()
    alembic_ini = migrate.alembic_ini_path()

    assert migrations_dir.name == "migrations"
    # 路径应位于 piko 包内
    assert "piko" in Path(migrations_dir).parts
    # alembic.ini 与 versions/ 必须真实存在于安装环境中
    assert alembic_ini.is_file()
    assert (migrations_dir / "versions").is_dir()
    assert (migrations_dir / "env.py").is_file()


def test_schema_v1_revision_file_exists_in_package() -> None:
    """验证改名节点 schema_v1.py 随包发布"""
    versions_dir = migrate.migrations_dir() / "versions"
    assert (versions_dir / "schema_v1.py").is_file()


def test_current_schema_revision_constant_is_workflow_head() -> None:
    """Workflow tables are part of the current schema contract."""
    assert CURRENT_SCHEMA_REVISION == "0006_workflow_control_plane"


def test_resolve_timeout_reads_env_default(monkeypatch: pytest.MonkeyPatch) -> None:
    """验证未显式传入时从环境变量读取 advisory lock 超时"""
    monkeypatch.setenv("PIKO_MIGRATION_LOCK_TIMEOUT_S", "42")
    assert migrate._resolve_timeout(None) == 42  # noqa: SLF001
    assert migrate._resolve_timeout(7) == 7  # noqa: SLF001


def test_resolve_timeout_falls_back_to_30(monkeypatch: pytest.MonkeyPatch) -> None:
    """验证环境变量未设置时默认 30 秒"""
    monkeypatch.delenv("PIKO_MIGRATION_LOCK_TIMEOUT_S", raising=False)
    assert migrate._resolve_timeout(None) == 30  # noqa: SLF001


# --------------------------------------------------------------------------- #
# 集成测试：需要隔离的 MySQL 测试库
# --------------------------------------------------------------------------- #


@pytest.mark.integration
@pytest.mark.skipif(
    not os.getenv("PIKO_TEST_MYSQL_DSN"),
    reason="需要通过 PIKO_TEST_MYSQL_DSN 指定隔离测试数据库",
)
def test_db_upgrade_initializes_to_workflow_head() -> None:
    """验证空库执行 ``piko db upgrade`` 后版本为 workflow head

    采用同步测试函数：CLI 入口内部用 asyncio.run，在同步上下文调用才不会
    与已有事件循环冲突（真实命令行场景即如此）。conftest 已通过
    PIKO_TEST_MYSQL_DSN 把 PIKO_MYSQL_DSN 注入环境。
    """
    import asyncio

    import piko.infra.db as db_infra
    from sqlalchemy import text

    async def _drop_piko_tables() -> None:
        """清空库，确保从基线开始"""
        db_infra.init_db()
        async with db_infra.get_session_context() as session:
            for table in (
                "workflow_task_manifest",
                "workflow_task_event",
                "workflow_task_dependency",
                "workflow_task",
                "workflow_run",
                "job_lock",
                "job_run",
                "job_config",
                "scheduled_job",
                "scheduler_leader",
                "alembic_version",
            ):
                await session.execute(text(f"DROP TABLE IF EXISTS `{table}`"))
            await session.commit()
        await db_infra.reset_db()

    async def _read_version() -> str:
        db_infra.init_db()
        async with db_infra.get_session_context() as session:
            result = await session.execute(text("SELECT version_num FROM alembic_version"))
            version = result.scalar_one()
        await db_infra.reset_db()
        return str(version)

    # 1) 清库（独立事件循环）
    asyncio.run(_drop_piko_tables())

    # 2) 执行升级（run_upgrade 内部用 asyncio.run，必须在同步上下文调用）
    rc = migrate.run_upgrade("head")
    assert rc == 0

    # 3) 校验版本（独立事件循环）
    version = asyncio.run(_read_version())
    assert version == "0006_workflow_control_plane"
