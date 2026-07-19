"""真实 MySQL 下的目标 schema 收敛契约。"""

from __future__ import annotations

from datetime import datetime
import os

import pytest
from sqlalchemy import (
    Column,
    Index,
    Integer,
    MetaData,
    Table,
    delete,
    inspect,
    insert,
    select,
    text,
)
from sqlalchemy.ext.asyncio import create_async_engine

from piko.infra.db import Base, normalize_mysql_dsn
from piko.infra.schema import SchemaMismatchError, _missing_indexes, check_schema, ensure_schema

pytestmark = pytest.mark.integration


def _mysql_test_dsn() -> str:
    """返回隔离 MySQL 集成测试 DSN。"""
    dsn = os.environ.get("PIKO_TEST_MYSQL_DSN")
    if not dsn:
        pytest.skip("PIKO_TEST_MYSQL_DSN is required")
    assert dsn is not None
    return dsn


async def _drop_piko_tables(engine) -> None:
    """删除测试库中的 Piko 目标表，不触碰其他业务表。"""
    async with engine.begin() as connection:
        await connection.execute(text("SET FOREIGN_KEY_CHECKS = 0"))
        for table in reversed(Base.metadata.sorted_tables):
            await connection.execute(text(f"DROP TABLE IF EXISTS `{table.name}`"))
        await connection.execute(text("DROP TABLE IF EXISTS `alembic_version`"))
        await connection.execute(text("SET FOREIGN_KEY_CHECKS = 1"))


@pytest.mark.asyncio
async def test_schema_reconciler_creates_and_rechecks_without_version_table() -> None:
    """空库自动建表，重复启动不产生版本表或重复 DDL。"""
    dsn = _mysql_test_dsn()
    engine = create_async_engine(normalize_mysql_dsn(dsn), pool_pre_ping=True)
    try:
        await _drop_piko_tables(engine)
        first = await ensure_schema(engine)
        second = await ensure_schema(engine)

        assert {
            "scheduled_job",
            "job_config",
            "job_run",
            "job_lock",
            "scheduler_leader",
            "workflow_run",
            "workflow_task",
            "workflow_task_dependency",
            "workflow_task_event",
            "workflow_task_manifest",
        } <= set(first.created_tables)
        assert not second.changed

        async with engine.connect() as connection:
            tables = await connection.run_sync(lambda sync: set(inspect(sync).get_table_names()))
        assert "alembic_version" not in tables
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_new_schema_defaults_support_readme_native_insert() -> None:
    """空库初始化后，README 的原生 scheduled_job 插入可以省略默认字段。"""
    dsn = _mysql_test_dsn()
    engine = create_async_engine(normalize_mysql_dsn(dsn), pool_pre_ping=True)
    job_id = "schema-default-native-insert"
    scheduled_job = Base.metadata.tables["scheduled_job"]
    try:
        await ensure_schema(engine)
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    "INSERT INTO scheduled_job(job_id, schedule_type, schedule_expr, enabled, version) "
                    'VALUES ("schema-default-native-insert", "cron", '
                    '\'{"minute":"0"}\', 1, 1) '
                    "ON DUPLICATE KEY UPDATE enabled = 1, "
                    'schedule_expr = \'{"minute":"0"}\', version = version + 1'
                )
            )

        async with engine.connect() as connection:
            row = (
                await connection.execute(
                    select(
                        scheduled_job.c.timezone,
                        scheduled_job.c.enabled,
                        scheduled_job.c.version,
                        scheduled_job.c.updated_at,
                    ).where(scheduled_job.c.job_id == job_id)
                )
            ).one()
            create_statement = (
                await connection.execute(text("SHOW CREATE TABLE `scheduled_job`"))
            ).one()[1]
        create_statement = str(create_statement).upper()

        assert row.timezone == "Asia/Shanghai"
        assert row.enabled is True
        assert row.version == 1
        assert row.updated_at is not None
        assert "`TIMEZONE` VARCHAR(64)" in create_statement
        assert "DEFAULT 'ASIA/SHANGHAI'" in create_statement
        assert "`UPDATED_AT` DATETIME(6)" in create_statement
        assert "DEFAULT CURRENT_TIMESTAMP(6)" in create_statement
    finally:
        async with engine.begin() as connection:
            await connection.execute(delete(scheduled_job).where(scheduled_job.c.job_id == job_id))
        await engine.dispose()


@pytest.mark.asyncio
async def test_schema_reconciler_repairs_missing_server_defaults() -> None:
    """已有表缺失数据库默认值时，check 报告并由 ensure 修复。"""
    dsn = _mysql_test_dsn()
    engine = create_async_engine(normalize_mysql_dsn(dsn), pool_pre_ping=True)
    try:
        await ensure_schema(engine)
        async with engine.begin() as connection:
            await connection.execute(
                text("ALTER TABLE `scheduled_job` ALTER COLUMN `timezone` DROP DEFAULT")
            )
            await connection.execute(
                text("ALTER TABLE `scheduled_job` ALTER COLUMN `updated_at` DROP DEFAULT")
            )
            await connection.execute(
                text("ALTER TABLE `job_config` ALTER COLUMN `updated_at` DROP DEFAULT")
            )

        report = await check_schema(engine)
        assert {
            "scheduled_job.timezone",
            "scheduled_job.updated_at",
            "job_config.updated_at",
        } <= set(report.missing_defaults)

        repaired = await ensure_schema(engine)
        assert {
            "scheduled_job.timezone",
            "scheduled_job.updated_at",
            "job_config.updated_at",
        } <= set(repaired.updated_defaults)
        assert (await check_schema(engine)).is_synchronized
    finally:
        await ensure_schema(engine)
        await engine.dispose()


@pytest.mark.asyncio
async def test_schema_reconciler_upgrades_legacy_job_lock_and_preserves_data() -> None:
    """旧锁表缺少租约字段时自动回填，已有锁记录保持可读。"""
    dsn = _mysql_test_dsn()
    engine = create_async_engine(normalize_mysql_dsn(dsn), pool_pre_ping=True)
    try:
        await ensure_schema(engine)
        async with engine.begin() as connection:
            await connection.execute(text("DROP TABLE IF EXISTS `job_lock`"))
            await connection.execute(
                text(
                    "CREATE TABLE `job_lock` ("
                    "`job_id` VARCHAR(128) NOT NULL, "
                    "`scheduled_time` DATETIME(6) NOT NULL, "
                    "`owner` VARCHAR(128) NOT NULL, "
                    "`acquired_at` DATETIME(6) NOT NULL, "
                    "PRIMARY KEY (`job_id`, `scheduled_time`)"
                    ")"
                )
            )
            await connection.execute(
                text(
                    "INSERT INTO `job_lock` "
                    "(`job_id`, `scheduled_time`, `owner`, `acquired_at`) "
                    "VALUES ('legacy-job', '2026-01-01 00:00:00.000000', 'legacy-owner', "
                    "'2026-01-01 00:00:00.000000')"
                )
            )

        report = await ensure_schema(engine)
        assert "job_lock.owner_token" in report.added_columns
        assert "job_lock.expires_at" in report.added_columns

        async with engine.connect() as connection:
            row = (
                await connection.execute(
                    text(
                        "SELECT owner, owner_token, expires_at FROM `job_lock` "
                        "WHERE job_id = 'legacy-job'"
                    )
                )
            ).one()
        assert row.owner == "legacy-owner"
        assert row.owner_token
        assert row.expires_at is not None
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_schema_reconciler_replays_legacy_job_data_migrations() -> None:
    """旧一次性任务和重复 job_run 在补约束前完成兼容迁移。"""
    dsn = _mysql_test_dsn()
    engine = create_async_engine(normalize_mysql_dsn(dsn), pool_pre_ping=True)
    job_id = "schema-legacy-date-job"
    scheduled_time = datetime(2026, 1, 1, 1, 0, 0)
    first_end = datetime(2026, 1, 1, 1, 1, 0)
    second_end = datetime(2026, 1, 1, 1, 2, 0)
    scheduled_job = Base.metadata.tables["scheduled_job"]
    job_run = Base.metadata.tables["job_run"]
    try:
        await ensure_schema(engine)
        async with engine.begin() as connection:
            await connection.execute(delete(job_run).where(job_run.c.job_id == job_id))
            await connection.execute(delete(scheduled_job).where(scheduled_job.c.job_id == job_id))
            await connection.execute(text("ALTER TABLE `scheduled_job` DROP COLUMN `completed_at`"))
            await connection.execute(
                text("ALTER TABLE `job_run` DROP INDEX `uq_run_job_time_attempt`")
            )
            await connection.execute(
                insert(scheduled_job).values(
                    job_id=job_id,
                    schedule_type="date",
                    schedule_expr="2026-01-01T01:00:00",
                    timezone="Asia/Shanghai",
                    enabled=True,
                    misfire_grace_s=300,
                    coalesce=True,
                    max_instances=1,
                    jitter_s=0,
                    executor="cpu",
                    concurrency_group="default",
                    is_stateful=False,
                    max_lookback_window=0,
                    version=1,
                    updated_at=scheduled_time,
                )
            )
            first = await connection.execute(
                insert(job_run).values(
                    job_id=job_id,
                    scheduled_time=scheduled_time,
                    start_time=scheduled_time,
                    end_time=first_end,
                    status="SUCCESS",
                    attempt=1,
                    created_at=first_end,
                )
            )
            first_key = first.inserted_primary_key
            assert first_key is not None
            first_run_id = first_key[0]
            second = await connection.execute(
                insert(job_run).values(
                    job_id=job_id,
                    scheduled_time=scheduled_time,
                    start_time=scheduled_time,
                    end_time=second_end,
                    status="SUCCESS",
                    attempt=1,
                    created_at=second_end,
                )
            )
            second_key = second.inserted_primary_key
            assert second_key is not None
            second_run_id = second_key[0]

        report = await ensure_schema(engine)

        assert "scheduled_job.completed_at" in report.added_columns
        assert "job_run_duplicate_cleanup:1" in report.compatibility_actions
        assert "date_job_completion_backfill:1" in report.compatibility_actions
        async with engine.connect() as connection:
            remaining_runs = (
                (
                    await connection.execute(
                        select(job_run.c.run_id).where(job_run.c.job_id == job_id)
                    )
                )
                .scalars()
                .all()
            )
            scheduled = (
                await connection.execute(
                    select(scheduled_job.c.enabled, scheduled_job.c.completed_at).where(
                        scheduled_job.c.job_id == job_id
                    )
                )
            ).one()
        assert remaining_runs == [second_run_id]
        assert first_run_id not in remaining_runs
        assert scheduled.enabled is False
        assert scheduled.completed_at == second_end
    finally:
        async with engine.begin() as connection:
            await connection.execute(delete(job_run).where(job_run.c.job_id == job_id))
            await connection.execute(delete(scheduled_job).where(scheduled_job.c.job_id == job_id))
        await ensure_schema(engine)
        await engine.dispose()


@pytest.mark.asyncio
async def test_schema_reconciler_rejects_index_uniqueness_mismatch() -> None:
    """已有同列但唯一性相反的索引不能被静默视为兼容。"""
    dsn = _mysql_test_dsn()
    engine = create_async_engine(normalize_mysql_dsn(dsn), pool_pre_ping=True)
    metadata = MetaData()
    table = Table(
        "piko_schema_index_probe",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("value", Integer, nullable=False),
    )
    Index("ix_piko_schema_index_probe_value", table.c.value)
    try:
        async with engine.begin() as connection:
            await connection.run_sync(metadata.create_all)
            await connection.execute(
                text("DROP INDEX `ix_piko_schema_index_probe_value` ON `piko_schema_index_probe`")
            )
            await connection.execute(
                text(
                    "CREATE UNIQUE INDEX `ix_piko_schema_index_probe_value` "
                    "ON `piko_schema_index_probe` (`value`)"
                )
            )

        with pytest.raises(SchemaMismatchError, match="incompatible uniqueness"):
            async with engine.connect() as connection:
                await connection.run_sync(lambda sync: _missing_indexes(table, inspect(sync)))
    finally:
        async with engine.begin() as connection:
            await connection.run_sync(metadata.drop_all)
        await engine.dispose()
