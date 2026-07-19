from __future__ import annotations

import os
import subprocess
import sys

import pytest
from sqlalchemy import inspect, text
from sqlalchemy.ext.asyncio import create_async_engine

from piko.infra.db import normalize_mysql_dsn

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_workflow_schema_is_created_by_alembic_head_and_downgrade_roundtrip():
    dsn = os.environ.get("PIKO_TEST_MYSQL_DSN")
    if not dsn:
        pytest.skip("PIKO_TEST_MYSQL_DSN is required")
    env = os.environ.copy()
    env["PIKO_MYSQL_DSN"] = dsn
    upgrade = subprocess.run(
        [sys.executable, "-m", "alembic", "-c", "piko/migrations/alembic.ini", "upgrade", "head"],
        cwd=os.getcwd(),
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert upgrade.returncode == 0, upgrade.stderr
    engine = create_async_engine(normalize_mysql_dsn(dsn))
    try:
        async with engine.connect() as connection:
            tables = await connection.run_sync(lambda sync: set(inspect(sync).get_table_names()))
            version = (
                await connection.execute(text("SELECT version_num FROM alembic_version"))
            ).scalar_one()
        assert {
            "workflow_run",
            "workflow_task",
            "workflow_task_dependency",
            "workflow_task_event",
            "workflow_task_manifest",
        } <= tables
        assert version == "0006_workflow_control_plane"
    finally:
        await engine.dispose()

    downgrade = subprocess.run(
        [
            sys.executable,
            "-m",
            "alembic",
            "-c",
            "piko/migrations/alembic.ini",
            "downgrade",
            "schema_v1",
        ],
        cwd=os.getcwd(),
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert downgrade.returncode == 0, downgrade.stderr

    downgraded_engine = create_async_engine(normalize_mysql_dsn(dsn))
    try:
        async with downgraded_engine.connect() as connection:
            tables = await connection.run_sync(lambda sync: set(inspect(sync).get_table_names()))
            version = (
                await connection.execute(text("SELECT version_num FROM alembic_version"))
            ).scalar_one()
        assert {
            "workflow_run",
            "workflow_task",
            "workflow_task_dependency",
            "workflow_task_event",
            "workflow_task_manifest",
        }.isdisjoint(tables)
        assert version == "schema_v1"
    finally:
        await downgraded_engine.dispose()

    roundtrip = subprocess.run(
        [sys.executable, "-m", "alembic", "-c", "piko/migrations/alembic.ini", "upgrade", "head"],
        cwd=os.getcwd(),
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert roundtrip.returncode == 0, roundtrip.stderr
