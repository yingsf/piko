from __future__ import annotations

import os
from datetime import datetime, timedelta
from collections.abc import AsyncIterator

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from piko.infra.db import normalize_mysql_dsn
from piko.infra.schema import ensure_schema
from piko.workflow.mysql_repository import MySQLWorkflowRepository
from piko.workflow.repository import InMemoryWorkflowRepository
from piko.workflow.repository import WorkflowControlBackend
from piko.workflow.types import DependencySpec, TaskSpec, WorkflowDefinition


NOW = datetime(2026, 1, 1)


@pytest.fixture
def memory_backend() -> InMemoryWorkflowRepository:
    return InMemoryWorkflowRepository()


@pytest.fixture(name="NOW")
def now_fixture() -> datetime:
    return NOW


@pytest.fixture
def simple_definition() -> WorkflowDefinition:
    return WorkflowDefinition(
        workflow_id="contract-workflow",
        idempotency_key="batch-1",
        tasks=(
            TaskSpec(stage="source"),
            TaskSpec(stage="transform", dependencies=(DependencySpec("source"),)),
        ),
        config_snapshot={"version": 1, "mode": "test"},
    )


@pytest.fixture(params=("memory", "mysql"), ids=("memory", "mysql"))
async def dual_backend(
    request: pytest.FixtureRequest,
) -> AsyncIterator[WorkflowControlBackend]:
    """Run the public workflow contract against both backend implementations."""
    if request.param == "memory":
        yield InMemoryWorkflowRepository()
        return

    dsn = os.environ.get("PIKO_TEST_MYSQL_DSN")
    if not dsn:
        pytest.skip("PIKO_TEST_MYSQL_DSN is required")
    engine = create_async_engine(normalize_mysql_dsn(dsn), pool_pre_ping=True)
    maker = async_sessionmaker(engine, expire_on_commit=False, autoflush=False)
    try:
        await ensure_schema(engine)
    except Exception as error:
        await engine.dispose()
        pytest.skip(f"workflow schema is not available: {error}")

    try:
        async with maker() as session, session.begin():
            await session.execute(text("DELETE FROM workflow_task_manifest"))
            await session.execute(text("DELETE FROM workflow_task_event"))
            await session.execute(text("DELETE FROM workflow_task_dependency"))
            await session.execute(text("DELETE FROM workflow_task"))
            await session.execute(text("DELETE FROM workflow_run"))
        yield MySQLWorkflowRepository(maker)
    finally:
        await engine.dispose()


def claim_time(seconds: int = 60) -> datetime:
    return NOW + timedelta(seconds=seconds)
