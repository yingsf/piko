from __future__ import annotations

import os
from collections.abc import AsyncIterator

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from piko.infra.db import normalize_mysql_dsn
from piko.workflow.mysql_repository import MySQLWorkflowRepository
from piko.workflow.repository import InMemoryWorkflowRepository


@pytest.fixture
async def mysql_backend() -> AsyncIterator[
    tuple[MySQLWorkflowRepository, async_sessionmaker[AsyncSession], AsyncEngine]
]:
    dsn = os.environ.get("PIKO_TEST_MYSQL_DSN")
    if not dsn:
        pytest.skip("PIKO_TEST_MYSQL_DSN is required")
    engine = create_async_engine(normalize_mysql_dsn(dsn), pool_pre_ping=True)
    maker = async_sessionmaker(engine, expire_on_commit=False, autoflush=False)
    try:
        async with engine.connect() as connection:
            await connection.execute(text("SELECT 1 FROM workflow_run LIMIT 1"))
    except Exception as error:
        await engine.dispose()
        pytest.skip(f"workflow schema is not available; run migrations first: {error}")

    try:
        async with maker() as session, session.begin():
            await session.execute(text("DELETE FROM workflow_task_manifest"))
            await session.execute(text("DELETE FROM workflow_task_event"))
            await session.execute(text("DELETE FROM workflow_task_dependency"))
            await session.execute(text("DELETE FROM workflow_task"))
            await session.execute(text("DELETE FROM workflow_run"))
        yield MySQLWorkflowRepository(maker), maker, engine
    finally:
        await engine.dispose()


@pytest.fixture(params=("memory", "mysql"), ids=("memory", "mysql"))
async def contract_backend(
    request: pytest.FixtureRequest,
) -> AsyncIterator[InMemoryWorkflowRepository | MySQLWorkflowRepository]:
    """Run the same public workflow contract against both implementations."""
    if request.param == "memory":
        yield InMemoryWorkflowRepository()
        return
    dsn = os.environ.get("PIKO_TEST_MYSQL_DSN")
    if not dsn:
        pytest.skip("PIKO_TEST_MYSQL_DSN is required")
    engine = create_async_engine(normalize_mysql_dsn(dsn), pool_pre_ping=True)
    maker = async_sessionmaker(engine, expire_on_commit=False, autoflush=False)
    try:
        async with engine.connect() as connection:
            await connection.execute(text("SELECT 1 FROM workflow_run LIMIT 1"))
    except Exception as error:
        await engine.dispose()
        pytest.skip(f"workflow schema is not available; run migrations first: {error}")
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
