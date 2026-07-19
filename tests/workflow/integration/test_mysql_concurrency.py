from __future__ import annotations

import asyncio

import pytest
from sqlalchemy import text

from tests.workflow.conftest import NOW, claim_time
from piko.workflow.types import TaskSpec, WorkflowDefinition

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_mysql_concurrent_claim_has_one_winner(mysql_backend):
    backend, _, _ = mysql_backend
    await backend.create_run(WorkflowDefinition("mysql", "claim", (TaskSpec("stage"),)), now=NOW)
    a, b = await asyncio.gather(
        backend.claim_ready_tasks(
            worker_id="a", stages=["stage"], lease_until=claim_time(), now=NOW, limit=1
        ),
        backend.claim_ready_tasks(
            worker_id="b", stages=["stage"], lease_until=claim_time(), now=NOW, limit=1
        ),
    )
    assert sorted((len(a), len(b))) == [0, 1]
    winner = a[0] if a else b[0]
    assert winner.attempt == 1
    assert winner.lock_token


@pytest.mark.asyncio
async def test_mysql_claim_skips_row_locked_by_another_transaction(mysql_backend):
    backend, maker, _ = mysql_backend
    first_run = await backend.create_run(
        WorkflowDefinition("mysql", "locked-1", (TaskSpec("stage"),)), now=NOW
    )
    second_run = await backend.create_run(
        WorkflowDefinition("mysql", "locked-2", (TaskSpec("stage"),)), now=NOW
    )
    async with maker() as session:
        first = (
            await session.execute(
                text("SELECT task_id FROM workflow_task WHERE run_id=:run_id"),
                {"run_id": first_run.run_id},
            )
        ).scalar_one()
        second = (
            await session.execute(
                text("SELECT task_id FROM workflow_task WHERE run_id=:run_id"),
                {"run_id": second_run.run_id},
            )
        ).scalar_one()
    locker = maker()
    await locker.begin()
    await locker.execute(
        text("SELECT task_id FROM workflow_task WHERE task_id=:task_id FOR UPDATE"),
        {"task_id": first},
    )
    try:
        claimed = await backend.claim_ready_tasks(
            worker_id="c", stages=["stage"], lease_until=claim_time(), now=NOW, limit=1
        )
    finally:
        await locker.rollback()
        await locker.close()
    assert [task.task_id for task in claimed] == [second]
