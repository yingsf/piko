from __future__ import annotations

import pytest
from sqlalchemy import text

from piko.workflow.types import DependencySpec, TaskResult, TaskSpec, TaskStatus, WorkflowDefinition
from tests.workflow.conftest import NOW, claim_time

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_mysql_claim_only_exhausts_registered_stages(mysql_backend):
    backend, maker, _ = mysql_backend
    run = await backend.create_run(
        WorkflowDefinition(
            "mysql",
            "registered-stage-filter",
            (TaskSpec("registered", max_attempts=1), TaskSpec("unregistered", max_attempts=1)),
        ),
        now=NOW,
    )
    async with maker() as session, session.begin():
        await session.execute(
            text(
                "UPDATE workflow_task SET attempt=max_attempts "
                "WHERE run_id=:run_id AND stage='unregistered'"
            ),
            {"run_id": run.run_id},
        )

    claimed = await backend.claim_ready_tasks(
        worker_id="worker",
        stages=["registered"],
        lease_until=claim_time(),
        now=NOW,
        limit=10,
    )
    assert [task.stage for task in claimed] == ["registered"]
    unregistered = None
    for task_id in await session_task_ids(maker, run.run_id):
        task = await backend.get_task(task_id)
        if task is not None and task.stage == "unregistered":
            unregistered = task
            break
    assert unregistered is not None
    assert unregistered.status == TaskStatus.READY.value
    assert unregistered.attempt == 1


async def session_task_ids(maker, run_id: str) -> list[str]:
    async with maker() as session:
        return list(
            (
                await session.execute(
                    text("SELECT task_id FROM workflow_task WHERE run_id=:run_id"),
                    {"run_id": run_id},
                )
            ).scalars()
        )


@pytest.mark.asyncio
async def test_mysql_activation_and_finalize_commit_run_business_state(mysql_backend):
    backend, maker, _ = mysql_backend
    run = await backend.create_run(
        WorkflowDefinition(
            "mysql",
            "lifecycle",
            (
                TaskSpec("root"),
                TaskSpec("child", (DependencySpec("root"),)),
            ),
        ),
        now=NOW,
    )
    root = (
        await backend.claim_ready_tasks(
            worker_id="worker", stages=["root"], lease_until=claim_time(), now=NOW, limit=1
        )
    )[0]
    assert await backend.finalize_task(
        task=root, result=TaskResult(result_status="complete"), now=NOW
    )
    assert await backend.activate_ready_tasks(now=NOW) == 1
    child = None
    for task_id in await session_task_ids(maker, run.run_id):
        task = await backend.get_task(task_id)
        if task is not None and task.stage == "child":
            child = task
            break
    assert child is not None
    assert child.status == TaskStatus.READY.value

    async with maker() as session:
        status, business_status = (
            await session.execute(
                text(
                    "SELECT status, business_result_status FROM workflow_run WHERE run_id=:run_id"
                ),
                {"run_id": run.run_id},
            )
        ).one()
    assert status == "running"
    assert business_status == "unknown"

    child = (
        await backend.claim_ready_tasks(
            worker_id="worker", stages=["child"], lease_until=claim_time(), now=NOW, limit=1
        )
    )[0]
    assert await backend.finalize_task(
        task=child, result=TaskResult(result_status="complete"), now=NOW
    )
    async with maker() as session:
        status, business_status = (
            await session.execute(
                text(
                    "SELECT status, business_result_status FROM workflow_run WHERE run_id=:run_id"
                ),
                {"run_id": run.run_id},
            )
        ).one()
    assert status == "succeeded"
    assert business_status == "complete"
