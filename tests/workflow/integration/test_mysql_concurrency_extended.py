"""真实 MySQL 控制面并发竞争契约。"""

from __future__ import annotations

import asyncio

import pytest

from piko.workflow.mysql_repository import MySQLWorkflowRepository
from piko.workflow.types import (
    BusinessResultStatus,
    DependencySpec,
    OwnershipLostError,
    TaskResult,
    TaskSpec,
    TaskStatus,
    WorkflowDefinition,
    WorkflowEventType,
)
from tests.workflow.conftest import NOW, claim_time

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_mysql_heartbeat_and_recovery_have_one_linearized_winner(mysql_backend):
    backend, _, _ = mysql_backend
    await backend.create_run(
        WorkflowDefinition("mysql-concurrency", "heartbeat-recovery", (TaskSpec("stage"),)),
        now=NOW,
    )
    task = (
        await backend.claim_ready_tasks(
            worker_id="worker-a",
            stages=["stage"],
            lease_until=claim_time(1),
            now=NOW,
            limit=1,
        )
    )[0]

    heartbeat, recovered = await asyncio.gather(
        backend.heartbeat(
            task_id=task.task_id,
            owner="worker-a",
            lock_token=task.lock_token or "",
            lease_until=claim_time(60),
            now=claim_time(1),
        ),
        backend.recover_expired_running_tasks(now=claim_time(2)),
    )

    current = await backend.get_task(task.task_id)
    assert current is not None
    assert (heartbeat, recovered) in {(True, 0), (False, 1)}
    if heartbeat:
        assert current.status == TaskStatus.RUNNING.value
        assert current.owner == "worker-a"
        assert current.lease_until is not None and current.lease_until > claim_time(2)
    else:
        assert current.status == TaskStatus.RETRY_WAITING.value
        assert current.owner is None

    events = await backend.list_events(task_id=task.task_id)
    linearized_events = {
        WorkflowEventType.HEARTBEAT.value,
        WorkflowEventType.LEASE_EXPIRED.value,
    }
    assert sum(event.event_type in linearized_events for event in events) == 1


@pytest.mark.asyncio
async def test_mysql_manual_retry_and_finalize_have_one_winner(mysql_backend):
    backend, _, _ = mysql_backend
    await backend.create_run(
        WorkflowDefinition("mysql-concurrency", "manual-retry", (TaskSpec("stage"),)),
        now=NOW,
    )
    task = (
        await backend.claim_ready_tasks(
            worker_id="worker-a",
            stages=["stage"],
            lease_until=claim_time(),
            now=NOW,
            limit=1,
        )
    )[0]

    finalize_result, control_result = await asyncio.gather(
        backend.finalize_task(
            task=task,
            result=TaskResult(result_status=BusinessResultStatus.COMPLETE.value),
            now=NOW,
        ),
        backend.control_task(
            run_id=task.run_id,
            stage=task.stage,
            action="retry",
            reason_digest="concurrent-manual-retry",
            now=NOW,
        ),
        return_exceptions=True,
    )

    assert (finalize_result is True) ^ isinstance(control_result, dict)
    assert isinstance(finalize_result, bool) or isinstance(finalize_result, OwnershipLostError)
    current = await backend.get_task(task.task_id)
    assert current is not None
    if isinstance(control_result, dict):
        assert current.status == TaskStatus.READY.value
        assert await backend.get_manifest(task.task_id) is None
    else:
        assert current.status == TaskStatus.SUCCEEDED.value
        assert await backend.get_manifest(task.task_id) is not None


@pytest.mark.asyncio
async def test_mysql_expiry_recovery_and_reclaim_have_one_owner(mysql_backend):
    backend, _, _ = mysql_backend
    await backend.create_run(
        WorkflowDefinition("mysql-concurrency", "reclaim", (TaskSpec("stage"),)), now=NOW
    )
    task = (
        await backend.claim_ready_tasks(
            worker_id="worker-a",
            stages=["stage"],
            lease_until=claim_time(1),
            now=NOW,
            limit=1,
        )
    )[0]

    async def recover_and_claim():
        await backend.recover_retry_waiting_tasks(now=claim_time(2))
        return await backend.claim_ready_tasks(
            worker_id="worker-b",
            stages=["stage"],
            lease_until=claim_time(60),
            now=claim_time(2),
            limit=1,
        )

    recovered, reclaimed = await asyncio.gather(
        backend.recover_expired_running_tasks(now=claim_time(2)), recover_and_claim()
    )

    assert recovered == 1
    assert len(reclaimed) <= 1
    current = await backend.get_task(task.task_id)
    assert current is not None
    if reclaimed:
        assert current.status == TaskStatus.RUNNING.value
        assert current.owner == "worker-b"
    else:
        assert current.status == TaskStatus.RETRY_WAITING.value
        assert current.owner is None


@pytest.mark.asyncio
async def test_mysql_workers_activate_one_dag_downstream_task(mysql_backend):
    backend, maker, _ = mysql_backend
    run = await backend.create_run(
        WorkflowDefinition(
            "mysql-concurrency",
            "dag-activation",
            (
                TaskSpec("root"),
                TaskSpec("child", dependencies=(DependencySpec("root"),)),
            ),
        ),
        now=NOW,
    )
    root = (
        await backend.claim_ready_tasks(
            worker_id="worker-root",
            stages=["root"],
            lease_until=claim_time(),
            now=NOW,
            limit=1,
        )
    )[0]
    assert await backend.finalize_task(
        task=root,
        result=TaskResult(result_status=BusinessResultStatus.COMPLETE.value),
        now=NOW,
    )

    worker_a = MySQLWorkflowRepository(maker)
    worker_b = MySQLWorkflowRepository(maker)
    changed_a, changed_b = await asyncio.gather(
        worker_a.activate_ready_tasks(now=NOW), worker_b.activate_ready_tasks(now=NOW)
    )

    assert sorted((changed_a, changed_b)) == [0, 1]
    tasks = [
        task
        for task_id in await _task_ids(maker, run.run_id)
        if (task := await backend.get_task(task_id)) is not None and task.stage == "child"
    ]
    assert len(tasks) == 1
    assert tasks[0].status == TaskStatus.READY.value
    events = await backend.list_events(task_id=tasks[0].task_id)
    assert [event.event_type for event in events].count("ready") == 1


async def _task_ids(maker, run_id: str) -> list[str]:
    from sqlalchemy import text

    async with maker() as session:
        result = await session.execute(
            text("SELECT task_id FROM workflow_task WHERE run_id=:run_id"),
            {"run_id": run_id},
        )
        return list(result.scalars())
