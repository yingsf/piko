from __future__ import annotations

import pytest

from tests.workflow.conftest import claim_time
from piko.workflow.types import DependencySpec, TaskResult, TaskSpec, TaskStatus, WorkflowDefinition


async def _finish(backend, stage: str, now, result_status="complete"):
    task = next(task for task in backend.tasks.values() if task.stage == stage)
    if task.status == TaskStatus.READY.value:
        task = (
            await backend.claim_ready_tasks(
                worker_id="w", stages=[stage], lease_until=claim_time(100), now=now, limit=1
            )
        )[0]
    await backend.finalize_task(task=task, result=TaskResult(result_status=result_status), now=now)


@pytest.mark.asyncio
async def test_fan_out_fan_in_and_multilayer_dag(memory_backend, NOW):
    definition = WorkflowDefinition(
        "dag",
        "dag-1",
        (
            TaskSpec("root"),
            TaskSpec("left", (DependencySpec("root"),)),
            TaskSpec("right", (DependencySpec("root"),)),
            TaskSpec("join", (DependencySpec("left"), DependencySpec("right"))),
            TaskSpec("tail", (DependencySpec("join"),)),
        ),
    )
    await memory_backend.create_run(definition, now=NOW)
    assert await memory_backend.activate_ready_tasks(now=NOW) == 0
    await _finish(memory_backend, "root", NOW)
    assert await memory_backend.activate_ready_tasks(now=NOW) == 2
    await _finish(memory_backend, "left", NOW)
    assert await memory_backend.activate_ready_tasks(now=NOW) == 0
    await _finish(memory_backend, "right", NOW)
    assert await memory_backend.activate_ready_tasks(now=NOW) == 1
    await _finish(memory_backend, "join", NOW)
    assert await memory_backend.activate_ready_tasks(now=NOW) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("result_status", ["partial", "unavailable"])
async def test_disallowed_business_result_blocks_downstream(memory_backend, NOW, result_status):
    definition = WorkflowDefinition(
        "business", result_status, (TaskSpec("a"), TaskSpec("b", (DependencySpec("a"),)))
    )
    await memory_backend.create_run(definition, now=NOW)
    await _finish(memory_backend, "a", NOW, result_status)
    assert await memory_backend.activate_ready_tasks(now=NOW) == 1
    downstream = next(task for task in memory_backend.tasks.values() if task.stage == "b")
    assert downstream.status == TaskStatus.BLOCKED.value
    assert (
        memory_backend.runs[next(iter(memory_backend.runs))].business_result_status == result_status
    )


@pytest.mark.asyncio
async def test_failed_or_canceled_upstream_does_not_leave_pending_downstream(memory_backend, NOW):
    definition = WorkflowDefinition(
        "blocked",
        "blocked-1",
        (TaskSpec("a", max_attempts=1), TaskSpec("b", (DependencySpec("a"),))),
    )
    await memory_backend.create_run(definition, now=NOW)
    a = (
        await memory_backend.claim_ready_tasks(
            worker_id="w", stages=["a"], lease_until=claim_time(1), now=NOW, limit=1
        )
    )[0]
    assert await memory_backend.fail_task(
        task_id=a.task_id,
        owner="w",
        lock_token=a.lock_token or "",
        error_code="bad",
        error_message="bad",
        now=NOW,
    )
    assert await memory_backend.activate_ready_tasks(now=NOW) == 1
    assert (
        next(task for task in memory_backend.tasks.values() if task.stage == "b").status
        == TaskStatus.BLOCKED.value
    )


@pytest.mark.asyncio
async def test_canceled_upstream_blocks_downstream_and_run_is_not_early_success(
    memory_backend, NOW
):
    definition = WorkflowDefinition(
        "canceled", "canceled-1", (TaskSpec("a"), TaskSpec("b", (DependencySpec("a"),)))
    )
    run = await memory_backend.create_run(definition, now=NOW)
    a = (
        await memory_backend.claim_ready_tasks(
            worker_id="w", stages=["a"], lease_until=claim_time(), now=NOW, limit=1
        )
    )[0]
    assert await memory_backend.cancel_task(
        task_id=a.task_id, owner="w", lock_token=a.lock_token, now=NOW
    )
    assert await memory_backend.activate_ready_tasks(now=NOW) == 1
    assert (
        next(task for task in memory_backend.tasks.values() if task.stage == "b").status
        == TaskStatus.BLOCKED.value
    )
    assert memory_backend.runs[run.run_id].status == "failed"


@pytest.mark.asyncio
async def test_partial_result_can_activate_only_when_edge_allows_it(memory_backend, NOW):
    definition = WorkflowDefinition(
        "partial-allowed",
        "partial-allowed-1",
        (
            TaskSpec("a"),
            TaskSpec("b", (DependencySpec("a", allowed_business_statuses=("partial",)),)),
        ),
    )
    await memory_backend.create_run(definition, now=NOW)
    await _finish(memory_backend, "a", NOW, "partial")
    assert await memory_backend.activate_ready_tasks(now=NOW) == 1
    assert (
        next(task for task in memory_backend.tasks.values() if task.stage == "b").status
        == TaskStatus.READY.value
    )


@pytest.mark.asyncio
async def test_lost_activation_is_repaired_by_a_later_worker(memory_backend, NOW):
    definition = WorkflowDefinition(
        "activation-repair",
        "activation-repair-1",
        (TaskSpec("a"), TaskSpec("b", (DependencySpec("a"),))),
    )
    await memory_backend.create_run(definition, now=NOW)
    await _finish(memory_backend, "a", NOW)
    original = memory_backend.activate_ready_tasks

    async def fail_once(*, now):
        memory_backend.activate_ready_tasks = original
        raise ConnectionError("activation database failure")

    memory_backend.activate_ready_tasks = fail_once
    with pytest.raises(ConnectionError):
        await memory_backend.activate_ready_tasks(now=NOW)
    assert await memory_backend.activate_ready_tasks(now=NOW) == 1
