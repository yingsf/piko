from __future__ import annotations

import pytest

from tests.workflow.conftest import claim_time
from piko.workflow.types import TaskResult, TaskStatus


@pytest.mark.asyncio
async def test_business_output_manifest_task_event_and_run_commit_together(
    memory_backend, simple_definition, NOW
):
    run = await memory_backend.create_run(simple_definition, now=NOW)
    task = (
        await memory_backend.claim_ready_tasks(
            worker_id="w", stages=["source"], lease_until=claim_time(), now=NOW, limit=1
        )
    )[0]

    async def hook(tx):
        tx.put("authoritative", {"value": 1})

    assert await memory_backend.finalize_task(
        task=task,
        result=TaskResult(result_status="complete", result_payload={"count": 1}),
        now=NOW,
        business_hook=hook,
    )
    assert memory_backend.business_outputs == {"authoritative": {"value": 1}}
    assert (await memory_backend.get_manifest(task.task_id)).result_status == "complete"
    assert (await memory_backend.get_task(task.task_id)).status == TaskStatus.SUCCEEDED.value
    assert (await memory_backend.list_events(task_id=task.task_id))[-1].event_type == "finalize"
    assert (await memory_backend.get_task(task.task_id)).run_id == run.run_id
    assert memory_backend.runs[run.run_id].business_result_status == "unknown"


@pytest.mark.asyncio
async def test_failure_after_business_write_rolls_back_all_authoritative_state(
    memory_backend, simple_definition, NOW
):
    await memory_backend.create_run(simple_definition, now=NOW)
    task = (
        await memory_backend.claim_ready_tasks(
            worker_id="w", stages=["source"], lease_until=claim_time(), now=NOW, limit=1
        )
    )[0]

    async def failing_hook(tx):
        tx.put("authoritative", "must_rollback")
        raise RuntimeError("injected hook failure")

    with pytest.raises(RuntimeError):
        await memory_backend.finalize_task(
            task=task,
            result=TaskResult(result_status="complete"),
            now=NOW,
            business_hook=failing_hook,
        )
    assert memory_backend.business_outputs == {}
    assert await memory_backend.get_manifest(task.task_id) is None
    assert (await memory_backend.get_task(task.task_id)).status == TaskStatus.RUNNING.value
    assert not any(
        event.event_type == "finalize"
        for event in await memory_backend.list_events(task_id=task.task_id)
    )
