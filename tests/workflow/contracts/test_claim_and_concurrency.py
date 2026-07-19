from __future__ import annotations

import asyncio

import pytest

from tests.workflow.conftest import claim_time
from piko.workflow.types import WorkflowDefinition, TaskSpec


@pytest.mark.asyncio
async def test_concurrent_claim_has_one_winner_and_increments_once(
    memory_backend, simple_definition, NOW
):
    await memory_backend.create_run(simple_definition, now=NOW)
    a, b = await asyncio.gather(
        memory_backend.claim_ready_tasks(
            worker_id="a", stages=["source"], lease_until=claim_time(), now=NOW, limit=1
        ),
        memory_backend.claim_ready_tasks(
            worker_id="b", stages=["source"], lease_until=claim_time(), now=NOW, limit=1
        ),
    )
    assert sorted((len(a), len(b))) == [0, 1]
    winner = a[0] if a else b[0]
    assert winner.attempt == 1
    assert winner.lock_token


@pytest.mark.asyncio
async def test_claim_filters_registered_stage_and_future_available_at(memory_backend, NOW):
    definition = WorkflowDefinition(
        "wf", "claim", (TaskSpec("registered"), TaskSpec("unregistered"))
    )
    await memory_backend.create_run(definition, now=NOW)
    assert (
        await memory_backend.claim_ready_tasks(
            worker_id="w", stages=["unknown"], lease_until=claim_time(), now=NOW, limit=10
        )
        == []
    )
    claimed = await memory_backend.claim_ready_tasks(
        worker_id="w", stages=["registered"], lease_until=claim_time(), now=NOW, limit=10
    )
    assert [task.stage for task in claimed] == ["registered"]


@pytest.mark.asyncio
async def test_ready_task_at_max_attempts_is_failed_before_claim(memory_backend, NOW):
    definition = WorkflowDefinition("wf", "max", (TaskSpec("stage", max_attempts=1),))
    await memory_backend.create_run(definition, now=NOW)
    task = next(iter(memory_backend.tasks.values()))
    from dataclasses import replace

    memory_backend.tasks[task.task_id] = replace(task, attempt=1)
    assert (
        await memory_backend.claim_ready_tasks(
            worker_id="w", stages=["stage"], lease_until=claim_time(), now=NOW, limit=1
        )
        == []
    )
    assert (await memory_backend.get_task(task.task_id)).status == "failed"
