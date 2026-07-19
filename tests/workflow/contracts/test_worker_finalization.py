"""Verify the Worker passes handler business hooks into finalization."""

from datetime import datetime, timedelta

import pytest

from piko.workflow.types import BusinessResultStatus, TaskResult, TaskSpec, WorkflowDefinition
from piko.workflow.worker import WorkflowWorker, WorkflowWorkerConfig


NOW = datetime(2026, 1, 1)


@pytest.mark.asyncio
async def test_worker_handler_business_hook_uses_finalization_transaction(memory_backend):
    definition = WorkflowDefinition(
        workflow_id="worker-hook",
        idempotency_key="worker-hook",
        tasks=(TaskSpec(stage="stage", task_id="worker-hook-stage"),),
    )
    await memory_backend.create_run(definition, now=NOW)
    task = (
        await memory_backend.claim_ready_tasks(
            worker_id="worker",
            stages=["stage"],
            lease_until=NOW + timedelta(minutes=1),
            now=NOW,
            limit=1,
        )
    )[0]

    async def handler(_task):
        async def business_hook(tx):
            tx.put("authority", {"task_id": _task.task_id, "value": 1})

        return TaskResult(
            result_status=BusinessResultStatus.COMPLETE.value,
            result_payload={"value": 1},
            business_hook=business_hook,
        )

    worker = WorkflowWorker(
        backend=memory_backend,
        handlers={"stage": handler},
        config=WorkflowWorkerConfig(
            worker_id="worker",
            retry_backoff_base_seconds=0,
            retry_jitter_seconds=0,
        ),
        now=lambda: NOW,
    )
    await worker._run_one(task)

    assert memory_backend.business_outputs == {"authority": {"task_id": task.task_id, "value": 1}}
    assert (await memory_backend.get_manifest(task.task_id)).result_status == (
        BusinessResultStatus.COMPLETE.value
    )
