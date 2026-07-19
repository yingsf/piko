from __future__ import annotations

import asyncio

import pytest

from tests.workflow.conftest import claim_time
from piko.workflow.types import TaskSpec, WorkflowDefinition
from piko.workflow.worker import WorkflowWorker, WorkflowWorkerConfig


@pytest.mark.asyncio
async def test_heartbeat_loss_stops_old_handler_without_finalize(memory_backend, NOW):
    await memory_backend.create_run(
        WorkflowDefinition("chaos", "heartbeat", (TaskSpec("stage"),)), now=NOW
    )
    claimed = (
        await memory_backend.claim_ready_tasks(
            worker_id="worker", stages=["stage"], lease_until=claim_time(1), now=NOW, limit=1
        )
    )[0]

    async def handler(_task):
        await asyncio.sleep(1)

    original_heartbeat = memory_backend.heartbeat

    async def lost_heartbeat(**kwargs):
        return False

    memory_backend.heartbeat = lost_heartbeat
    worker = WorkflowWorker(
        backend=memory_backend,
        handlers={"stage": handler},
        config=WorkflowWorkerConfig(
            worker_id="worker",
            lease_duration_seconds=0.03,
            task_timeout_seconds=1,
            cancel_cleanup_seconds=0.05,
        ),
        now=lambda: NOW,
    )
    try:
        await worker._run_one(claimed)
    finally:
        memory_backend.heartbeat = original_heartbeat
    assert await memory_backend.get_manifest(claimed.task_id) is None
    assert (await memory_backend.get_task(claimed.task_id)).status == "running"
