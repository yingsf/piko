from __future__ import annotations

import asyncio
import contextlib

import pytest

from tests.workflow.conftest import claim_time
from piko.workflow.types import TaskSpec, WorkflowDefinition
from piko.workflow.worker import WorkflowWorker, WorkflowWorkerConfig


@pytest.mark.asyncio
async def test_non_cooperative_handler_is_orphaned_and_task_is_recoverable(memory_backend, NOW):
    await memory_backend.create_run(
        WorkflowDefinition("chaos", "shutdown", (TaskSpec("stage"),)), now=NOW
    )
    claimed = (
        await memory_backend.claim_ready_tasks(
            worker_id="worker", stages=["stage"], lease_until=claim_time(100), now=NOW, limit=1
        )
    )[0]
    release = asyncio.Event()

    async def handler(_task):
        try:
            await release.wait()
        except asyncio.CancelledError:
            await release.wait()

    worker = WorkflowWorker(
        backend=memory_backend,
        handlers={"stage": handler},
        config=WorkflowWorkerConfig(
            worker_id="worker", task_timeout_seconds=0.01, cancel_cleanup_seconds=0
        ),
        now=lambda: NOW,
    )
    await worker._run_one(claimed)
    assert (await memory_backend.get_task(claimed.task_id)).status == "retry_waiting"
    release.set()
    for orphan in list(worker._orphaned):
        with contextlib.suppress(asyncio.CancelledError):
            await orphan


@pytest.mark.asyncio
async def test_shutdown_cancellation_cancels_and_tracks_handler_task(memory_backend, NOW):
    await memory_backend.create_run(
        WorkflowDefinition("chaos", "shutdown-cancel", (TaskSpec("stage"),)), now=NOW
    )
    claimed = (
        await memory_backend.claim_ready_tasks(
            worker_id="worker", stages=["stage"], lease_until=claim_time(100), now=NOW, limit=1
        )
    )[0]
    started = asyncio.Event()
    release = asyncio.Event()

    async def handler(_task):
        started.set()
        try:
            await release.wait()
        except asyncio.CancelledError:
            await release.wait()

    worker = WorkflowWorker(
        backend=memory_backend,
        handlers={"stage": handler},
        config=WorkflowWorkerConfig(
            worker_id="worker", cancel_cleanup_seconds=0.01, retry_jitter_seconds=0
        ),
        now=lambda: NOW,
    )
    run_task = asyncio.create_task(worker._run_one(claimed))
    await started.wait()
    run_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await run_task

    assert (await memory_backend.get_task(claimed.task_id)).status == "retry_waiting"
    assert len(worker._orphaned) == 1
    release.set()
    for orphan in list(worker._orphaned):
        with contextlib.suppress(asyncio.CancelledError):
            await orphan
