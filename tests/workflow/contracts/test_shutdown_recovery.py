from __future__ import annotations

import asyncio

import pytest

from piko.workflow.types import TaskResult
from piko.workflow.worker import WorkflowWorker, WorkflowWorkerConfig


async def _noop_handler(_task):
    return None


@pytest.mark.asyncio
async def test_worker_without_registered_handlers_never_claims(
    memory_backend, simple_definition, NOW
):
    await memory_backend.create_run(simple_definition, now=NOW)
    worker = WorkflowWorker(
        backend=memory_backend,
        handlers={},
        config=WorkflowWorkerConfig(worker_id="idle", poll_interval_seconds=0.001),
        now=lambda: NOW,
    )
    run_task = asyncio.create_task(worker.run())
    await asyncio.sleep(0.01)
    worker.request_stop()
    await asyncio.wait_for(run_task, timeout=1)
    assert all(
        task.status == "ready" or task.status == "pending" for task in memory_backend.tasks.values()
    )


@pytest.mark.asyncio
async def test_worker_success_and_stop_do_not_reclaim_successful_task(
    memory_backend, simple_definition, NOW
):
    await memory_backend.create_run(simple_definition, now=NOW)

    async def handler(_task):
        return TaskResult(result_status="complete")

    worker = WorkflowWorker(
        backend=memory_backend,
        handlers={"source": handler},
        config=WorkflowWorkerConfig(worker_id="worker", poll_interval_seconds=0.001),
        now=lambda: NOW,
    )
    run_task = asyncio.create_task(worker.run())
    for _ in range(100):
        if any(task.status == "succeeded" for task in memory_backend.tasks.values()):
            break
        await asyncio.sleep(0.001)
    worker.request_stop()
    await asyncio.wait_for(run_task, timeout=1)
    source = next(task for task in memory_backend.tasks.values() if task.stage == "source")
    assert source.status == "succeeded"
    assert source.attempt == 1


@pytest.mark.asyncio
async def test_stop_during_activation_does_not_claim_new_work(
    memory_backend, simple_definition, NOW
):
    await memory_backend.create_run(simple_definition, now=NOW)
    worker = WorkflowWorker(
        backend=memory_backend,
        handlers={"source": _noop_handler},
        config=WorkflowWorkerConfig(worker_id="worker", poll_interval_seconds=0.001),
        now=lambda: NOW,
    )
    original = memory_backend.activate_ready_tasks

    async def stop_after_activation(*, now):
        worker.request_stop()
        return await original(now=now)

    memory_backend.activate_ready_tasks = stop_after_activation
    await worker._loop_once()
    assert all(task.status != "running" for task in memory_backend.tasks.values())
