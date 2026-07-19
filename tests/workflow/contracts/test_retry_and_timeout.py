from __future__ import annotations

import asyncio
from dataclasses import replace

import pytest

from tests.workflow.conftest import claim_time
from piko.workflow.types import TaskSpec, TaskStatus, WorkflowDefinition
from piko.workflow.worker import NonRetryableWorkflowError, WorkflowWorker, WorkflowWorkerConfig


async def _worker_for(backend, task, handler, NOW, **config):
    worker = WorkflowWorker(
        backend=backend,
        handlers={task.stage: handler},
        config=WorkflowWorkerConfig(worker_id="worker", **config),
        now=lambda: NOW,
    )
    await worker._run_one(task)
    return await backend.get_task(task.task_id)


@pytest.mark.asyncio
async def test_retry_does_not_increment_attempt_and_max_attempts_is_terminal(
    memory_backend, simple_definition, NOW
):
    await memory_backend.create_run(simple_definition, now=NOW)
    task = (
        await memory_backend.claim_ready_tasks(
            worker_id="a", stages=["source"], lease_until=claim_time(), now=NOW, limit=1
        )
    )[0]
    assert await memory_backend.retry_task(
        task_id=task.task_id,
        owner="a",
        lock_token=task.lock_token or "",
        error_code="temporary",
        error_message="temporary",
        available_at=claim_time(10),
        now=NOW,
    )
    assert (await memory_backend.get_task(task.task_id)).attempt == 1
    assert await memory_backend.recover_retry_waiting_tasks(now=claim_time(10)) == 1
    next_claim = (
        await memory_backend.claim_ready_tasks(
            worker_id="a",
            stages=["source"],
            lease_until=claim_time(20),
            now=claim_time(10),
            limit=1,
        )
    )[0]
    assert next_claim.attempt == 2
    memory_backend.tasks[next_claim.task_id] = replace(next_claim, max_attempts=2)
    await memory_backend.recover_expired_running_tasks(now=claim_time(21))
    failed = await memory_backend.get_task(task.task_id)
    assert failed is not None
    assert failed.status == TaskStatus.FAILED.value


@pytest.mark.asyncio
async def test_handler_exception_is_retryable_and_nonretryable_error_is_terminal(
    memory_backend, NOW
):
    await memory_backend.create_run(
        WorkflowDefinition("worker", "exception", (TaskSpec("stage"),)), now=NOW
    )
    task = (
        await memory_backend.claim_ready_tasks(
            worker_id="worker", stages=["stage"], lease_until=claim_time(), now=NOW, limit=1
        )
    )[0]

    async def raises(_task):
        raise RuntimeError("temporary failure")

    retrying = await _worker_for(memory_backend, task, raises, NOW, retry_jitter_seconds=0)
    assert retrying.status == TaskStatus.RETRY_WAITING.value
    assert retrying.error_code == "handler_exception"

    await memory_backend.create_run(
        WorkflowDefinition("worker", "terminal", (TaskSpec("stage"),)), now=NOW
    )
    terminal_task = (
        await memory_backend.claim_ready_tasks(
            worker_id="worker", stages=["stage"], lease_until=claim_time(), now=NOW, limit=1
        )
    )[0]

    async def invalid(_task):
        raise NonRetryableWorkflowError("invalid_input", "not retryable")

    failed = await _worker_for(memory_backend, terminal_task, invalid, NOW, retry_jitter_seconds=0)
    assert failed.status == TaskStatus.FAILED.value
    assert failed.error_code == "invalid_input"


@pytest.mark.asyncio
async def test_handler_timeout_is_bounded_and_task_returns_to_recovery_state(memory_backend, NOW):
    await memory_backend.create_run(
        WorkflowDefinition("worker", "timeout", (TaskSpec("stage"),)), now=NOW
    )
    task = (
        await memory_backend.claim_ready_tasks(
            worker_id="worker", stages=["stage"], lease_until=claim_time(60), now=NOW, limit=1
        )
    )[0]
    started = asyncio.Event()

    async def hangs(_task):
        started.set()
        await asyncio.Event().wait()

    result = await _worker_for(
        memory_backend,
        task,
        hangs,
        NOW,
        task_timeout_seconds=0.01,
        cancel_cleanup_seconds=0.05,
        lease_duration_seconds=1,
        retry_jitter_seconds=0,
    )
    assert started.is_set()
    assert result.status == TaskStatus.RETRY_WAITING.value
    assert result.error_code == "handler_timeout"
