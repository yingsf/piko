from __future__ import annotations

import pytest

from tests.workflow.conftest import claim_time
from piko.workflow.types import TaskStatus


@pytest.mark.asyncio
async def test_heartbeat_requires_owner_token_and_live_lease(
    memory_backend, simple_definition, NOW
):
    await memory_backend.create_run(simple_definition, now=NOW)
    task = (
        await memory_backend.claim_ready_tasks(
            worker_id="a", stages=["source"], lease_until=claim_time(), now=NOW, limit=1
        )
    )[0]
    assert not await memory_backend.heartbeat(
        task_id=task.task_id,
        owner="b",
        lock_token=task.lock_token or "",
        lease_until=claim_time(120),
        now=NOW,
    )
    assert not await memory_backend.heartbeat(
        task_id=task.task_id, owner="a", lock_token="wrong", lease_until=claim_time(120), now=NOW
    )
    assert await memory_backend.heartbeat(
        task_id=task.task_id,
        owner="a",
        lock_token=task.lock_token or "",
        lease_until=claim_time(120),
        now=NOW,
    )
    assert not await memory_backend.heartbeat(
        task_id=task.task_id,
        owner="a",
        lock_token=task.lock_token or "",
        lease_until=claim_time(120),
        now=claim_time(121),
    )


@pytest.mark.asyncio
async def test_recovery_is_idempotent_and_does_not_increment_attempt(
    memory_backend, simple_definition, NOW
):
    await memory_backend.create_run(simple_definition, now=NOW)
    task = (
        await memory_backend.claim_ready_tasks(
            worker_id="a", stages=["source"], lease_until=claim_time(1), now=NOW, limit=1
        )
    )[0]
    assert await memory_backend.recover_expired_running_tasks(now=claim_time(2)) == 1
    assert await memory_backend.recover_expired_running_tasks(now=claim_time(2)) == 0
    recovered = await memory_backend.get_task(task.task_id)
    assert recovered is not None
    assert recovered.status == TaskStatus.RETRY_WAITING.value
    assert recovered.attempt == 1
    assert await memory_backend.recover_retry_waiting_tasks(now=claim_time(2)) == 1
    assert await memory_backend.recover_retry_waiting_tasks(now=claim_time(2)) == 0
