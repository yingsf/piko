from __future__ import annotations

import pytest

from tests.workflow.conftest import claim_time
from piko.workflow.types import OwnershipLostError, TaskResult


@pytest.mark.asyncio
async def test_old_token_cannot_finalize_retry_fail_or_cancel_after_reclaim(
    memory_backend, simple_definition, NOW
):
    await memory_backend.create_run(simple_definition, now=NOW)
    first = (
        await memory_backend.claim_ready_tasks(
            worker_id="a", stages=["source"], lease_until=claim_time(1), now=NOW, limit=1
        )
    )[0]
    assert not await memory_backend.retry_task(
        task_id=first.task_id,
        owner="a",
        lock_token="wrong",
        error_code="late",
        error_message="late",
        available_at=claim_time(3),
        now=NOW,
    )
    assert not await memory_backend.fail_task(
        task_id=first.task_id,
        owner="a",
        lock_token="wrong",
        error_code="late",
        error_message="late",
        now=NOW,
    )
    await memory_backend.recover_expired_running_tasks(now=claim_time(2))
    await memory_backend.recover_retry_waiting_tasks(now=claim_time(2))
    second = (
        await memory_backend.claim_ready_tasks(
            worker_id="b", stages=["source"], lease_until=claim_time(60), now=claim_time(2), limit=1
        )
    )[0]
    assert first.lock_token != second.lock_token
    assert not await memory_backend.retry_task(
        task_id=first.task_id,
        owner="a",
        lock_token=first.lock_token or "",
        error_code="late",
        error_message="late",
        available_at=claim_time(3),
        now=claim_time(2),
    )
    assert not await memory_backend.fail_task(
        task_id=first.task_id,
        owner="a",
        lock_token=first.lock_token or "",
        error_code="late",
        error_message="late",
        now=claim_time(2),
    )
    assert not await memory_backend.cancel_task(
        task_id=first.task_id, owner="a", lock_token=first.lock_token or "", now=claim_time(2)
    )
    with pytest.raises(OwnershipLostError):
        await memory_backend.finalize_task(
            task=first, result=TaskResult(result_status="complete"), now=claim_time(2)
        )
    assert await memory_backend.finalize_task(
        task=second, result=TaskResult(result_status="complete"), now=claim_time(3)
    )
