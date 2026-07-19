from __future__ import annotations

import pytest

from tests.workflow.conftest import claim_time
from piko.workflow.types import TaskSpec, WorkflowDefinition


@pytest.mark.asyncio
async def test_crash_after_claim_is_recovered_without_attempt_rewrite(memory_backend, NOW):
    await memory_backend.create_run(
        WorkflowDefinition("chaos", "claim", (TaskSpec("stage"),)), now=NOW
    )
    task = (
        await memory_backend.claim_ready_tasks(
            worker_id="dead", stages=["stage"], lease_until=claim_time(1), now=NOW, limit=1
        )
    )[0]
    assert await memory_backend.recover_expired_running_tasks(now=claim_time(2)) == 1
    recovered = await memory_backend.get_task(task.task_id)
    assert recovered.attempt == 1
    assert recovered.owner is None
