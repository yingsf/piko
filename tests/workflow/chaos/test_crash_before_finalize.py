from __future__ import annotations

import pytest

from tests.workflow.conftest import claim_time
from piko.workflow.types import TaskResult, TaskSpec, WorkflowDefinition


@pytest.mark.asyncio
async def test_crash_before_finalize_leaves_no_manifest_and_can_retry(memory_backend, NOW):
    await memory_backend.create_run(
        WorkflowDefinition("chaos", "finalize", (TaskSpec("stage"),)), now=NOW
    )
    task = (
        await memory_backend.claim_ready_tasks(
            worker_id="dead", stages=["stage"], lease_until=claim_time(1), now=NOW, limit=1
        )
    )[0]

    async def crash(_tx):
        raise RuntimeError("crash before finalize")

    with pytest.raises(RuntimeError):
        await memory_backend.finalize_task(
            task=task, result=TaskResult(result_status="complete"), now=NOW, business_hook=crash
        )
    assert await memory_backend.get_manifest(task.task_id) is None
    assert await memory_backend.recover_expired_running_tasks(now=claim_time(2)) == 1
