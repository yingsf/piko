from __future__ import annotations

import pytest

from tests.workflow.conftest import NOW, claim_time
from piko.workflow.types import TaskSpec, WorkflowDefinition

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_mysql_recovery_changes_owner_without_incrementing_attempt(mysql_backend):
    backend, _, _ = mysql_backend
    await backend.create_run(WorkflowDefinition("mysql", "recovery", (TaskSpec("stage"),)), now=NOW)
    first = (
        await backend.claim_ready_tasks(
            worker_id="a", stages=["stage"], lease_until=claim_time(1), now=NOW, limit=1
        )
    )[0]
    assert await backend.recover_expired_running_tasks(now=claim_time(2)) == 1
    assert await backend.recover_retry_waiting_tasks(now=claim_time(2)) == 1
    second = (
        await backend.claim_ready_tasks(
            worker_id="b", stages=["stage"], lease_until=claim_time(60), now=claim_time(2), limit=1
        )
    )[0]
    assert second.attempt == first.attempt + 1
    assert second.lock_token != first.lock_token
