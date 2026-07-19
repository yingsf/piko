"""Run the core workflow contract against memory and real MySQL backends."""

from datetime import datetime, timedelta

import pytest

from piko.workflow.types import (
    BusinessResultStatus,
    DependencySpec,
    OwnershipLostError,
    TaskResult,
    TaskSpec,
    TaskStatus,
    WorkflowDefinition,
)

pytestmark = pytest.mark.integration

NOW = datetime(2026, 1, 1)


async def _claim(
    backend,
    task_id: str,
    *,
    worker_id: str = "worker-a",
    now: datetime = NOW,
):
    claimed = await backend.claim_ready_tasks(
        worker_id=worker_id,
        stages=[task_id.split("-")[0]],
        lease_until=now + timedelta(minutes=1),
        now=now,
        limit=1,
    )
    assert len(claimed) == 1
    return claimed[0]


def _single_task_definition(name: str, *, max_attempts: int = 3) -> WorkflowDefinition:
    return WorkflowDefinition(
        workflow_id=f"dual-{name}",
        idempotency_key=f"dual-{name}",
        tasks=(
            TaskSpec(
                stage="stage",
                task_id=f"stage-{name}",
                max_attempts=max_attempts,
            ),
        ),
    )


@pytest.mark.asyncio
async def test_recovery_retry_and_max_attempts_are_backend_identical(contract_backend):
    backend = contract_backend
    definition = _single_task_definition("retry")
    await backend.create_run(definition, now=NOW)
    first = await _claim(backend, "stage-retry")

    assert first.attempt == 1
    assert await backend.retry_task(
        task_id=first.task_id,
        owner=first.owner,
        lock_token=first.lock_token,
        error_code="handler_exception",
        error_message="retry once",
        available_at=NOW + timedelta(seconds=30),
        now=NOW,
    )
    assert await backend.recover_retry_waiting_tasks(now=NOW) == 0
    assert await backend.recover_retry_waiting_tasks(now=NOW + timedelta(seconds=30)) == 1
    assert await backend.recover_retry_waiting_tasks(now=NOW + timedelta(seconds=30)) == 0

    second = await _claim(backend, "stage-retry", now=NOW + timedelta(seconds=30))
    assert second.attempt == 2

    exhausted_definition = _single_task_definition("exhausted", max_attempts=1)
    await backend.create_run(exhausted_definition, now=NOW)
    exhausted = await _claim(backend, "stage-exhausted")
    assert await backend.retry_task(
        task_id=exhausted.task_id,
        owner=exhausted.owner,
        lock_token=exhausted.lock_token,
        error_code="last_error",
        error_message="must become terminal",
        available_at=NOW,
        now=NOW,
    )
    terminal = await backend.get_task(exhausted.task_id)
    assert terminal is not None
    assert terminal.status == TaskStatus.FAILED.value
    events = await backend.list_events(task_id=exhausted.task_id)
    assert [event.event_type for event in events].count("retry") == 0


@pytest.mark.asyncio
async def test_fencing_blocks_stale_retry_and_finalize_on_both_backends(contract_backend):
    backend = contract_backend
    definition = _single_task_definition("fencing")
    await backend.create_run(definition, now=NOW)
    stale = await _claim(backend, "stage-fencing")
    assert await backend.recover_expired_running_tasks(now=NOW + timedelta(minutes=2)) == 1
    assert await backend.recover_retry_waiting_tasks(now=NOW + timedelta(minutes=2)) == 1
    current = await _claim(
        backend,
        "stage-fencing",
        worker_id="worker-b",
        now=NOW + timedelta(minutes=2),
    )

    assert not await backend.heartbeat(
        task_id=stale.task_id,
        owner=stale.owner,
        lock_token=stale.lock_token,
        lease_until=NOW + timedelta(minutes=3),
        now=NOW + timedelta(minutes=2),
    )
    assert not await backend.retry_task(
        task_id=stale.task_id,
        owner=stale.owner,
        lock_token=stale.lock_token,
        error_code="stale",
        error_message="must be fenced",
        available_at=NOW,
        now=NOW + timedelta(minutes=2),
    )
    with pytest.raises(OwnershipLostError):
        await backend.finalize_task(
            task=stale,
            result=TaskResult(result_status=BusinessResultStatus.COMPLETE.value),
            now=NOW + timedelta(minutes=2),
        )
    assert await backend.get_manifest(current.task_id) is None
    assert await backend.finalize_task(
        task=current,
        result=TaskResult(result_status=BusinessResultStatus.COMPLETE.value),
        now=NOW + timedelta(minutes=2),
    )


@pytest.mark.asyncio
async def test_dag_activation_requires_all_dependencies_and_is_idempotent(contract_backend):
    backend = contract_backend
    definition = WorkflowDefinition(
        workflow_id="dual-dag",
        idempotency_key="dual-dag",
        tasks=(
            TaskSpec(stage="root", task_id="root-dual"),
            TaskSpec(stage="left", task_id="left-dual", dependencies=(DependencySpec("root"),)),
            TaskSpec(stage="right", task_id="right-dual", dependencies=(DependencySpec("root"),)),
            TaskSpec(
                stage="join",
                task_id="join-dual",
                dependencies=(DependencySpec("left"), DependencySpec("right")),
            ),
        ),
    )
    await backend.create_run(definition, now=NOW)
    root = await _claim(backend, "root-dual")
    assert await backend.finalize_task(
        task=root,
        result=TaskResult(result_status=BusinessResultStatus.COMPLETE.value),
        now=NOW,
    )
    assert await backend.activate_ready_tasks(now=NOW) == 2
    assert await backend.activate_ready_tasks(now=NOW) == 0

    left = await _claim(backend, "left-dual")
    assert await backend.finalize_task(
        task=left,
        result=TaskResult(result_status=BusinessResultStatus.COMPLETE.value),
        now=NOW,
    )
    assert await backend.activate_ready_tasks(now=NOW) == 0
    right = await _claim(backend, "right-dual")
    assert await backend.finalize_task(
        task=right,
        result=TaskResult(result_status=BusinessResultStatus.PARTIAL.value),
        now=NOW,
    )
    assert await backend.activate_ready_tasks(now=NOW) == 1
    join = await backend.get_task("join-dual")
    assert join is not None
    assert join.status == TaskStatus.BLOCKED.value


@pytest.mark.asyncio
async def test_manual_control_is_audited_and_fenced(contract_backend):
    backend = contract_backend
    definition = _single_task_definition("control")
    run = await backend.create_run(definition, now=NOW)
    task = await _claim(backend, "stage-control")
    assert await backend.control_task(
        run_id=run.run_id,
        stage="stage",
        action="cancel",
        reason_digest="operator-cancel",
        now=NOW,
    ) == {
        "task_id": task.task_id,
        "stage": "stage",
        "status": TaskStatus.CANCELED.value,
        "action": "cancel",
    }
    assert await backend.get_task(task.task_id) is not None
    assert (await backend.get_task(task.task_id)).status == TaskStatus.CANCELED.value
    retry_result = await backend.control_task(
        run_id=run.run_id,
        stage="stage",
        action="retry",
        reason_digest="operator-retry",
        now=NOW,
    )
    assert retry_result is not None
    assert retry_result["status"] == TaskStatus.READY.value
    events = await backend.list_events(task_id=task.task_id)
    manual = [event for event in events if event.event_type == "manual_cancel"]
    assert len(manual) == 1
    assert manual[0].payload["reason_digest"] == "operator-cancel"


@pytest.mark.asyncio
async def test_manual_retry_and_unblock_are_available_on_both_backends(contract_backend):
    backend = contract_backend
    retry_definition = _single_task_definition("manual-retry")
    retry_run = await backend.create_run(retry_definition, now=NOW)
    failed = await _claim(backend, "stage-manual-retry")
    assert await backend.fail_task(
        task_id=failed.task_id,
        owner=failed.owner,
        lock_token=failed.lock_token,
        error_code="manual-test",
        error_message="operator retry",
        now=NOW,
    )
    retry_result = await backend.control_task(
        run_id=retry_run.run_id,
        stage="stage",
        action="retry",
        reason_digest="operator-retry",
        now=NOW,
    )
    assert retry_result is not None
    assert retry_result["status"] == TaskStatus.READY.value

    unblock_definition = WorkflowDefinition(
        workflow_id="dual-unblock",
        idempotency_key="dual-unblock",
        tasks=(
            TaskSpec(stage="root", task_id="root-unblock"),
            TaskSpec(
                stage="child",
                task_id="child-unblock",
                dependencies=(DependencySpec("root"),),
            ),
        ),
    )
    unblock_run = await backend.create_run(unblock_definition, now=NOW)
    root = await _claim(backend, "root-unblock")
    assert await backend.fail_task(
        task_id=root.task_id,
        owner=root.owner,
        lock_token=root.lock_token,
        error_code="blocked-upstream",
        error_message="operator unblock",
        now=NOW,
    )
    assert await backend.activate_ready_tasks(now=NOW) == 1
    unblock_result = await backend.control_task(
        run_id=unblock_run.run_id,
        stage="child",
        action="unblock",
        reason_digest="operator-unblock",
        now=NOW,
    )
    assert unblock_result is not None
    assert unblock_result["status"] == TaskStatus.READY.value
