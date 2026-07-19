from __future__ import annotations

import asyncio
from datetime import timedelta

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
from piko.workflow.worker import NonRetryableWorkflowError, WorkflowWorker, WorkflowWorkerConfig
from tests.workflow.conftest import NOW


pytestmark = pytest.mark.integration


async def _claim(
    backend, task_id: str, *, stage: str = "stage", now=NOW, worker_id: str = "worker"
):
    task = (
        await backend.claim_ready_tasks(
            worker_id=worker_id,
            stages=[stage],
            lease_until=now + timedelta(minutes=1),
            now=now,
            limit=1,
        )
    )[0]
    assert task.task_id == task_id
    return task


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "result_status",
    [
        BusinessResultStatus.PARTIAL.value,
        BusinessResultStatus.FAILED.value,
        BusinessResultStatus.EMPTY.value,
    ],
)
async def test_terminal_business_status_is_preserved_on_both_backends(dual_backend, result_status):
    definition = WorkflowDefinition(
        "dual-business-status",
        f"{result_status}-run",
        (TaskSpec("stage", task_id=f"stage-{result_status}"),),
    )
    run = await dual_backend.create_run(definition, now=NOW)
    task = await _claim(dual_backend, f"stage-{result_status}")

    assert await dual_backend.finalize_task(
        task=task, result=TaskResult(result_status=result_status), now=NOW
    )
    stored_run = await dual_backend.get_run(run.run_id)
    assert stored_run is not None
    assert stored_run.status == TaskStatus.SUCCEEDED.value
    assert stored_run.business_result_status == result_status


@pytest.mark.asyncio
async def test_idempotency_rerun_and_snapshot_contract_on_both_backends(dual_backend):
    config = {"nested": {"value": 1}}
    definition = WorkflowDefinition(
        "dual-idempotency",
        "batch-1",
        (TaskSpec("stage", task_id="stage-original"),),
        config_snapshot=config,
    )
    original = await dual_backend.create_run(definition, now=NOW)
    same = await dual_backend.create_run(definition, now=NOW)
    rerun = await dual_backend.create_run(
        WorkflowDefinition(
            "dual-idempotency",
            "batch-1",
            (TaskSpec("stage", task_id="stage-rerun"),),
            config_snapshot=config,
            rerun=True,
        ),
        now=NOW,
    )
    normal = await dual_backend.create_run(definition, now=NOW)

    assert same.run_id == original.run_id
    assert rerun.run_id != original.run_id
    assert normal.run_id == original.run_id
    config["nested"]["value"] = 9
    stored = await dual_backend.get_run(original.run_id)
    assert stored is not None
    assert stored.config_snapshot["nested"]["value"] == 1


@pytest.mark.asyncio
async def test_first_rerun_establishes_base_idempotency_on_both_backends(dual_backend):
    """验证首次 rerun 与普通创建在两个后端使用一致的基础幂等映射"""
    first = await dual_backend.create_run(
        WorkflowDefinition(
            "dual-first-rerun",
            "batch-1",
            (TaskSpec("stage"),),
            rerun=True,
        ),
        now=NOW,
    )
    normal = await dual_backend.create_run(
        WorkflowDefinition("dual-first-rerun", "batch-1", (TaskSpec("stage"),)),
        now=NOW,
    )
    second = await dual_backend.create_run(
        WorkflowDefinition(
            "dual-first-rerun",
            "batch-1",
            (TaskSpec("stage"),),
            rerun=True,
        ),
        now=NOW,
    )

    assert first.idempotency_key == "batch-1"
    assert normal.run_id == first.run_id
    assert second.run_id != first.run_id
    assert second.idempotency_key.startswith("batch-1:rerun:")


@pytest.mark.asyncio
async def test_heartbeat_recovery_and_attempt_contract_on_both_backends(dual_backend):
    await dual_backend.create_run(
        WorkflowDefinition(
            "dual-heartbeat",
            "heartbeat",
            (TaskSpec("stage", task_id="stage-heartbeat"),),
        ),
        now=NOW,
    )
    task = await _claim(dual_backend, "stage-heartbeat")

    assert await dual_backend.heartbeat(
        task_id=task.task_id,
        owner=task.owner or "",
        lock_token=task.lock_token or "",
        lease_until=NOW + timedelta(minutes=2),
        now=NOW,
    )
    assert await dual_backend.recover_expired_running_tasks(now=NOW + timedelta(minutes=2)) == 1
    recovered = await dual_backend.get_task(task.task_id)
    assert recovered is not None
    assert recovered.status == TaskStatus.RETRY_WAITING.value
    assert recovered.attempt == 1
    assert await dual_backend.recover_retry_waiting_tasks(now=NOW + timedelta(minutes=2)) == 1
    reclaimed = await _claim(
        dual_backend, task.task_id, now=NOW + timedelta(minutes=2), worker_id="worker-b"
    )
    assert reclaimed.attempt == 2


@pytest.mark.asyncio
async def test_fencing_contract_on_both_backends(dual_backend):
    await dual_backend.create_run(
        WorkflowDefinition(
            "dual-fencing", "fencing", (TaskSpec("stage", task_id="stage-fencing"),)
        ),
        now=NOW,
    )
    stale = await _claim(dual_backend, "stage-fencing")
    assert await dual_backend.recover_expired_running_tasks(now=NOW + timedelta(minutes=2)) == 1
    assert await dual_backend.recover_retry_waiting_tasks(now=NOW + timedelta(minutes=2)) == 1
    current = await _claim(
        dual_backend, "stage-fencing", now=NOW + timedelta(minutes=2), worker_id="worker-b"
    )

    assert not await dual_backend.retry_task(
        task_id=stale.task_id,
        owner=stale.owner or "",
        lock_token=stale.lock_token or "",
        error_code="stale",
        error_message="stale owner",
        available_at=NOW,
        now=NOW + timedelta(minutes=2),
    )
    with pytest.raises(OwnershipLostError):
        await dual_backend.finalize_task(
            task=stale,
            result=TaskResult(result_status=BusinessResultStatus.COMPLETE.value),
            now=NOW + timedelta(minutes=2),
        )
    assert await dual_backend.finalize_task(
        task=current,
        result=TaskResult(result_status=BusinessResultStatus.COMPLETE.value),
        now=NOW + timedelta(minutes=2),
    )


@pytest.mark.asyncio
async def test_cancel_unclaimed_states_contract_on_both_backends(dual_backend):
    definition = WorkflowDefinition(
        "dual-cancel",
        "cancel",
        (
            TaskSpec("root", task_id="root-cancel"),
            TaskSpec("child", (DependencySpec("root"),), task_id="child-cancel"),
        ),
    )
    await dual_backend.create_run(definition, now=NOW)
    pending = await dual_backend.get_task("child-cancel")
    assert pending is not None
    assert await dual_backend.cancel_task(task_id=pending.task_id, now=NOW)

    ready_definition = WorkflowDefinition(
        "dual-cancel-ready",
        "cancel-ready",
        (TaskSpec("stage", task_id="ready-stage"),),
    )
    await dual_backend.create_run(ready_definition, now=NOW)
    ready = await dual_backend.get_task("ready-stage")
    assert ready is not None
    assert await dual_backend.cancel_task(task_id=ready.task_id, now=NOW)

    retry_definition = WorkflowDefinition(
        "dual-cancel-retry",
        "cancel-retry",
        (TaskSpec("retry-stage", task_id="retry-task"),),
    )
    await dual_backend.create_run(retry_definition, now=NOW)
    claimed = await _claim(dual_backend, "retry-task", stage="retry-stage")
    assert await dual_backend.retry_task(
        task_id=claimed.task_id,
        owner=claimed.owner or "",
        lock_token=claimed.lock_token or "",
        error_code="retry",
        error_message="retry",
        available_at=NOW + timedelta(minutes=1),
        now=NOW,
    )
    assert await dual_backend.cancel_task(task_id=claimed.task_id, now=NOW)


@pytest.mark.asyncio
async def test_worker_timeout_contract_on_both_backends(dual_backend):
    await dual_backend.create_run(
        WorkflowDefinition(
            "dual-timeout", "timeout", (TaskSpec("stage", task_id="stage-timeout"),)
        ),
        now=NOW,
    )
    task = await _claim(dual_backend, "stage-timeout")

    async def handler(_task):
        await asyncio.Event().wait()

    worker = WorkflowWorker(
        backend=dual_backend,
        handlers={"stage": handler},
        config=WorkflowWorkerConfig(
            worker_id="worker",
            task_timeout_seconds=0.01,
            cancel_cleanup_seconds=0.01,
            retry_jitter_seconds=0,
        ),
        now=lambda: NOW,
    )
    await worker._run_one(task)
    stored = await dual_backend.get_task(task.task_id)
    assert stored is not None
    assert stored.status == TaskStatus.RETRY_WAITING.value
    assert stored.error_code == "handler_timeout"


@pytest.mark.asyncio
async def test_forced_shutdown_recovers_running_task_on_both_backends(dual_backend):
    await dual_backend.create_run(
        WorkflowDefinition(
            "dual-shutdown", "shutdown", (TaskSpec("stage", task_id="stage-shutdown"),)
        ),
        now=NOW,
    )
    started = asyncio.Event()
    release = asyncio.Event()

    async def handler(_task):
        started.set()
        try:
            await release.wait()
        except asyncio.CancelledError:
            await release.wait()

    worker = WorkflowWorker(
        backend=dual_backend,
        handlers={"stage": handler},
        config=WorkflowWorkerConfig(
            worker_id="worker",
            poll_interval_seconds=0.001,
            cancel_cleanup_seconds=0.01,
            shutdown_grace_seconds=0,
            retry_jitter_seconds=0,
        ),
        now=lambda: NOW,
    )
    worker_task = asyncio.create_task(worker.run())
    await started.wait()
    await worker.force_recover_inflight()
    recovered = await dual_backend.get_task("stage-shutdown")
    assert recovered is not None
    assert recovered.status == TaskStatus.RETRY_WAITING.value
    release.set()
    worker.request_stop()
    await asyncio.wait_for(worker_task, timeout=1)


@pytest.mark.asyncio
async def test_forced_shutdown_honors_max_attempts_on_both_backends(dual_backend):
    await dual_backend.create_run(
        WorkflowDefinition(
            "dual-shutdown-terminal",
            "shutdown-terminal",
            (TaskSpec("stage", task_id="stage-shutdown-terminal", max_attempts=1),),
        ),
        now=NOW,
    )
    started = asyncio.Event()
    release = asyncio.Event()

    async def handler(_task):
        started.set()
        try:
            await release.wait()
        except asyncio.CancelledError:
            await release.wait()

    worker = WorkflowWorker(
        backend=dual_backend,
        handlers={"stage": handler},
        config=WorkflowWorkerConfig(
            worker_id="worker",
            poll_interval_seconds=0.001,
            cancel_cleanup_seconds=0.01,
            shutdown_grace_seconds=0,
            retry_jitter_seconds=0,
        ),
        now=lambda: NOW,
    )
    worker_task = asyncio.create_task(worker.run())
    await started.wait()
    await worker.force_recover_inflight(recovery_budget_seconds=0.1)
    terminal = await dual_backend.get_task("stage-shutdown-terminal")
    assert terminal is not None
    assert terminal.status == TaskStatus.FAILED.value
    assert terminal.error_code == "max_attempts_exceeded"
    release.set()
    worker.request_stop()
    await asyncio.wait_for(worker_task, timeout=1)


@pytest.mark.asyncio
async def test_finalization_failure_rolls_back_on_both_backends(dual_backend):
    await dual_backend.create_run(
        WorkflowDefinition(
            "dual-finalization-rollback",
            "rollback",
            (TaskSpec("stage", task_id="stage-rollback"),),
        ),
        now=NOW,
    )
    task = await _claim(dual_backend, "stage-rollback")

    async def failing_hook(_transaction):
        raise RuntimeError("injected finalization failure")

    with pytest.raises(RuntimeError, match="injected finalization failure"):
        await dual_backend.finalize_task(
            task=task,
            result=TaskResult(result_status=BusinessResultStatus.COMPLETE.value),
            now=NOW,
            business_hook=failing_hook,
        )
    stored = await dual_backend.get_task(task.task_id)
    assert stored is not None
    assert stored.status == TaskStatus.RUNNING.value
    assert await dual_backend.get_manifest(task.task_id) is None
    assert not any(
        event.event_type == "finalize"
        for event in await dual_backend.list_events(task_id=task.task_id)
    )


@pytest.mark.asyncio
async def test_handler_business_hook_payload_and_events_on_both_backends(dual_backend):
    await dual_backend.create_run(
        WorkflowDefinition(
            "dual-handler-hook",
            "handler-hook",
            (TaskSpec("stage", task_id="stage-handler-hook"),),
        ),
        now=NOW,
    )
    task = await _claim(dual_backend, "stage-handler-hook")
    hook_called = asyncio.Event()

    async def handler_hook(_transaction):
        hook_called.set()

    async def handler(_task):
        return TaskResult(
            result_status=BusinessResultStatus.COMPLETE.value,
            result_payload={"nested": {"value": 1}},
            business_hook=handler_hook,
        )

    worker = WorkflowWorker(
        backend=dual_backend,
        handlers={"stage": handler},
        config=WorkflowWorkerConfig(
            worker_id="worker", retry_backoff_base_seconds=0, retry_jitter_seconds=0
        ),
        now=lambda: NOW,
    )
    await worker._run_one(task)
    assert hook_called.is_set()
    stored_task = await dual_backend.get_task(task.task_id)
    assert stored_task is not None
    assert stored_task.status == TaskStatus.SUCCEEDED.value
    manifest = await dual_backend.get_manifest(task.task_id)
    assert manifest is not None
    assert manifest.result_payload["nested"]["value"] == 1
    event_types = [
        event.event_type for event in await dual_backend.list_events(task_id=task.task_id)
    ]
    assert event_types == ["created", "claim", "finalize"]


@pytest.mark.asyncio
async def test_heartbeat_failure_threshold_and_recovery_on_both_backends(
    dual_backend, monkeypatch: pytest.MonkeyPatch
):
    await dual_backend.create_run(
        WorkflowDefinition(
            "dual-heartbeat-failure",
            "heartbeat-failure",
            (TaskSpec("stage", task_id="stage-heartbeat-failure"),),
        ),
        now=NOW,
    )
    task = await _claim(dual_backend, "stage-heartbeat-failure")

    async def failed_heartbeat(**_kwargs):
        raise ConnectionError("heartbeat unavailable")

    monkeypatch.setattr(dual_backend, "heartbeat", failed_heartbeat)

    async def handler(_task):
        await asyncio.Event().wait()

    worker = WorkflowWorker(
        backend=dual_backend,
        handlers={"stage": handler},
        config=WorkflowWorkerConfig(
            worker_id="worker",
            lease_duration_seconds=0.03,
            heartbeat_failure_threshold=1,
            cancel_cleanup_seconds=0.01,
            retry_jitter_seconds=0,
        ),
        now=lambda: NOW,
    )
    await worker._run_one(task)
    still_running = await dual_backend.get_task(task.task_id)
    assert still_running is not None
    assert still_running.status == TaskStatus.RUNNING.value
    assert await dual_backend.recover_expired_running_tasks(now=NOW + timedelta(minutes=1)) == 1
    recovered = await dual_backend.get_task(task.task_id)
    assert recovered is not None
    assert recovered.status == TaskStatus.RETRY_WAITING.value


@pytest.mark.asyncio
async def test_worker_restart_recovers_and_completes_on_both_backends(dual_backend):
    await dual_backend.create_run(
        WorkflowDefinition(
            "dual-worker-restart",
            "worker-restart",
            (TaskSpec("stage", task_id="stage-worker-restart"),),
        ),
        now=NOW,
    )
    old_task = await _claim(dual_backend, "stage-worker-restart")
    assert await dual_backend.recover_expired_running_tasks(now=NOW + timedelta(minutes=1)) == 1
    assert await dual_backend.recover_retry_waiting_tasks(now=NOW + timedelta(minutes=1)) == 1
    restarted_task = await _claim(
        dual_backend,
        "stage-worker-restart",
        now=NOW + timedelta(minutes=1),
        worker_id="worker-restarted",
    )
    assert restarted_task.attempt == old_task.attempt + 1

    async def handler(_task):
        return TaskResult(result_status=BusinessResultStatus.COMPLETE.value)

    worker = WorkflowWorker(
        backend=dual_backend,
        handlers={"stage": handler},
        config=WorkflowWorkerConfig(worker_id="worker-restarted"),
        now=lambda: NOW + timedelta(minutes=1),
    )
    await worker._run_one(restarted_task)
    completed = await dual_backend.get_task(restarted_task.task_id)
    assert completed is not None
    assert completed.status == TaskStatus.SUCCEEDED.value


@pytest.mark.asyncio
async def test_full_retry_failure_state_machine_is_identical_on_both_backends(dual_backend):
    await dual_backend.create_run(
        WorkflowDefinition(
            "dual-state-machine",
            "retry-failure",
            (TaskSpec("stage", task_id="stage-state-machine", max_attempts=2),),
        ),
        now=NOW,
    )
    first = await _claim(dual_backend, "stage-state-machine")
    assert await dual_backend.retry_task(
        task_id=first.task_id,
        owner=first.owner or "",
        lock_token=first.lock_token or "",
        error_code="transient",
        error_message="retry",
        available_at=NOW,
        now=NOW,
    )
    assert await dual_backend.recover_retry_waiting_tasks(now=NOW) == 1
    second = await _claim(dual_backend, "stage-state-machine", worker_id="worker-b")
    assert second.attempt == 2
    assert await dual_backend.fail_task(
        task_id=second.task_id,
        owner=second.owner or "",
        lock_token=second.lock_token or "",
        error_code="fatal",
        error_message="terminal failure",
        now=NOW,
    )
    stored = await dual_backend.get_task(second.task_id)
    assert stored is not None and stored.status == TaskStatus.FAILED.value
    run = await dual_backend.get_run(second.run_id)
    assert run is not None and run.status == "failed"


@pytest.mark.asyncio
async def test_three_layer_dag_activation_is_identical_on_both_backends(dual_backend):
    await dual_backend.create_run(
        WorkflowDefinition(
            "dual-dag-depth",
            "three-layer",
            (
                TaskSpec("root", task_id="dag-root"),
                TaskSpec("middle", dependencies=(DependencySpec("root"),), task_id="dag-middle"),
                TaskSpec("leaf", dependencies=(DependencySpec("middle"),), task_id="dag-leaf"),
            ),
        ),
        now=NOW,
    )
    root = await _claim(dual_backend, "dag-root", stage="root")
    assert await dual_backend.finalize_task(
        task=root, result=TaskResult(result_status="complete"), now=NOW
    )
    assert await dual_backend.activate_ready_tasks(now=NOW) == 1
    middle = await _claim(dual_backend, "dag-middle", stage="middle")
    assert await dual_backend.finalize_task(
        task=middle, result=TaskResult(result_status="complete"), now=NOW
    )
    assert await dual_backend.activate_ready_tasks(now=NOW) == 1
    leaf = await _claim(dual_backend, "dag-leaf", stage="leaf")
    assert await dual_backend.finalize_task(
        task=leaf, result=TaskResult(result_status="complete"), now=NOW
    )
    run = await dual_backend.get_run(leaf.run_id)
    assert run is not None
    assert run.status == TaskStatus.SUCCEEDED.value
    assert run.business_result_status == BusinessResultStatus.COMPLETE.value


@pytest.mark.asyncio
async def test_partial_cancellation_is_identical_on_both_backends(dual_backend):
    await dual_backend.create_run(
        WorkflowDefinition(
            "dual-partial-cancel",
            "partial-cancel",
            (
                TaskSpec("keep", task_id="keep-task"),
                TaskSpec("cancel", task_id="cancel-task"),
            ),
        ),
        now=NOW,
    )
    assert await dual_backend.cancel_task(task_id="cancel-task", now=NOW)
    keep = await _claim(dual_backend, "keep-task", stage="keep")
    assert await dual_backend.finalize_task(
        task=keep, result=TaskResult(result_status="complete"), now=NOW
    )
    canceled = await dual_backend.get_task("cancel-task")
    completed = await dual_backend.get_task("keep-task")
    run = await dual_backend.get_run(keep.run_id)
    assert canceled is not None and canceled.status == TaskStatus.CANCELED.value
    assert completed is not None and completed.status == TaskStatus.SUCCEEDED.value
    assert run is not None and run.status == "canceled"


@pytest.mark.asyncio
async def test_partial_handler_failure_is_identical_on_both_backends(dual_backend):
    await dual_backend.create_run(
        WorkflowDefinition(
            "dual-partial-failure",
            "partial-failure",
            (
                TaskSpec("ok", task_id="ok-task"),
                TaskSpec("fatal", task_id="fatal-task"),
            ),
        ),
        now=NOW,
    )
    tasks = await dual_backend.claim_ready_tasks(
        worker_id="worker",
        stages=["ok", "fatal"],
        lease_until=NOW + timedelta(minutes=1),
        now=NOW,
        limit=2,
    )
    assert {task.stage for task in tasks} == {"ok", "fatal"}

    async def ok_handler(_task):
        return TaskResult(result_status=BusinessResultStatus.COMPLETE.value)

    async def fatal_handler(_task):
        raise NonRetryableWorkflowError("fatal", "injected handler failure")

    worker = WorkflowWorker(
        backend=dual_backend,
        handlers={"ok": ok_handler, "fatal": fatal_handler},
        config=WorkflowWorkerConfig(worker_id="worker", retry_jitter_seconds=0),
        now=lambda: NOW,
    )
    await asyncio.gather(*(worker._run_one(task) for task in tasks))
    ok = await dual_backend.get_task("ok-task")
    fatal = await dual_backend.get_task("fatal-task")
    run = await dual_backend.get_run(tasks[0].run_id)
    assert ok is not None and ok.status == TaskStatus.SUCCEEDED.value
    assert fatal is not None and fatal.status == TaskStatus.FAILED.value
    assert run is not None and run.status == "failed"
