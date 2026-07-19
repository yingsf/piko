"""Verify PikoApp owns the durable workflow worker lifecycle."""

import asyncio
from datetime import datetime

import pytest

import piko.app as app_module
from piko import PikoApp
from piko.workflow.repository import InMemoryWorkflowRepository
from piko.workflow.types import BusinessResultStatus, TaskResult, TaskSpec, WorkflowDefinition
from piko.workflow.worker import WorkflowWorkerConfig


NOW = datetime(2026, 1, 1)


@pytest.mark.asyncio
async def test_app_starts_registered_workflow_handler_and_stops_worker() -> None:
    app = PikoApp(name="workflow-app-test")
    backend = InMemoryWorkflowRepository()
    app.workflow_repository = backend
    completed = asyncio.Event()

    @app.workflow("stage")
    async def handle_stage(_task):
        completed.set()
        return TaskResult(result_status=BusinessResultStatus.COMPLETE.value)

    definition = WorkflowDefinition(
        workflow_id="app-lifecycle",
        idempotency_key="app-lifecycle",
        tasks=(TaskSpec(stage="stage", task_id="app-lifecycle-stage"),),
    )
    await backend.create_run(definition, now=NOW)
    await app.start_workflow_worker(
        WorkflowWorkerConfig(
            worker_id="workflow-app-test",
            poll_interval_seconds=0.01,
            task_timeout_seconds=1,
            retry_backoff_base_seconds=0,
            retry_jitter_seconds=0,
        )
    )
    try:
        await asyncio.wait_for(completed.wait(), timeout=1)
        task = None
        for _ in range(100):
            task = await backend.get_task("app-lifecycle-stage")
            if task is not None and task.status == "succeeded":
                break
            await asyncio.sleep(0.01)
        assert task is not None
        assert task.status == "succeeded"
    finally:
        await app.stop_workflow_worker()

    assert app._workflow_worker_task is None


@pytest.mark.asyncio
async def test_app_control_workflow_task_delegates_audited_action() -> None:
    app = PikoApp(name="workflow-control-test")
    backend = InMemoryWorkflowRepository()
    app.workflow_repository = backend
    definition = WorkflowDefinition(
        workflow_id="app-control",
        idempotency_key="app-control",
        tasks=(TaskSpec(stage="stage", task_id="app-control-stage"),),
    )
    run = await backend.create_run(definition, now=NOW)

    result = await app.control_workflow_task(
        run_id=run.run_id,
        stage="stage",
        action="cancel",
        reason_digest="operator-request",
    )

    assert result is not None
    assert result["status"] == "canceled"


@pytest.mark.asyncio
async def test_app_bounds_worker_shutdown_inside_total_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """验证 Worker 的宽限期和清理期不会超过应用总停机预算。"""
    monkeypatch.setattr(app_module.settings, "shutdown_timeout_s", 30, raising=False)
    app = PikoApp(name="workflow-shutdown-budget-test")
    app.workflow_repository = InMemoryWorkflowRepository()

    worker = await app.start_workflow_worker(
        WorkflowWorkerConfig(
            worker_id="workflow-shutdown-budget-test",
            shutdown_grace_seconds=300,
            cancel_cleanup_seconds=5,
        )
    )
    assert worker.config.shutdown_grace_seconds + worker.config.cancel_cleanup_seconds < 15
    await app.stop_workflow_worker()
