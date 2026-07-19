from __future__ import annotations

import pytest

from tests.workflow.conftest import claim_time
from piko.workflow.lifecycle import WorkflowFinalizer
from piko.workflow.types import (
    DependencySpec,
    IdempotencyConflictError,
    TaskResult,
    TaskSpec,
    TaskStatus,
    WorkflowDefinition,
)


@pytest.mark.asyncio
async def test_finalize_retry_after_success_is_idempotent(memory_backend, simple_definition, NOW):
    await memory_backend.create_run(simple_definition, now=NOW)
    task = (
        await memory_backend.claim_ready_tasks(
            worker_id="w", stages=["source"], lease_until=claim_time(), now=NOW, limit=1
        )
    )[0]
    result = TaskResult(result_status="complete", result_payload={"key": "value"})
    assert await memory_backend.finalize_task(task=task, result=result, now=NOW)
    assert not await memory_backend.finalize_task(task=task, result=result, now=NOW)
    manifests = [
        manifest
        for manifest in memory_backend.manifests.values()
        if manifest.task_id == task.task_id
    ]
    assert len(manifests) == 1


@pytest.mark.asyncio
async def test_rerun_keeps_original_idempotency_mapping_and_json_snapshot_isolated(
    memory_backend, NOW
):
    config = {"nested": {"value": 1}}
    definition = WorkflowDefinition(
        "rerun-workflow",
        "batch-1",
        (TaskSpec("stage"),),
        config_snapshot=config,
    )
    original = await memory_backend.create_run(definition, now=NOW)
    rerun = await memory_backend.create_run(
        WorkflowDefinition(
            "rerun-workflow",
            "batch-1",
            (TaskSpec("stage"),),
            config_snapshot=config,
            rerun=True,
        ),
        now=NOW,
    )
    normal = await memory_backend.create_run(definition, now=NOW)

    assert rerun.run_id != original.run_id
    assert normal.run_id == original.run_id
    config["nested"]["value"] = 99
    assert original.config_snapshot["nested"]["value"] == 1
    assert memory_backend.runs[original.run_id].config_snapshot["nested"]["value"] == 1


@pytest.mark.asyncio
async def test_create_run_rolls_back_after_task_creation_failure(memory_backend, NOW):
    """验证任务创建失败时不会遗留不完整的 run 或幂等映射"""
    definition = WorkflowDefinition(
        "create-rollback",
        "batch-1",
        (
            TaskSpec("first", task_id="first-task", idempotency_key="shared-task"),
            TaskSpec("second", task_id="second-task", idempotency_key="shared-task"),
        ),
    )

    with pytest.raises(IdempotencyConflictError):
        await memory_backend.create_run(definition, now=NOW)

    assert memory_backend.runs == {}
    assert memory_backend.tasks == {}
    assert memory_backend.dependencies == {}
    assert memory_backend.events == []
    assert memory_backend._run_by_idempotency == {}


@pytest.mark.asyncio
async def test_nested_manifest_payload_is_isolated_from_caller_mutation(
    memory_backend, simple_definition, NOW
):
    await memory_backend.create_run(simple_definition, now=NOW)
    task = (
        await memory_backend.claim_ready_tasks(
            worker_id="w", stages=["source"], lease_until=claim_time(), now=NOW, limit=1
        )
    )[0]
    payload = {"nested": {"value": 1}}
    assert await memory_backend.finalize_task(
        task=task,
        result=TaskResult(result_status="complete", result_payload=payload),
        now=NOW,
    )
    payload["nested"]["value"] = 99

    manifest = await memory_backend.get_manifest(task.task_id)
    assert manifest is not None
    assert manifest.result_payload["nested"]["value"] == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "state", [TaskStatus.PENDING.value, TaskStatus.READY.value, TaskStatus.RETRY_WAITING.value]
)
async def test_finalizer_can_cancel_unclaimed_task_states(memory_backend, NOW, state):
    if state == TaskStatus.PENDING.value:
        definition = WorkflowDefinition(
            "cancel-workflow",
            "pending",
            (TaskSpec("root"), TaskSpec("child", (DependencySpec("root"),))),
        )
        await memory_backend.create_run(definition, now=NOW)
        task = next(item for item in memory_backend.tasks.values() if item.stage == "child")
    else:
        await memory_backend.create_run(
            WorkflowDefinition("cancel-workflow", state, (TaskSpec("stage"),)), now=NOW
        )
        task = await memory_backend.get_task(next(iter(memory_backend.tasks)))
        assert task is not None
        if state == TaskStatus.RETRY_WAITING.value:
            claimed = (
                await memory_backend.claim_ready_tasks(
                    worker_id="w", stages=["stage"], lease_until=claim_time(), now=NOW, limit=1
                )
            )[0]
            await memory_backend.retry_task(
                task_id=claimed.task_id,
                owner="w",
                lock_token=claimed.lock_token or "",
                error_code="retry",
                error_message="retry",
                available_at=claim_time(60),
                now=NOW,
            )
            task = await memory_backend.get_task(claimed.task_id)
            assert task is not None

    finalizer = WorkflowFinalizer(memory_backend, now=lambda: NOW)
    assert task.status == state
    assert await finalizer.cancel(task)
    assert (await memory_backend.get_task(task.task_id)).status == TaskStatus.CANCELED.value
