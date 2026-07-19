from __future__ import annotations

import pytest

from tests.workflow.conftest import claim_time
from piko.workflow.state_machine import validate_dag
from piko.workflow.types import DependencySpec, InvalidWorkflowDefinition, WorkflowDefinition


@pytest.mark.asyncio
async def test_run_creation_is_idempotent_and_explicit_rerun_gets_new_run(
    memory_backend, simple_definition, NOW
):
    first = await memory_backend.create_run(simple_definition, now=NOW)
    same = await memory_backend.create_run(simple_definition, now=NOW)
    rerun = await memory_backend.create_run(
        WorkflowDefinition(
            workflow_id=simple_definition.workflow_id,
            idempotency_key=simple_definition.idempotency_key,
            tasks=simple_definition.tasks,
            config_snapshot=simple_definition.config_snapshot,
            rerun=True,
        ),
        now=NOW,
    )
    assert first.run_id == same.run_id
    assert rerun.run_id != first.run_id
    assert {task.run_id for task in memory_backend.tasks.values()} == {first.run_id, rerun.run_id}


def test_missing_dependency_and_cycle_are_rejected():
    with pytest.raises(InvalidWorkflowDefinition):
        validate_dag(["a"], {"a": (DependencySpec("missing"),)})
    with pytest.raises(InvalidWorkflowDefinition):
        validate_dag(
            ["a", "b"],
            {"a": (DependencySpec("b"),), "b": (DependencySpec("a"),)},
        )


@pytest.mark.asyncio
async def test_tasks_are_scoped_to_their_run(memory_backend, simple_definition, NOW):
    first = await memory_backend.create_run(simple_definition, now=NOW)
    second = await memory_backend.create_run(
        WorkflowDefinition(
            workflow_id=simple_definition.workflow_id,
            idempotency_key="batch-2",
            tasks=simple_definition.tasks,
        ),
        now=NOW,
    )
    source = [
        task
        for task in memory_backend.tasks.values()
        if task.run_id == first.run_id and task.stage == "source"
    ][0]
    claimed = await memory_backend.claim_ready_tasks(
        worker_id="worker-1",
        stages=["source"],
        lease_until=claim_time(),
        now=NOW,
        limit=1,
    )
    assert claimed[0].run_id in {first.run_id, second.run_id}
    assert source.task_id != next(
        task.task_id
        for task in memory_backend.tasks.values()
        if task.run_id == second.run_id and task.stage == "source"
    )
