"""Pure workflow state and dependency rules."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from enum import StrEnum

from piko.workflow.types import (
    DependencySpec,
    InvalidStateTransition,
    InvalidWorkflowDefinition,
    TaskStatus,
)


_ALLOWED_TRANSITIONS: Mapping[str, frozenset[str]] = {
    TaskStatus.PENDING.value: frozenset(
        {TaskStatus.READY.value, TaskStatus.BLOCKED.value, TaskStatus.CANCELED.value}
    ),
    TaskStatus.READY.value: frozenset(
        {TaskStatus.RUNNING.value, TaskStatus.FAILED.value, TaskStatus.CANCELED.value}
    ),
    TaskStatus.RUNNING.value: frozenset(
        {
            TaskStatus.SUCCEEDED.value,
            TaskStatus.FAILED.value,
            TaskStatus.RETRY_WAITING.value,
            TaskStatus.CANCELED.value,
        }
    ),
    TaskStatus.RETRY_WAITING.value: frozenset({TaskStatus.READY.value, TaskStatus.CANCELED.value}),
    TaskStatus.BLOCKED.value: frozenset({TaskStatus.READY.value, TaskStatus.CANCELED.value}),
    TaskStatus.SUCCEEDED.value: frozenset(),
    TaskStatus.FAILED.value: frozenset(),
    TaskStatus.CANCELED.value: frozenset(),
}


class DependencyDecision(StrEnum):
    WAIT = "wait"
    READY = "ready"
    BLOCK = "block"


def validate_transition(current: str, target: str) -> None:
    """Reject illegal transitions before any persistent mutation."""
    if target not in _ALLOWED_TRANSITIONS:
        raise InvalidStateTransition(f"unknown target state: {target}")
    if current not in _ALLOWED_TRANSITIONS or target not in _ALLOWED_TRANSITIONS[current]:
        raise InvalidStateTransition(f"illegal task transition: {current} -> {target}")


def validate_dag(
    stages: Iterable[str], dependencies: Mapping[str, Iterable[DependencySpec]]
) -> None:
    """Validate dependency existence, uniqueness, and cycles."""
    stage_list = tuple(stages)
    stage_set = set(stage_list)
    if len(stage_set) != len(stage_list):
        raise InvalidWorkflowDefinition("workflow stages must be unique")
    adjacency = _build_adjacency(stage_set, dependencies)
    visited: set[str] = set()
    for stage in stage_set:
        _visit_stage(stage, adjacency, set(), visited)


def _build_adjacency(
    stage_set: set[str], dependencies: Mapping[str, Iterable[DependencySpec]]
) -> dict[str, set[str]]:
    adjacency: dict[str, set[str]] = {stage: set() for stage in stage_set}
    for task_stage, edges in dependencies.items():
        if task_stage not in stage_set:
            raise InvalidWorkflowDefinition(f"dependency owner stage is missing: {task_stage}")
        for edge in edges:
            if edge.depends_on_stage not in stage_set:
                raise InvalidWorkflowDefinition(
                    f"dependency stage is missing: {edge.depends_on_stage}"
                )
            if edge.depends_on_stage == task_stage:
                raise InvalidWorkflowDefinition(f"self dependency: {task_stage}")
            if edge.depends_on_stage in adjacency[task_stage]:
                raise InvalidWorkflowDefinition(
                    f"duplicate dependency: {task_stage} <- {edge.depends_on_stage}"
                )
            adjacency[task_stage].add(edge.depends_on_stage)
    return adjacency


def _visit_stage(
    stage: str,
    adjacency: Mapping[str, set[str]],
    visiting: set[str],
    visited: set[str],
) -> None:
    if stage in visiting:
        raise InvalidWorkflowDefinition("workflow dependency graph contains a cycle")
    if stage in visited:
        return
    visiting.add(stage)
    for upstream in adjacency[stage]:
        _visit_stage(upstream, adjacency, visiting, visited)
    visiting.remove(stage)
    visited.add(stage)


def dependency_decision(
    edges: Iterable[DependencySpec],
    upstream: Mapping[str, tuple[str, str | None]],
) -> DependencyDecision:
    """Return explicit wait/ready/block semantics for all upstream tasks.

    ``upstream`` maps stage to ``(technical_status, business_result_status)``.
    A failed/canceled/blocked upstream or an otherwise disallowed business result
    blocks the downstream task instead of leaving it pending forever.
    """
    edge_list = tuple(edges)
    if not edge_list:
        return DependencyDecision.READY
    for edge in edge_list:
        state = upstream.get(edge.depends_on_stage)
        if state is None:
            raise InvalidWorkflowDefinition(f"dependency row is missing: {edge.depends_on_stage}")
        technical, business = state
        if technical in {
            TaskStatus.FAILED.value,
            TaskStatus.BLOCKED.value,
            TaskStatus.CANCELED.value,
        }:
            return DependencyDecision.BLOCK
        if technical not in edge.allowed_technical_statuses:
            return DependencyDecision.WAIT
        if business not in edge.allowed_business_statuses:
            return DependencyDecision.BLOCK
    return DependencyDecision.READY
