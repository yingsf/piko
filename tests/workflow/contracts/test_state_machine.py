from __future__ import annotations

import pytest

from piko.workflow.state_machine import DependencyDecision, dependency_decision, validate_transition
from piko.workflow.types import DependencySpec, InvalidStateTransition, TaskStatus


def test_terminal_and_illegal_transitions_are_rejected():
    validate_transition(TaskStatus.PENDING.value, TaskStatus.READY.value)
    validate_transition(TaskStatus.RUNNING.value, TaskStatus.RETRY_WAITING.value)
    with pytest.raises(InvalidStateTransition):
        validate_transition(TaskStatus.SUCCEEDED.value, TaskStatus.RUNNING.value)
    with pytest.raises(InvalidStateTransition):
        validate_transition(TaskStatus.PENDING.value, TaskStatus.RUNNING.value)


def test_dependency_decision_requires_all_upstreams_and_business_rule():
    edges = (DependencySpec("a"), DependencySpec("b"))
    assert (
        dependency_decision(edges, {"a": ("succeeded", "complete"), "b": ("running", None)})
        == DependencyDecision.WAIT
    )
    assert (
        dependency_decision(edges, {"a": ("succeeded", "complete"), "b": ("failed", None)})
        == DependencyDecision.BLOCK
    )
    assert (
        dependency_decision(edges, {"a": ("succeeded", "partial"), "b": ("succeeded", "complete")})
        == DependencyDecision.BLOCK
    )


def test_partial_and_unavailable_can_be_explicitly_allowed():
    edges = (
        DependencySpec("partial", allowed_business_statuses=("partial",)),
        DependencySpec("unavailable", allowed_business_statuses=("unavailable",)),
    )
    assert (
        dependency_decision(
            edges,
            {
                "partial": ("succeeded", "partial"),
                "unavailable": ("succeeded", "unavailable"),
            },
        )
        == DependencyDecision.READY
    )
