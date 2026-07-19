"""Business-neutral workflow control-plane data contracts."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from collections.abc import Awaitable, Callable, Sequence
from typing import Any, Mapping


def _empty_mapping() -> dict[str, Any]:
    return {}


BusinessHook = Callable[[Any], Awaitable[None]]


class TaskStatus(StrEnum):
    PENDING = "pending"
    READY = "ready"
    RUNNING = "running"
    RETRY_WAITING = "retry_waiting"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    BLOCKED = "blocked"
    CANCELED = "canceled"


class RunStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELED = "canceled"


class BusinessResultStatus(StrEnum):
    UNKNOWN = "unknown"
    COMPLETE = "complete"
    PARTIAL = "partial"
    EMPTY = "empty"
    UNAVAILABLE = "unavailable"
    FAILED = "failed"


def aggregate_business_result_status(task_count: int, result_statuses: Sequence[str]) -> str:
    """Aggregate task business results without discarding valid terminal values."""
    statuses = set(result_statuses)
    for status in (
        BusinessResultStatus.FAILED.value,
        BusinessResultStatus.UNAVAILABLE.value,
        BusinessResultStatus.PARTIAL.value,
        BusinessResultStatus.EMPTY.value,
    ):
        if status in statuses:
            return status
    if task_count and len(result_statuses) == task_count:
        if statuses == {BusinessResultStatus.COMPLETE.value}:
            return BusinessResultStatus.COMPLETE.value
    return BusinessResultStatus.UNKNOWN.value


class WorkflowEventType(StrEnum):
    CLAIM = "claim"
    HEARTBEAT = "heartbeat"
    RETRY = "retry"
    RECOVERY = "recovery"
    FINALIZE = "finalize"
    BLOCKED = "blocked"
    CANCELED = "canceled"
    OWNERSHIP_LOST = "ownership_lost"
    SUCCESS = "success"
    FAILURE = "failure"
    TIMEOUT = "timeout"
    LEASE_EXPIRED = "lease_expired"


@dataclass(frozen=True, slots=True)
class DependencySpec:
    """A same-run edge and its explicit activation rule."""

    depends_on_stage: str
    allowed_business_statuses: tuple[str, ...] = (BusinessResultStatus.COMPLETE.value,)
    allowed_technical_statuses: tuple[str, ...] = (TaskStatus.SUCCEEDED.value,)

    def __post_init__(self) -> None:
        if not self.depends_on_stage.strip():
            raise ValueError("depends_on_stage must not be blank")
        if not self.allowed_business_statuses:
            raise ValueError("allowed_business_statuses must not be empty")
        if not self.allowed_technical_statuses:
            raise ValueError("allowed_technical_statuses must not be empty")

    def as_json(self) -> dict[str, Any]:
        return {
            "allowed_business_statuses": list(self.allowed_business_statuses),
            "allowed_technical_statuses": list(self.allowed_technical_statuses),
        }


@dataclass(frozen=True, slots=True)
class TaskSpec:
    stage: str
    dependencies: tuple[DependencySpec, ...] = ()
    max_attempts: int = 3
    idempotency_key: str | None = None
    task_id: str | None = None

    def __post_init__(self) -> None:
        if not self.stage.strip():
            raise ValueError("stage must not be blank")
        if self.max_attempts <= 0:
            raise ValueError("max_attempts must be greater than zero")


@dataclass(frozen=True, slots=True)
class WorkflowDefinition:
    workflow_id: str
    idempotency_key: str
    tasks: tuple[TaskSpec, ...]
    config_snapshot: Mapping[str, Any] = field(default_factory=_empty_mapping)
    rerun: bool = False

    def __post_init__(self) -> None:
        if not self.workflow_id.strip():
            raise ValueError("workflow_id must not be blank")
        if not self.idempotency_key.strip():
            raise ValueError("idempotency_key must not be blank")
        if not self.tasks:
            raise ValueError("workflow must contain at least one task")


@dataclass(frozen=True, slots=True)
class WorkflowRunRecord:
    run_id: str
    workflow_id: str
    idempotency_key: str
    status: str
    business_result_status: str
    config_digest: str
    config_snapshot: Mapping[str, Any]
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class WorkflowTaskRecord:
    task_id: str
    run_id: str
    workflow_id: str
    stage: str
    status: str
    attempt: int
    max_attempts: int
    available_at: datetime | None
    owner: str | None
    lock_token: str | None
    lease_until: datetime | None
    heartbeat_at: datetime | None
    idempotency_key: str
    error_code: str | None
    error_message: str | None
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class TaskResult:
    """Result committed with a technical success.

    ``result_status`` is intentionally separate from ``TaskStatus.SUCCEEDED``.
    A task can finish its technical work while its business result remains partial
    or unavailable.
    """

    result_status: str = BusinessResultStatus.UNKNOWN.value
    result_payload: Mapping[str, Any] = field(default_factory=_empty_mapping)
    output_digest: str | None = None
    business_hook: BusinessHook | None = field(default=None, repr=False, compare=False)


@dataclass(frozen=True, slots=True)
class WorkflowTaskEvent:
    event_id: int | str
    task_id: str
    run_id: str
    stage: str
    event_type: str
    payload: Mapping[str, Any]
    created_at: datetime


@dataclass(frozen=True, slots=True)
class WorkflowManifest:
    task_id: str
    run_id: str
    idempotency_key: str
    result_status: str
    result_payload: Mapping[str, Any]
    output_digest: str | None
    created_at: datetime
    updated_at: datetime


class WorkflowError(RuntimeError):
    """Base class for control-plane errors."""


class InvalidStateTransition(WorkflowError):
    pass


class InvalidWorkflowDefinition(WorkflowError):
    pass


class OwnershipLostError(WorkflowError):
    """The caller no longer owns the task fencing token."""


class IdempotencyConflictError(WorkflowError):
    pass
