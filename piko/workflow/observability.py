"""Workflow metrics and safe lifecycle logging helpers."""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

from prometheus_client import Counter, Gauge, Histogram

from piko.infra.logging import get_logger

logger = get_logger(__name__)

WORKFLOW_EVENT_TOTAL = Counter(
    "piko_workflow_event_total",
    "Workflow lifecycle events",
    ["workflow_id", "stage", "event_type"],
)
WORKFLOW_TASK_TOTAL = Counter(
    "piko_workflow_task_total",
    "Workflow task outcomes",
    ["workflow_id", "stage", "technical_status", "business_result_status"],
)
WORKFLOW_TASK_DURATION_SECONDS = Histogram(
    "piko_workflow_task_duration_seconds",
    "Workflow task execution duration",
    ["workflow_id", "stage"],
)
WORKFLOW_TASKS_INFLIGHT = Gauge(
    "piko_workflow_tasks_inflight",
    "Currently running workflow tasks",
    ["worker_id"],
)

_SECRET_PATTERN = re.compile(r"(?i)(password|passwd|token|secret|dsn)=[^\s,;]+")


def safe_error_message(message: str, *, limit: int = 512) -> str:
    """Keep error location/code useful while removing credentials and payload noise."""
    sanitized = _SECRET_PATTERN.sub(r"\1=[REDACTED]", message)
    return sanitized[:limit]


def short_lock_token(token: str | None) -> str:
    """Return a diagnostic prefix, never the full fencing token."""
    return f"{token[:8]}..." if token else ""


def safe_log_fields(fields: Mapping[str, Any]) -> dict[str, Any]:
    """Redact common credential/token fields before structured logging."""
    result: dict[str, Any] = {}
    for key, value in fields.items():
        lowered = key.lower()
        if lowered in {"password", "passwd", "secret", "dsn", "connection_string"}:
            result[key] = "[REDACTED]"
        elif lowered in {"token", "lock_token", "owner_token"}:
            result[key] = short_lock_token(str(value))
        else:
            result[key] = value
    return result


def record_event(*, workflow_id: str, stage: str, event_type: str) -> None:
    WORKFLOW_EVENT_TOTAL.labels(
        workflow_id=workflow_id,
        stage=stage,
        event_type=event_type,
    ).inc()


def record_outcome(
    *,
    workflow_id: str,
    stage: str,
    technical_status: str,
    business_result_status: str,
) -> None:
    WORKFLOW_TASK_TOTAL.labels(
        workflow_id=workflow_id,
        stage=stage,
        technical_status=technical_status,
        business_result_status=business_result_status,
    ).inc()
