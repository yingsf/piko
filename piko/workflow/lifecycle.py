"""Explicit finalization facade for handlers and application code."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import datetime
from typing import Any

from piko.workflow.repository import BusinessHook, WorkflowControlBackend
from piko.workflow.types import TaskResult, WorkflowTaskRecord


class WorkflowFinalizer:
    """Keep business writes and control-plane finalization on one backend boundary."""

    def __init__(
        self,
        backend: WorkflowControlBackend,
        *,
        now: Callable[[], datetime],
    ) -> None:
        self.backend = backend
        self._now = now

    async def finalize(
        self,
        task: WorkflowTaskRecord,
        *,
        result_status: str,
        result_payload: Mapping[str, Any] | None = None,
        output_digest: str | None = None,
        business_hook: BusinessHook | None = None,
    ) -> bool:
        return await self.backend.finalize_task(
            task=task,
            result=TaskResult(
                result_status=result_status,
                result_payload=dict(result_payload or {}),
                output_digest=output_digest,
            ),
            now=self._now(),
            business_hook=business_hook,
        )

    async def retry(
        self,
        task: WorkflowTaskRecord,
        *,
        error_code: str,
        error_message: str,
        available_at: datetime,
    ) -> bool:
        return await self.backend.retry_task(
            task_id=task.task_id,
            owner=task.owner or "",
            lock_token=task.lock_token or "",
            error_code=error_code,
            error_message=error_message,
            available_at=available_at,
            now=self._now(),
        )

    async def fail(
        self,
        task: WorkflowTaskRecord,
        *,
        error_code: str,
        error_message: str,
    ) -> bool:
        return await self.backend.fail_task(
            task_id=task.task_id,
            owner=task.owner or "",
            lock_token=task.lock_token or "",
            error_code=error_code,
            error_message=error_message,
            now=self._now(),
        )

    async def cancel(self, task: WorkflowTaskRecord) -> bool:
        return await self.backend.cancel_task(
            task_id=task.task_id,
            owner=task.owner,
            lock_token=task.lock_token,
            now=self._now(),
        )
