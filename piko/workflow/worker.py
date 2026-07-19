"""Recoverable workflow worker with bounded shutdown and fencing-aware writes."""

from __future__ import annotations

import asyncio
import contextlib
import math
import secrets
import time
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Protocol

from piko.workflow.observability import (
    WORKFLOW_TASK_DURATION_SECONDS,
    WORKFLOW_TASKS_INFLIGHT,
    record_event,
    record_outcome,
    safe_error_message,
    short_lock_token,
)
from piko.workflow.repository import WorkflowControlBackend
from piko.workflow.types import (
    BusinessResultStatus,
    OwnershipLostError,
    TaskResult,
    TaskStatus,
    WorkflowEventType,
    WorkflowTaskRecord,
)

_JITTER_SOURCE = secrets.SystemRandom()


class WorkflowHandler(Protocol):
    async def __call__(self, task: WorkflowTaskRecord, /) -> TaskResult | None: ...


class NonRetryableWorkflowError(Exception):
    def __init__(self, error_code: str, message: str = "non-retryable workflow error") -> None:
        if not error_code.strip():
            raise ValueError("error_code must not be blank")
        self.error_code = error_code
        self.safe_message = safe_error_message(message)
        super().__init__(self.safe_message)


@dataclass(frozen=True, slots=True)
class WorkflowWorkerConfig:
    worker_id: str
    poll_interval_seconds: float = 1.0
    concurrency: int = 4
    lease_duration_seconds: float = 60.0
    task_timeout_seconds: float = 7200.0
    cancel_cleanup_seconds: float = 5.0
    shutdown_grace_seconds: float = 20.0
    heartbeat_failure_threshold: int = 3
    retry_backoff_base_seconds: float = 1.0
    retry_jitter_seconds: float = 0.5

    def __post_init__(self) -> None:
        if not self.worker_id.strip() or self.concurrency <= 0:
            raise ValueError("worker_id and concurrency must be valid")
        if self.heartbeat_failure_threshold <= 0:
            raise ValueError("heartbeat_failure_threshold must be greater than zero")
        for name, value in (
            ("poll_interval_seconds", self.poll_interval_seconds),
            ("lease_duration_seconds", self.lease_duration_seconds),
            ("task_timeout_seconds", self.task_timeout_seconds),
        ):
            if not math.isfinite(value) or value <= 0:
                raise ValueError(f"{name} must be finite and greater than zero")
        for name, value in (
            ("cancel_cleanup_seconds", self.cancel_cleanup_seconds),
            ("shutdown_grace_seconds", self.shutdown_grace_seconds),
            ("retry_backoff_base_seconds", self.retry_backoff_base_seconds),
            ("retry_jitter_seconds", self.retry_jitter_seconds),
        ):
            if not math.isfinite(value) or value < 0:
                raise ValueError(f"{name} must be finite and non-negative")


@dataclass(frozen=True, slots=True)
class _InFlight:
    task: WorkflowTaskRecord
    run_task: asyncio.Task[None]


class WorkflowWorker:
    def __init__(
        self,
        *,
        backend: WorkflowControlBackend,
        handlers: Mapping[str, WorkflowHandler],
        config: WorkflowWorkerConfig,
        now: Callable[[], datetime],
    ) -> None:
        self.backend = backend
        self.handlers: dict[str, WorkflowHandler] = dict(handlers)
        self.config = config
        self._now = now
        self._stopping = asyncio.Event()
        self._inflight: dict[str, _InFlight] = {}
        self._orphaned: set[asyncio.Task[Any]] = set()

    def registered_stages(self) -> tuple[str, ...]:
        return tuple(self.handlers)

    def register_handler(self, stage: str, handler: WorkflowHandler) -> None:
        self.handlers[stage] = handler

    def request_stop(self) -> None:
        self._stopping.set()

    async def force_recover_inflight(self, recovery_budget_seconds: float | None = None) -> None:
        """Cancel in-flight work and fence any task still marked as running."""
        deadline = (
            None
            if recovery_budget_seconds is None
            else asyncio.get_running_loop().time() + recovery_budget_seconds
        )
        in_flight = tuple(self._inflight.values())
        await self._cancel_inflight(in_flight, deadline)
        for item in in_flight:
            if not await self._recover_inflight_item(item, deadline):
                break

    async def _cancel_inflight(
        self, in_flight: tuple[_InFlight, ...], deadline: float | None
    ) -> None:
        run_tasks = [item.run_task for item in in_flight if not item.run_task.done()]
        for run_task in run_tasks:
            run_task.cancel()
        if not run_tasks:
            return
        cleanup_timeout = self.config.cancel_cleanup_seconds
        if deadline is not None:
            remaining = self._remaining(deadline)
            if remaining is not None:
                cleanup_timeout = max(0.0, min(cleanup_timeout, remaining))
        _, pending = await asyncio.wait(run_tasks, timeout=cleanup_timeout)
        for run_task in pending:
            self._orphaned.add(run_task)
            run_task.add_done_callback(self._orphan_done)

    async def _recover_inflight_item(self, item: _InFlight, deadline: float | None) -> bool:
        remaining = self._remaining(deadline)
        if remaining is not None and remaining <= 0:
            return False
        try:
            current = await self._with_timeout(self.backend.get_task(item.task.task_id), remaining)
        except asyncio.TimeoutError:
            return False
        except Exception as error:  # noqa: BLE001
            self._log_task_error(item.task, error)
            return True
        if current is None or current.status != TaskStatus.RUNNING.value:
            return True
        remaining = self._remaining(deadline)
        if remaining is not None and remaining <= 0:
            return False
        try:
            await self._with_timeout(
                self._retry(
                    item.task,
                    "shutdown_timeout",
                    f"stage={item.task.stage} task_id={item.task.task_id} shutdown timeout",
                ),
                remaining,
            )
        except asyncio.TimeoutError:
            return False
        return True

    @staticmethod
    def _remaining(deadline: float | None) -> float | None:
        if deadline is None:
            return None
        return deadline - asyncio.get_running_loop().time()

    @staticmethod
    async def _with_timeout(operation: Awaitable[Any], budget_seconds: float | None) -> Any:
        async with asyncio.timeout(budget_seconds):
            return await operation

    async def run(self) -> None:
        try:
            while not self._stopping.is_set():
                try:
                    await self._loop_once()
                except asyncio.CancelledError:
                    raise
                except Exception as error:  # noqa: BLE001
                    record_event(workflow_id="*", stage="*", event_type="control_loop_error")
                    self._log_control_error(error)
                    await self._sleep_or_stop()
        finally:
            await self._shutdown_inflight()

    async def _loop_once(self) -> None:
        if self._stopping.is_set():
            return
        now = self._now()
        await self.backend.recover_expired_running_tasks(now=now)
        await self.backend.recover_retry_waiting_tasks(now=now)
        stages = self.registered_stages()
        if not stages:
            await self._sleep_or_stop()
            return
        await self.backend.activate_ready_tasks(now=self._now())
        if self._stopping.is_set():
            return
        available = self.config.concurrency - len(self._inflight)
        if available <= 0:
            await self._wait_for_one()
            return
        claim_now = self._now()
        tasks = await self.backend.claim_ready_tasks(
            worker_id=self.config.worker_id,
            stages=stages,
            lease_until=claim_now + timedelta(seconds=self.config.lease_duration_seconds),
            now=claim_now,
            limit=available,
        )
        for task in tasks:
            if self._stopping.is_set():
                break
            run_task = asyncio.create_task(self._run_one_safe(task))
            self._inflight[task.task_id] = _InFlight(task=task, run_task=run_task)
            WORKFLOW_TASKS_INFLIGHT.labels(worker_id=self.config.worker_id).set(len(self._inflight))
        if tasks:
            await self._wait_for_one()
        else:
            await self._sleep_or_stop()

    async def _run_one_safe(self, task: WorkflowTaskRecord) -> None:
        started = time.monotonic()
        try:
            await self._run_one(task)
        except asyncio.CancelledError:
            raise
        except Exception as error:  # noqa: BLE001
            self._log_task_error(task, error)
        finally:
            WORKFLOW_TASK_DURATION_SECONDS.labels(
                workflow_id=task.workflow_id, stage=task.stage
            ).observe(time.monotonic() - started)
            self._inflight.pop(task.task_id, None)
            WORKFLOW_TASKS_INFLIGHT.labels(worker_id=self.config.worker_id).set(len(self._inflight))

    async def _handle_completed_handler(
        self, task: WorkflowTaskRecord, handler_task: asyncio.Task[TaskResult | None]
    ) -> None:
        try:
            result = handler_task.result()
        except NonRetryableWorkflowError as error:
            await self._fail(task, error.error_code, error.safe_message)
            return
        except Exception as error:  # noqa: BLE001
            await self._retry(task, "handler_exception", safe_error_message(str(error)))
            return
        result = result or TaskResult(result_status=BusinessResultStatus.UNKNOWN.value)
        try:
            finalized = await self.backend.finalize_task(
                task=task,
                result=result,
                now=self._now(),
                business_hook=result.business_hook,
            )
        except OwnershipLostError:
            record_event(
                workflow_id=task.workflow_id,
                stage=task.stage,
                event_type=WorkflowEventType.OWNERSHIP_LOST.value,
            )
            return
        if finalized:
            record_outcome(
                workflow_id=task.workflow_id,
                stage=task.stage,
                technical_status="succeeded",
                business_result_status=result.result_status,
            )

    async def _run_one(self, task: WorkflowTaskRecord) -> None:
        handler = self.handlers.get(task.stage)
        if handler is None:
            await self._retry(task, "handler_missing", f"stage={task.stage} handler_missing")
            return
        heartbeat = asyncio.create_task(self._heartbeat_loop(task))
        handler_task = asyncio.create_task(handler(task))
        try:
            done, _ = await asyncio.wait(
                {handler_task, heartbeat},
                timeout=self.config.task_timeout_seconds,
                return_when=asyncio.FIRST_COMPLETED,
            )
            if not done:
                record_event(
                    workflow_id=task.workflow_id,
                    stage=task.stage,
                    event_type=WorkflowEventType.TIMEOUT.value,
                )
                await self._cancel_handler(handler_task, task, "handler_timeout")
                return
            if heartbeat in done and heartbeat.exception() is not None:
                await self._cancel_handler(handler_task, task, "ownership_lost", write_state=False)
                return
            if handler_task in done:
                await self._handle_completed_handler(task, handler_task)
        except asyncio.CancelledError:
            await self._cancel_handler(
                handler_task,
                task,
                "shutdown_canceled",
                write_state=False,
            )
            await self._retry(task, "shutdown_canceled", "worker shutdown canceled handler")
            raise
        finally:
            if not heartbeat.done():
                heartbeat.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await heartbeat

    async def _heartbeat_loop(self, task: WorkflowTaskRecord) -> None:
        interval = min(
            self.config.lease_duration_seconds / 3,
            max(0.01, self.config.lease_duration_seconds / 2),
        )
        failures = 0
        while True:
            await asyncio.sleep(interval)
            try:
                alive = await self.backend.heartbeat(
                    task_id=task.task_id,
                    owner=self.config.worker_id,
                    lock_token=task.lock_token or "",
                    lease_until=self._now() + timedelta(seconds=self.config.lease_duration_seconds),
                    now=self._now(),
                )
                if not alive:
                    record_event(
                        workflow_id=task.workflow_id,
                        stage=task.stage,
                        event_type=WorkflowEventType.OWNERSHIP_LOST.value,
                    )
                    raise OwnershipLostError(
                        f"ownership lost task_id={task.task_id} token={short_lock_token(task.lock_token)}"
                    )
                failures = 0
                record_event(
                    workflow_id=task.workflow_id,
                    stage=task.stage,
                    event_type=WorkflowEventType.HEARTBEAT.value,
                )
            except asyncio.CancelledError:
                raise
            except OwnershipLostError:
                raise
            except Exception as error:  # noqa: BLE001
                failures += 1
                if failures >= self.config.heartbeat_failure_threshold:
                    record_event(
                        workflow_id=task.workflow_id,
                        stage=task.stage,
                        event_type=WorkflowEventType.OWNERSHIP_LOST.value,
                    )
                    raise OwnershipLostError(
                        f"heartbeat failure threshold reached task_id={task.task_id} "
                        f"error_type={type(error).__name__}"
                    ) from error

    async def _cancel_handler(
        self,
        handler_task: asyncio.Task[Any],
        task: WorkflowTaskRecord,
        reason: str,
        *,
        write_state: bool = True,
    ) -> None:
        handler_task.cancel()
        _, pending = await asyncio.wait({handler_task}, timeout=self.config.cancel_cleanup_seconds)
        if pending:
            self._orphaned.add(handler_task)
            handler_task.add_done_callback(self._orphan_done)
        if write_state:
            await self._retry(task, reason, f"stage={task.stage} task_id={task.task_id} {reason}")

    async def _retry(self, task: WorkflowTaskRecord, error_code: str, message: str) -> None:
        delay = self.config.retry_backoff_base_seconds * (2 ** max(0, task.attempt - 1))
        delay += _JITTER_SOURCE.uniform(0, self.config.retry_jitter_seconds)
        try:
            changed = await self.backend.retry_task(
                task_id=task.task_id,
                owner=self.config.worker_id,
                lock_token=task.lock_token or "",
                error_code=error_code,
                error_message=safe_error_message(message),
                available_at=self._now() + timedelta(seconds=delay),
                now=self._now(),
            )
            if changed:
                current = await self.backend.get_task(task.task_id)
                if current is not None and current.status == TaskStatus.FAILED.value:
                    record_outcome(
                        workflow_id=task.workflow_id,
                        stage=task.stage,
                        technical_status="failed",
                        business_result_status=BusinessResultStatus.UNKNOWN.value,
                    )
                else:
                    record_event(
                        workflow_id=task.workflow_id,
                        stage=task.stage,
                        event_type=WorkflowEventType.RETRY.value,
                    )
        except Exception as error:  # noqa: BLE001
            self._log_task_error(task, error)

    async def _fail(self, task: WorkflowTaskRecord, error_code: str, message: str) -> None:
        try:
            changed = await self.backend.fail_task(
                task_id=task.task_id,
                owner=self.config.worker_id,
                lock_token=task.lock_token or "",
                error_code=error_code,
                error_message=safe_error_message(message),
                now=self._now(),
            )
            if changed:
                record_outcome(
                    workflow_id=task.workflow_id,
                    stage=task.stage,
                    technical_status="failed",
                    business_result_status=BusinessResultStatus.UNKNOWN.value,
                )
        except Exception as error:  # noqa: BLE001
            self._log_task_error(task, error)

    async def _wait_for_one(self) -> None:
        if not self._inflight:
            await self._sleep_or_stop()
            return
        await asyncio.wait(
            [item.run_task for item in self._inflight.values()],
            timeout=self.config.poll_interval_seconds,
            return_when=asyncio.FIRST_COMPLETED,
        )

    async def _sleep_or_stop(self) -> None:
        with contextlib.suppress(asyncio.TimeoutError):
            async with asyncio.timeout(self.config.poll_interval_seconds):
                await self._stopping.wait()

    async def _shutdown_inflight(self) -> None:
        if not self._inflight:
            return
        tasks = [item.run_task for item in self._inflight.values()]
        _, pending = await asyncio.wait(tasks, timeout=self.config.shutdown_grace_seconds)
        for run_task in pending:
            run_task.cancel()
        if pending:
            done, still_pending = await asyncio.wait(
                pending, timeout=self.config.cancel_cleanup_seconds
            )
            for run_task in still_pending:
                self._orphaned.add(run_task)
                run_task.add_done_callback(self._orphan_done)
            for run_task in done:
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await run_task

    def _orphan_done(self, task: asyncio.Task[Any]) -> None:
        self._orphaned.discard(task)
        with contextlib.suppress(asyncio.CancelledError, Exception):
            task.exception()

    @staticmethod
    def _log_control_error(error: Exception) -> None:
        # Structured logger adapters differ between deployments; never include DSNs or payloads.
        from piko.infra.logging import get_logger

        get_logger(__name__).error(
            "workflow_control_loop_error",
            error_type=type(error).__name__,
            error_message=safe_error_message(str(error)),
        )

    @staticmethod
    def _log_task_error(task: WorkflowTaskRecord, error: Exception) -> None:
        from piko.infra.logging import get_logger

        get_logger(__name__).error(
            "workflow_task_control_error",
            workflow_id=task.workflow_id,
            task_id=task.task_id,
            stage=task.stage,
            error_type=type(error).__name__,
            lock_token=short_lock_token(task.lock_token),
        )
