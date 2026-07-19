"""Backend contract and a deterministic in-memory implementation."""

from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import secrets
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import asdict
from datetime import datetime
from typing import Any, Protocol

from piko.workflow.state_machine import (
    DependencyDecision,
    dependency_decision,
    validate_dag,
    validate_transition,
)
from piko.infra.db import utcnow
from piko.workflow.types import (
    BusinessHook,
    BusinessResultStatus,
    DependencySpec,
    IdempotencyConflictError,
    OwnershipLostError,
    RunStatus,
    TaskResult,
    TaskStatus,
    WorkflowDefinition,
    WorkflowEventType,
    WorkflowManifest,
    WorkflowRunRecord,
    WorkflowTaskEvent,
    WorkflowTaskRecord,
    aggregate_business_result_status,
)


MANUAL_CONTROL_TRANSITIONS: Mapping[str, tuple[str, frozenset[str]]] = {
    "retry": (
        TaskStatus.READY.value,
        frozenset(
            {
                TaskStatus.FAILED.value,
                TaskStatus.RETRY_WAITING.value,
                TaskStatus.BLOCKED.value,
                TaskStatus.CANCELED.value,
            }
        ),
    ),
    "unblock": (TaskStatus.READY.value, frozenset({TaskStatus.BLOCKED.value})),
    "cancel": (
        TaskStatus.CANCELED.value,
        frozenset(
            {
                TaskStatus.PENDING.value,
                TaskStatus.READY.value,
                TaskStatus.RUNNING.value,
                TaskStatus.RETRY_WAITING.value,
                TaskStatus.BLOCKED.value,
            }
        ),
    ),
}


def manual_control_target(action: str, current_status: str) -> str | None:
    """Return the explicit operator target for an allowed current status."""
    rule = MANUAL_CONTROL_TRANSITIONS.get(action)
    if rule is None or current_status not in rule[1]:
        return None
    return rule[0]


class WorkflowControlBackend(Protocol):
    """The durable workflow contract implemented by every backend."""

    async def create_run(
        self,
        definition: WorkflowDefinition,
        *,
        now: datetime | None = None,
    ) -> WorkflowRunRecord: ...

    async def claim_ready_tasks(
        self,
        *,
        worker_id: str,
        stages: Sequence[str],
        lease_until: datetime,
        now: datetime,
        limit: int,
    ) -> list[WorkflowTaskRecord]: ...

    async def heartbeat(
        self,
        *,
        task_id: str,
        owner: str,
        lock_token: str,
        lease_until: datetime,
        now: datetime,
    ) -> bool: ...

    async def recover_expired_running_tasks(self, *, now: datetime) -> int: ...

    async def recover_retry_waiting_tasks(self, *, now: datetime) -> int: ...

    async def activate_ready_tasks(self, *, now: datetime) -> int: ...

    async def retry_task(
        self,
        *,
        task_id: str,
        owner: str,
        lock_token: str,
        error_code: str,
        error_message: str,
        available_at: datetime,
        now: datetime,
    ) -> bool: ...

    async def fail_task(
        self,
        *,
        task_id: str,
        owner: str,
        lock_token: str,
        error_code: str,
        error_message: str,
        now: datetime,
    ) -> bool: ...

    async def cancel_task(
        self,
        *,
        task_id: str,
        owner: str | None = None,
        lock_token: str | None = None,
        now: datetime,
    ) -> bool: ...

    async def finalize_task(
        self,
        *,
        task: WorkflowTaskRecord,
        result: TaskResult,
        now: datetime,
        business_hook: BusinessHook | None = None,
    ) -> bool: ...

    async def control_task(
        self,
        *,
        run_id: str,
        stage: str,
        action: str,
        reason_digest: str,
        now: datetime | None = None,
    ) -> dict[str, str] | None: ...

    async def get_run(self, run_id: str) -> WorkflowRunRecord | None: ...

    async def get_task(self, task_id: str) -> WorkflowTaskRecord | None: ...

    async def get_manifest(self, task_id: str) -> WorkflowManifest | None: ...

    async def list_events(self, *, task_id: str | None = None) -> list[WorkflowTaskEvent]: ...


class MemoryTransaction:
    """Small fake transaction passed to in-memory business hooks."""

    def __init__(self, outputs: dict[str, Any]) -> None:
        self.outputs = outputs

    def put(self, key: str, value: Any) -> None:
        self.outputs[key] = copy.deepcopy(value)


class InMemoryWorkflowRepository:
    """Reference backend used by contract tests.

    The lock and copy-on-write finalization model intentionally mirrors the
    atomic boundaries of the MySQL implementation without pretending to test
    database locking.
    """

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self.runs: dict[str, WorkflowRunRecord] = {}
        self.tasks: dict[str, WorkflowTaskRecord] = {}
        self.dependencies: dict[str, tuple[DependencySpec, ...]] = {}
        self.manifests: dict[str, WorkflowManifest] = {}
        self.events: list[WorkflowTaskEvent] = []
        self.business_outputs: dict[str, Any] = {}
        self._run_by_idempotency: dict[tuple[str, str], str] = {}
        self._next_event_id = 1

    @staticmethod
    def _digest(value: Mapping[str, Any]) -> str:
        encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
        return hashlib.sha256(encoded.encode()).hexdigest()

    @staticmethod
    def _copy_task(task: WorkflowTaskRecord) -> WorkflowTaskRecord:
        return copy.copy(task)

    def _event(
        self,
        task: WorkflowTaskRecord,
        event_type: str,
        now: datetime,
        payload: Mapping[str, Any] | None = None,
    ) -> None:
        event = WorkflowTaskEvent(
            event_id=self._next_event_id,
            task_id=task.task_id,
            run_id=task.run_id,
            stage=task.stage,
            event_type=event_type,
            payload=copy.deepcopy(dict(payload or {})),
            created_at=now,
        )
        self._next_event_id += 1
        self.events.append(event)

    def _replace_task(self, task_id: str, **changes: Any) -> WorkflowTaskRecord:
        task = self.tasks[task_id]
        updated = WorkflowTaskRecord(**{**asdict(task), **changes})
        self.tasks[task_id] = updated
        return updated

    def _refresh_run(self, run_id: str, now: datetime) -> None:
        run = self.runs[run_id]
        rows = [task for task in self.tasks.values() if task.run_id == run_id]
        if any(task.status in {TaskStatus.FAILED.value, TaskStatus.BLOCKED.value} for task in rows):
            status = RunStatus.FAILED.value
        elif any(task.status == TaskStatus.CANCELED.value for task in rows):
            status = RunStatus.CANCELED.value
        elif rows and all(task.status == TaskStatus.SUCCEEDED.value for task in rows):
            status = RunStatus.SUCCEEDED.value
        elif any(
            task.status
            in {
                TaskStatus.READY.value,
                TaskStatus.RUNNING.value,
                TaskStatus.RETRY_WAITING.value,
                TaskStatus.SUCCEEDED.value,
            }
            for task in rows
        ):
            status = RunStatus.RUNNING.value
        else:
            status = RunStatus.PENDING.value
        manifests = [m for m in self.manifests.values() if m.run_id == run_id]
        business = aggregate_business_result_status(
            len(rows), [manifest.result_status for manifest in manifests]
        )
        started = run.started_at
        if status != RunStatus.PENDING.value and started is None:
            started = now
        finished = (
            run.finished_at
            if status
            in {RunStatus.SUCCEEDED.value, RunStatus.FAILED.value, RunStatus.CANCELED.value}
            else None
        )
        self.runs[run_id] = WorkflowRunRecord(
            run_id=run.run_id,
            workflow_id=run.workflow_id,
            idempotency_key=run.idempotency_key,
            status=status,
            business_result_status=business,
            config_digest=run.config_digest,
            config_snapshot=copy.deepcopy(run.config_snapshot),
            created_at=run.created_at,
            updated_at=now,
            started_at=started,
            finished_at=finished
            or (
                now
                if status
                in {RunStatus.SUCCEEDED.value, RunStatus.FAILED.value, RunStatus.CANCELED.value}
                else None
            ),
        )

    def _upstream(self, task: WorkflowTaskRecord) -> dict[str, tuple[str, str | None]]:
        result: dict[str, tuple[str, str | None]] = {}
        for dependency in self.dependencies.get(task.task_id, ()):
            upstream_task = next(
                item
                for item in self.tasks.values()
                if item.run_id == task.run_id and item.stage == dependency.depends_on_stage
            )
            manifest = self.manifests.get(upstream_task.task_id)
            result[dependency.depends_on_stage] = (
                upstream_task.status,
                manifest.result_status if manifest else None,
            )
        return result

    def _add_run_tasks(self, definition: WorkflowDefinition, run_id: str, now: datetime) -> None:
        task_ids = {spec.stage: spec.task_id or uuid.uuid4().hex for spec in definition.tasks}
        for task_id in task_ids.values():
            if task_id in self.tasks:
                raise IdempotencyConflictError(f"task_id already exists: {task_id}")
        for spec in definition.tasks:
            task_key = spec.idempotency_key or f"{run_id}:{spec.stage}"
            if definition.rerun and spec.idempotency_key:
                task_key = f"{task_key}:rerun:{run_id}"
            if any(task.idempotency_key == task_key for task in self.tasks.values()):
                raise IdempotencyConflictError(f"task idempotency key already exists: {task_key}")
            status = TaskStatus.READY.value if not spec.dependencies else TaskStatus.PENDING.value
            task = WorkflowTaskRecord(
                task_id=task_ids[spec.stage],
                run_id=run_id,
                workflow_id=definition.workflow_id,
                stage=spec.stage,
                status=status,
                attempt=0,
                max_attempts=spec.max_attempts,
                available_at=now if status == TaskStatus.READY.value else None,
                owner=None,
                lock_token=None,
                lease_until=None,
                heartbeat_at=None,
                idempotency_key=task_key,
                error_code=None,
                error_message=None,
                created_at=now,
                updated_at=now,
            )
            self.tasks[task.task_id] = task
            self.dependencies[task.task_id] = tuple(spec.dependencies)
            self._event(task, "created", now, {"status": status})

    @staticmethod
    def _is_claim_candidate(task: WorkflowTaskRecord, stages: Sequence[str], now: datetime) -> bool:
        return (
            task.stage in stages
            and task.status == TaskStatus.READY.value
            and (task.available_at is None or task.available_at <= now)
        )

    def _claim_task(
        self,
        task: WorkflowTaskRecord,
        worker_id: str,
        lease_until: datetime,
        now: datetime,
    ) -> WorkflowTaskRecord | None:
        if task.attempt >= task.max_attempts:
            validate_transition(task.status, TaskStatus.FAILED.value)
            failed = self._replace_task(
                task.task_id,
                status=TaskStatus.FAILED.value,
                error_code="max_attempts_exceeded",
                error_message="maximum task attempts reached",
                finished_at=now,
                available_at=None,
                updated_at=now,
            )
            self._event(failed, "failure", now, {"error_code": "max_attempts_exceeded"})
            self._refresh_run(failed.run_id, now)
            return None
        claimed = self._replace_task(
            task.task_id,
            status=TaskStatus.RUNNING.value,
            owner=worker_id,
            lock_token=secrets.token_hex(32),
            lease_until=lease_until,
            heartbeat_at=now,
            attempt=task.attempt + 1,
            started_at=task.started_at or now,
            updated_at=now,
        )
        self._event(
            claimed,
            WorkflowEventType.CLAIM.value,
            now,
            {"attempt": claimed.attempt, "owner": worker_id},
        )
        return self._copy_task(claimed)

    async def create_run(
        self,
        definition: WorkflowDefinition,
        *,
        now: datetime | None = None,
    ) -> WorkflowRunRecord:
        now = now or utcnow()
        stages = [spec.stage for spec in definition.tasks]
        dependency_map = {spec.stage: spec.dependencies for spec in definition.tasks}
        validate_dag(stages, dependency_map)
        async with self._lock:
            existing_id = self._run_by_idempotency.get(
                (definition.workflow_id, definition.idempotency_key)
            )
            if existing_id and not definition.rerun:
                return copy.deepcopy(self.runs[existing_id])
            run_id = uuid.uuid4().hex
            run_key = definition.idempotency_key
            if existing_id:
                run_key = f"{run_key}:rerun:{run_id}"
            config_snapshot = copy.deepcopy(dict(definition.config_snapshot))
            run = WorkflowRunRecord(
                run_id=run_id,
                workflow_id=definition.workflow_id,
                idempotency_key=run_key,
                status=RunStatus.PENDING.value,
                business_result_status=BusinessResultStatus.UNKNOWN.value,
                config_digest=self._digest(config_snapshot),
                config_snapshot=config_snapshot,
                created_at=now,
                updated_at=now,
            )
            snapshot = (
                self.runs.copy(),
                self.tasks.copy(),
                self.dependencies.copy(),
                self.events.copy(),
                self._run_by_idempotency.copy(),
                self._next_event_id,
            )
            try:
                self.runs[run_id] = run
                if existing_id is None:
                    self._run_by_idempotency[
                        (definition.workflow_id, definition.idempotency_key)
                    ] = run_id
                self._add_run_tasks(definition, run_id, now)
                return copy.deepcopy(run)
            except Exception:
                (
                    self.runs,
                    self.tasks,
                    self.dependencies,
                    self.events,
                    self._run_by_idempotency,
                    self._next_event_id,
                ) = snapshot
                raise

    async def claim_ready_tasks(
        self,
        *,
        worker_id: str,
        stages: Sequence[str],
        lease_until: datetime,
        now: datetime,
        limit: int,
    ) -> list[WorkflowTaskRecord]:
        if limit <= 0 or not stages:
            return []
        async with self._lock:
            result: list[WorkflowTaskRecord] = []
            for task in sorted(
                self.tasks.values(), key=lambda item: (item.available_at or now, item.created_at)
            ):
                if len(result) >= limit:
                    break
                if not self._is_claim_candidate(task, stages, now):
                    continue
                claimed = self._claim_task(task, worker_id, lease_until, now)
                if claimed is not None:
                    result.append(claimed)
            for task in result:
                self._refresh_run(task.run_id, now)
            return result

    async def heartbeat(
        self,
        *,
        task_id: str,
        owner: str,
        lock_token: str,
        lease_until: datetime,
        now: datetime,
    ) -> bool:
        async with self._lock:
            task = self.tasks.get(task_id)
            if (
                task is None
                or task.status != TaskStatus.RUNNING.value
                or task.owner != owner
                or task.lock_token != lock_token
                or task.lease_until is None
                or task.lease_until <= now
            ):
                return False
            self._replace_task(task_id, lease_until=lease_until, heartbeat_at=now, updated_at=now)
            return True

    async def recover_expired_running_tasks(self, *, now: datetime) -> int:
        async with self._lock:
            recovered = 0
            for task in tuple(self.tasks.values()):
                if (
                    task.status != TaskStatus.RUNNING.value
                    or task.lease_until is None
                    or task.lease_until > now
                ):
                    continue
                terminal = task.attempt >= task.max_attempts
                target = TaskStatus.FAILED.value if terminal else TaskStatus.RETRY_WAITING.value
                validate_transition(task.status, target)
                task = self._replace_task(
                    task.task_id,
                    status=target,
                    available_at=None if terminal else now,
                    owner=None,
                    lock_token=None,
                    lease_until=None,
                    heartbeat_at=None,
                    finished_at=now if terminal else None,
                    error_code="max_attempts_exceeded" if terminal else "lease_expired",
                    error_message="maximum task attempts reached after lease expiry"
                    if terminal
                    else "task lease expired",
                    updated_at=now,
                )
                self._event(
                    task, "lease_expired", now, {"retryable": not terminal, "attempt": task.attempt}
                )
                self._refresh_run(task.run_id, now)
                recovered += 1
            return recovered

    async def recover_retry_waiting_tasks(self, *, now: datetime) -> int:
        async with self._lock:
            recovered = 0
            for task in tuple(self.tasks.values()):
                if (
                    task.status != TaskStatus.RETRY_WAITING.value
                    or task.available_at is None
                    or task.available_at > now
                ):
                    continue
                validate_transition(task.status, TaskStatus.READY.value)
                task = self._replace_task(
                    task.task_id, status=TaskStatus.READY.value, updated_at=now
                )
                self._event(
                    task, WorkflowEventType.RECOVERY.value, now, {"from_status": "retry_waiting"}
                )
                recovered += 1
            return recovered

    def _activate_pending_task(self, task: WorkflowTaskRecord, now: datetime) -> bool:
        decision = dependency_decision(
            self.dependencies.get(task.task_id, ()), self._upstream(task)
        )
        if decision == DependencyDecision.WAIT:
            return False
        target = (
            TaskStatus.READY.value
            if decision == DependencyDecision.READY
            else TaskStatus.BLOCKED.value
        )
        validate_transition(task.status, target)
        task = self._replace_task(
            task.task_id,
            status=target,
            available_at=now if target == TaskStatus.READY.value else None,
            finished_at=now if target == TaskStatus.BLOCKED.value else None,
            error_code="dependency_unavailable" if target == TaskStatus.BLOCKED.value else None,
            error_message="dependency rule was not satisfied"
            if target == TaskStatus.BLOCKED.value
            else None,
            updated_at=now,
        )
        self._event(
            task,
            WorkflowEventType.BLOCKED.value if target == TaskStatus.BLOCKED.value else "ready",
            now,
            {},
        )
        self._refresh_run(task.run_id, now)
        return True

    async def activate_ready_tasks(self, *, now: datetime) -> int:
        async with self._lock:
            changed = 0
            for task in tuple(self.tasks.values()):
                if task.status == TaskStatus.PENDING.value and self._activate_pending_task(
                    task, now
                ):
                    changed += 1
            return changed

    async def retry_task(
        self,
        *,
        task_id: str,
        owner: str,
        lock_token: str,
        error_code: str,
        error_message: str,
        available_at: datetime,
        now: datetime,
    ) -> bool:
        async with self._lock:
            task = self.tasks.get(task_id)
            if task is None or not self._owns(task, owner, lock_token, now):
                return False
            terminal = task.attempt >= task.max_attempts
            target = TaskStatus.FAILED.value if terminal else TaskStatus.RETRY_WAITING.value
            validate_transition(task.status, target)
            task = self._replace_task(
                task_id,
                status=target,
                available_at=None if terminal else available_at,
                owner=None,
                lock_token=None,
                lease_until=None,
                heartbeat_at=None,
                finished_at=now if terminal else None,
                error_code="max_attempts_exceeded" if terminal else error_code[:128],
                error_message=(
                    "maximum task attempts reached" if terminal else error_message[:1024]
                ),
                updated_at=now,
            )
            event_type = (
                WorkflowEventType.FAILURE.value if terminal else WorkflowEventType.RETRY.value
            )
            self._event(
                task,
                event_type,
                now,
                {
                    "error_code": "max_attempts_exceeded" if terminal else error_code,
                    "retryable": not terminal,
                },
            )
            self._refresh_run(task.run_id, now)
            return True

    async def fail_task(
        self,
        *,
        task_id: str,
        owner: str,
        lock_token: str,
        error_code: str,
        error_message: str,
        now: datetime,
    ) -> bool:
        async with self._lock:
            task = self.tasks.get(task_id)
            if task is None or not self._owns(task, owner, lock_token, now):
                return False
            validate_transition(task.status, TaskStatus.FAILED.value)
            task = self._replace_task(
                task_id,
                status=TaskStatus.FAILED.value,
                available_at=None,
                owner=None,
                lock_token=None,
                lease_until=None,
                heartbeat_at=None,
                finished_at=now,
                error_code=error_code[:128],
                error_message=error_message[:1024],
                updated_at=now,
            )
            self._event(task, WorkflowEventType.FAILURE.value, now, {"error_code": error_code})
            self._refresh_run(task.run_id, now)
            return True

    async def cancel_task(
        self,
        *,
        task_id: str,
        owner: str | None = None,
        lock_token: str | None = None,
        now: datetime,
    ) -> bool:
        async with self._lock:
            task = self.tasks.get(task_id)
            if task is None:
                return False
            if task.status == TaskStatus.RUNNING.value:
                if (
                    owner is None
                    or lock_token is None
                    or not self._owns(task, owner, lock_token, now)
                ):
                    return False
            elif owner is not None or lock_token is not None:
                return False
            elif task.status not in {
                TaskStatus.PENDING.value,
                TaskStatus.READY.value,
                TaskStatus.RETRY_WAITING.value,
                TaskStatus.BLOCKED.value,
            }:
                return False
            validate_transition(task.status, TaskStatus.CANCELED.value)
            task = self._replace_task(
                task_id,
                status=TaskStatus.CANCELED.value,
                available_at=None,
                owner=None,
                lock_token=None,
                lease_until=None,
                heartbeat_at=None,
                finished_at=now,
                updated_at=now,
            )
            self._event(task, WorkflowEventType.CANCELED.value, now, {})
            self._refresh_run(task.run_id, now)
            return True

    @staticmethod
    def _owns(task: WorkflowTaskRecord | None, owner: str, token: str, now: datetime) -> bool:
        return bool(
            task
            and task.status == TaskStatus.RUNNING.value
            and task.owner == owner
            and task.lock_token == token
            and task.lease_until is not None
            and task.lease_until > now
        )

    async def finalize_task(
        self,
        *,
        task: WorkflowTaskRecord,
        result: TaskResult,
        now: datetime,
        business_hook: BusinessHook | None = None,
    ) -> bool:
        async with self._lock:
            current = self.tasks.get(task.task_id)
            if current is None:
                raise OwnershipLostError("task no longer exists")
            existing = self.manifests.get(task.task_id)
            if current.status == TaskStatus.SUCCEEDED.value and existing is not None:
                if existing.idempotency_key == task.idempotency_key:
                    return False
            if not self._owns(current, task.owner or "", task.lock_token or "", now):
                raise OwnershipLostError("task ownership lost")
            snapshot = (
                copy.deepcopy(self.tasks),
                copy.deepcopy(self.manifests),
                copy.deepcopy(self.events),
                copy.deepcopy(self.business_outputs),
                copy.deepcopy(self.runs),
                self._next_event_id,
            )
            try:
                tx = MemoryTransaction(self.business_outputs)
                if business_hook is not None:
                    await business_hook(tx)
                manifest = WorkflowManifest(
                    task_id=current.task_id,
                    run_id=current.run_id,
                    idempotency_key=current.idempotency_key,
                    result_status=result.result_status,
                    result_payload=copy.deepcopy(dict(result.result_payload)),
                    output_digest=result.output_digest,
                    created_at=now,
                    updated_at=now,
                )
                other = next(
                    (
                        item
                        for item in self.manifests.values()
                        if item.idempotency_key == manifest.idempotency_key
                    ),
                    None,
                )
                if other is not None and other.task_id != manifest.task_id:
                    raise IdempotencyConflictError("manifest idempotency key already has an owner")
                self.manifests[current.task_id] = manifest
                validate_transition(current.status, TaskStatus.SUCCEEDED.value)
                current = self._replace_task(
                    current.task_id,
                    status=TaskStatus.SUCCEEDED.value,
                    available_at=None,
                    owner=None,
                    lock_token=None,
                    lease_until=None,
                    heartbeat_at=None,
                    finished_at=now,
                    error_code=None,
                    error_message=None,
                    updated_at=now,
                )
                self._event(
                    current,
                    WorkflowEventType.FINALIZE.value,
                    now,
                    {"result_status": result.result_status},
                )
                self._refresh_run(current.run_id, now)
            except Exception:
                (
                    self.tasks,
                    self.manifests,
                    self.events,
                    self.business_outputs,
                    self.runs,
                    self._next_event_id,
                ) = snapshot
                raise
            return True

    async def control_task(
        self,
        *,
        run_id: str,
        stage: str,
        action: str,
        reason_digest: str,
        now: datetime | None = None,
    ) -> dict[str, str] | None:
        """Apply an audited operator transition without weakening fencing."""
        if not reason_digest.strip():
            raise ValueError("reason_digest must not be blank")
        now = now or utcnow()
        async with self._lock:
            task = next(
                (
                    item
                    for item in self.tasks.values()
                    if item.run_id == run_id and item.stage == stage
                ),
                None,
            )
            if task is None:
                return None
            target = manual_control_target(action, task.status)
            if target is None:
                return None
            previous_status = task.status
            task = self._replace_task(
                task.task_id,
                status=target,
                available_at=None if target == TaskStatus.CANCELED.value else now,
                owner=None,
                lock_token=None,
                lease_until=None,
                heartbeat_at=None,
                error_code=None,
                error_message=None,
                finished_at=now if target == TaskStatus.CANCELED.value else None,
                updated_at=now,
            )
            self._event(
                task,
                f"manual_{action}",
                now,
                {
                    "action": action,
                    "from_status": previous_status,
                    "to_status": target,
                    "reason_digest": reason_digest,
                },
            )
            self._refresh_run(run_id, now)
            return {"task_id": task.task_id, "stage": stage, "status": target, "action": action}

    async def get_run(self, run_id: str) -> WorkflowRunRecord | None:
        async with self._lock:
            run = self.runs.get(run_id)
            return copy.deepcopy(run) if run is not None else None

    async def get_task(self, task_id: str) -> WorkflowTaskRecord | None:
        async with self._lock:
            task = self.tasks.get(task_id)
            return self._copy_task(task) if task else None

    async def get_manifest(self, task_id: str) -> WorkflowManifest | None:
        async with self._lock:
            return copy.deepcopy(self.manifests.get(task_id)) if task_id in self.manifests else None

    async def list_events(self, *, task_id: str | None = None) -> list[WorkflowTaskEvent]:
        async with self._lock:
            return [
                copy.deepcopy(event)
                for event in self.events
                if task_id is None or event.task_id == task_id
            ]
