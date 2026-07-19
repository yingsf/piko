"""MySQL implementation of the generic workflow control contract."""

from __future__ import annotations

import hashlib
import json
import secrets
import uuid
from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any

from sqlalchemy import func, or_, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from piko.infra.db import (
    WorkflowRun,
    WorkflowTask,
    WorkflowTaskDependency,
    WorkflowTaskEvent,
    WorkflowTaskManifest,
    utcnow,
)
from piko.workflow.repository import (
    BusinessHook,
    WorkflowControlBackend,
    manual_control_target,
)
from piko.workflow.state_machine import DependencyDecision, dependency_decision, validate_dag
from piko.workflow.types import (
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
    WorkflowTaskEvent as WorkflowTaskEventRecord,
    WorkflowTaskRecord,
    aggregate_business_result_status,
)

_EMPTY_LOCK_TOKEN: str | None = None


def _technical_run_status(tasks: Sequence[WorkflowTask]) -> str:
    statuses = {task.status for task in tasks}
    if statuses & {TaskStatus.FAILED.value, TaskStatus.BLOCKED.value}:
        return RunStatus.FAILED.value
    if TaskStatus.CANCELED.value in statuses:
        return RunStatus.CANCELED.value
    if tasks and statuses == {TaskStatus.SUCCEEDED.value}:
        return RunStatus.SUCCEEDED.value
    if statuses & {
        TaskStatus.READY.value,
        TaskStatus.RUNNING.value,
        TaskStatus.RETRY_WAITING.value,
        TaskStatus.SUCCEEDED.value,
    }:
        return RunStatus.RUNNING.value
    return RunStatus.PENDING.value


def _business_run_status(
    tasks: Sequence[WorkflowTask], manifests: Sequence[WorkflowTaskManifest]
) -> str:
    return aggregate_business_result_status(
        len(tasks), [manifest.result_status for manifest in manifests]
    )


class MySQLWorkflowRepository(WorkflowControlBackend):
    """Transactional repository using InnoDB row locks and fencing predicates."""

    def __init__(self, session_maker: async_sessionmaker[AsyncSession]) -> None:
        self.session_maker = session_maker

    @staticmethod
    def _rowcount(result: Any) -> int:
        return int(getattr(result, "rowcount", 0) or 0)

    @staticmethod
    def _digest(value: Mapping[str, Any]) -> str:
        encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
        return hashlib.sha256(encoded.encode()).hexdigest()

    @staticmethod
    def _run_record(row: WorkflowRun) -> WorkflowRunRecord:
        return WorkflowRunRecord(
            run_id=row.run_id,
            workflow_id=row.workflow_id,
            idempotency_key=row.idempotency_key,
            status=row.status,
            business_result_status=row.business_result_status,
            config_digest=row.config_digest,
            config_snapshot=dict(row.config_snapshot_json),
            created_at=row.created_at,
            updated_at=row.updated_at,
            started_at=row.started_at,
            finished_at=row.finished_at,
        )

    @staticmethod
    def _task_record(row: WorkflowTask) -> WorkflowTaskRecord:
        return WorkflowTaskRecord(
            task_id=row.task_id,
            run_id=row.run_id,
            workflow_id=row.workflow_id,
            stage=row.stage,
            status=row.status,
            attempt=row.attempt,
            max_attempts=row.max_attempts,
            available_at=row.available_at,
            owner=row.owner,
            lock_token=row.lock_token,
            lease_until=row.lease_until,
            heartbeat_at=row.heartbeat_at,
            idempotency_key=row.idempotency_key,
            error_code=row.error_code,
            error_message=row.error_message,
            created_at=row.created_at,
            updated_at=row.updated_at,
            started_at=row.started_at,
            finished_at=row.finished_at,
        )

    @staticmethod
    def _manifest_record(row: WorkflowTaskManifest) -> WorkflowManifest:
        return WorkflowManifest(
            task_id=row.task_id,
            run_id=row.run_id,
            idempotency_key=row.idempotency_key,
            result_status=row.result_status,
            result_payload=dict(row.result_json),
            output_digest=row.output_digest,
            created_at=row.created_at,
            updated_at=row.updated_at,
        )

    @staticmethod
    def _event(
        session: AsyncSession,
        task: WorkflowTask,
        event_type: str,
        now: datetime,
        payload: Mapping[str, Any] | None = None,
    ) -> None:
        session.add(
            WorkflowTaskEvent(
                task_id=task.task_id,
                run_id=task.run_id,
                stage=task.stage,
                event_type=event_type,
                payload_json=dict(payload or {}),
                created_at=now,
            )
        )

    @staticmethod
    async def _find_existing_run(
        session: AsyncSession, definition: WorkflowDefinition
    ) -> WorkflowRun | None:
        return (
            await session.execute(
                select(WorkflowRun).where(
                    WorkflowRun.workflow_id == definition.workflow_id,
                    WorkflowRun.idempotency_key == definition.idempotency_key,
                )
            )
        ).scalar_one_or_none()

    @staticmethod
    def _run_key(definition: WorkflowDefinition, run_id: str, has_original: bool) -> str:
        if definition.rerun and has_original:
            return f"{definition.idempotency_key}:rerun:{run_id}"
        return definition.idempotency_key

    async def _insert_run_tasks(
        self,
        session: AsyncSession,
        definition: WorkflowDefinition,
        run_id: str,
        now: datetime,
    ) -> None:
        task_ids = {spec.stage: spec.task_id or uuid.uuid4().hex for spec in definition.tasks}
        for spec in definition.tasks:
            task_key = spec.idempotency_key or f"{run_id}:{spec.stage}"
            if definition.rerun and spec.idempotency_key:
                task_key = f"{task_key}:rerun:{run_id}"
            task = WorkflowTask(
                task_id=task_ids[spec.stage],
                run_id=run_id,
                workflow_id=definition.workflow_id,
                stage=spec.stage,
                status=TaskStatus.READY.value
                if not spec.dependencies
                else TaskStatus.PENDING.value,
                attempt=0,
                max_attempts=spec.max_attempts,
                available_at=now if not spec.dependencies else None,
                idempotency_key=task_key,
                created_at=now,
                updated_at=now,
            )
            session.add(task)
            self._event(session, task, "created", now, {"status": task.status})
        await session.flush()
        for spec in definition.tasks:
            for edge in spec.dependencies:
                session.add(
                    WorkflowTaskDependency(
                        run_id=run_id,
                        task_id=task_ids[spec.stage],
                        depends_on_task_id=task_ids[edge.depends_on_stage],
                        condition_json=edge.as_json(),
                    )
                )

    async def _recover_idempotent_conflict(
        self, definition: WorkflowDefinition, error: IntegrityError
    ) -> WorkflowRunRecord:
        if definition.rerun:
            raise error
        async with self.session_maker() as session:
            existing = await self._find_existing_run(session, definition)
        if existing is not None:
            return self._run_record(existing)
        raise IdempotencyConflictError("workflow run creation conflicted") from error

    async def _refresh_run(self, session: AsyncSession, run_id: str, now: datetime) -> None:
        run = await session.get(WorkflowRun, run_id)
        if run is None:
            return
        tasks = (
            (await session.execute(select(WorkflowTask).where(WorkflowTask.run_id == run_id)))
            .scalars()
            .all()
        )
        manifests = (
            (
                await session.execute(
                    select(WorkflowTaskManifest).where(WorkflowTaskManifest.run_id == run_id)
                )
            )
            .scalars()
            .all()
        )
        run.status = _technical_run_status(tasks)
        run.business_result_status = _business_run_status(tasks, manifests)
        if run.status != RunStatus.PENDING.value and run.started_at is None:
            run.started_at = now
        run.finished_at = (
            now
            if run.status
            in {RunStatus.SUCCEEDED.value, RunStatus.FAILED.value, RunStatus.CANCELED.value}
            else None
        )
        run.updated_at = now

    async def create_run(
        self,
        definition: WorkflowDefinition,
        *,
        now: datetime | None = None,
    ) -> WorkflowRunRecord:
        now = now or utcnow()
        stages = tuple(spec.stage for spec in definition.tasks)
        dependencies = {spec.stage: spec.dependencies for spec in definition.tasks}
        validate_dag(stages, dependencies)
        run_id = uuid.uuid4().hex
        try:
            async with self.session_maker() as session, session.begin():
                existing = await self._find_existing_run(session, definition)
                if existing is not None and not definition.rerun:
                    return self._run_record(existing)
                run_key = self._run_key(definition, run_id, existing is not None)
                run = WorkflowRun(
                    run_id=run_id,
                    workflow_id=definition.workflow_id,
                    idempotency_key=run_key,
                    config_snapshot_json=dict(definition.config_snapshot),
                    config_digest=self._digest(definition.config_snapshot),
                    status=RunStatus.PENDING.value,
                    business_result_status=BusinessResultStatus.UNKNOWN.value,
                    created_at=now,
                    updated_at=now,
                )
                session.add(run)
                await session.flush()
                await self._insert_run_tasks(session, definition, run_id, now)
                return self._run_record(run)
        except IntegrityError as error:
            return await self._recover_idempotent_conflict(definition, error)

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
        registered_stages = tuple(stages)
        async with self.session_maker() as session, session.begin():
            exhausted = (
                (
                    await session.execute(
                        select(WorkflowTask)
                        .where(
                            WorkflowTask.status == TaskStatus.READY.value,
                            WorkflowTask.stage.in_(registered_stages),
                            WorkflowTask.attempt >= WorkflowTask.max_attempts,
                        )
                        .with_for_update(skip_locked=True)
                    )
                )
                .scalars()
                .all()
            )
            for row in exhausted:
                result = await session.execute(
                    update(WorkflowTask)
                    .where(
                        WorkflowTask.task_id == row.task_id,
                        WorkflowTask.status == TaskStatus.READY.value,
                        WorkflowTask.attempt >= WorkflowTask.max_attempts,
                    )
                    .values(
                        status=TaskStatus.FAILED.value,
                        available_at=None,
                        finished_at=now,
                        error_code="max_attempts_exceeded",
                        error_message="maximum task attempts reached",
                        updated_at=now,
                    )
                )
                if self._rowcount(result) == 1:
                    self._event(
                        session,
                        row,
                        WorkflowEventType.FAILURE.value,
                        now,
                        {"error_code": "max_attempts_exceeded"},
                    )
                    await self._refresh_run(session, row.run_id, now)
            rows = (
                (
                    await session.execute(
                        select(WorkflowTask)
                        .where(
                            WorkflowTask.status == TaskStatus.READY.value,
                            WorkflowTask.stage.in_(registered_stages),
                            WorkflowTask.attempt < WorkflowTask.max_attempts,
                            or_(
                                WorkflowTask.available_at.is_(None),
                                WorkflowTask.available_at <= now,
                            ),
                        )
                        .order_by(
                            WorkflowTask.available_at, WorkflowTask.created_at, WorkflowTask.task_id
                        )
                        .limit(limit)
                        .with_for_update(skip_locked=True)
                    )
                )
                .scalars()
                .all()
            )
            claimed: list[WorkflowTaskRecord] = []
            for row in rows:
                token = secrets.token_hex(32)
                updated = await session.execute(
                    update(WorkflowTask)
                    .where(
                        WorkflowTask.task_id == row.task_id,
                        WorkflowTask.status == TaskStatus.READY.value,
                        WorkflowTask.stage.in_(list(stages)),
                        WorkflowTask.attempt < WorkflowTask.max_attempts,
                    )
                    .values(
                        status=TaskStatus.RUNNING.value,
                        owner=worker_id,
                        lock_token=token,
                        lease_until=lease_until,
                        heartbeat_at=now,
                        attempt=WorkflowTask.attempt + 1,
                        started_at=func.coalesce(WorkflowTask.started_at, now),
                        updated_at=now,
                    )
                )
                if self._rowcount(updated) != 1:
                    continue
                refreshed = (
                    await session.execute(
                        select(WorkflowTask)
                        .where(WorkflowTask.task_id == row.task_id)
                        .execution_options(populate_existing=True)
                    )
                ).scalar_one()
                self._event(
                    session,
                    refreshed,
                    WorkflowEventType.CLAIM.value,
                    now,
                    {"attempt": refreshed.attempt, "owner": worker_id},
                )
                claimed.append(self._task_record(refreshed))
                await self._refresh_run(session, refreshed.run_id, now)
            return claimed

    async def heartbeat(
        self,
        *,
        task_id: str,
        owner: str,
        lock_token: str,
        lease_until: datetime,
        now: datetime,
    ) -> bool:
        async with self.session_maker() as session, session.begin():
            result = await session.execute(
                update(WorkflowTask)
                .where(
                    WorkflowTask.task_id == task_id,
                    WorkflowTask.status == TaskStatus.RUNNING.value,
                    WorkflowTask.owner == owner,
                    WorkflowTask.lock_token == lock_token,
                    WorkflowTask.lease_until > now,
                )
                .values(lease_until=lease_until, heartbeat_at=now, updated_at=now)
            )
            if self._rowcount(result) != 1:
                return False
            row = await session.get(WorkflowTask, task_id)
            if row is not None:
                self._event(session, row, WorkflowEventType.HEARTBEAT.value, now, {"owner": owner})
            return True

    async def recover_expired_running_tasks(self, *, now: datetime) -> int:
        async with self.session_maker() as session, session.begin():
            rows = (
                (
                    await session.execute(
                        select(WorkflowTask)
                        .where(
                            WorkflowTask.status == TaskStatus.RUNNING.value,
                            WorkflowTask.lease_until.is_not(None),
                            WorkflowTask.lease_until <= now,
                        )
                        .with_for_update(skip_locked=True)
                    )
                )
                .scalars()
                .all()
            )
            recovered = 0
            for row in rows:
                terminal = row.attempt >= row.max_attempts
                target = TaskStatus.FAILED.value if terminal else TaskStatus.RETRY_WAITING.value
                values: dict[str, Any] = {
                    "status": target,
                    "available_at": None if terminal else now,
                    "owner": None,
                    "lock_token": _EMPTY_LOCK_TOKEN,
                    "lease_until": None,
                    "heartbeat_at": None,
                    "finished_at": now if terminal else None,
                    "error_code": "max_attempts_exceeded" if terminal else "lease_expired",
                    "error_message": "maximum task attempts reached after lease expiry"
                    if terminal
                    else "task lease expired",
                    "updated_at": now,
                }
                result = await session.execute(
                    update(WorkflowTask)
                    .where(
                        WorkflowTask.task_id == row.task_id,
                        WorkflowTask.status == TaskStatus.RUNNING.value,
                        WorkflowTask.lease_until <= now,
                    )
                    .values(**values)
                )
                if self._rowcount(result) != 1:
                    continue
                self._event(
                    session,
                    row,
                    WorkflowEventType.LEASE_EXPIRED.value,
                    now,
                    {"retryable": not terminal, "attempt": row.attempt},
                )
                await self._refresh_run(session, row.run_id, now)
                recovered += 1
            return recovered

    async def recover_retry_waiting_tasks(self, *, now: datetime) -> int:
        async with self.session_maker() as session, session.begin():
            rows = (
                (
                    await session.execute(
                        select(WorkflowTask)
                        .where(
                            WorkflowTask.status == TaskStatus.RETRY_WAITING.value,
                            WorkflowTask.available_at.is_not(None),
                            WorkflowTask.available_at <= now,
                        )
                        .with_for_update(skip_locked=True)
                    )
                )
                .scalars()
                .all()
            )
            recovered = 0
            for row in rows:
                result = await session.execute(
                    update(WorkflowTask)
                    .where(
                        WorkflowTask.task_id == row.task_id,
                        WorkflowTask.status == TaskStatus.RETRY_WAITING.value,
                        WorkflowTask.available_at <= now,
                    )
                    .values(status=TaskStatus.READY.value, updated_at=now)
                )
                if self._rowcount(result) == 1:
                    self._event(
                        session,
                        row,
                        WorkflowEventType.RECOVERY.value,
                        now,
                        {"from_status": "retry_waiting"},
                    )
                    recovered += 1
            return recovered

    async def _dependency_decision_for_row(
        self, session: AsyncSession, row: WorkflowTask
    ) -> DependencyDecision:
        dependencies = (
            (
                await session.execute(
                    select(WorkflowTaskDependency).where(
                        WorkflowTaskDependency.run_id == row.run_id,
                        WorkflowTaskDependency.task_id == row.task_id,
                    )
                )
            )
            .scalars()
            .all()
        )
        specs: list[DependencySpec] = []
        upstream_states: dict[str, tuple[str, str | None]] = {}
        for dependency in dependencies:
            upstream = await session.get(WorkflowTask, dependency.depends_on_task_id)
            if upstream is None or upstream.run_id != row.run_id:
                return DependencyDecision.BLOCK
            condition = dict(dependency.condition_json)
            specs.append(
                DependencySpec(
                    depends_on_stage=upstream.stage,
                    allowed_business_statuses=tuple(
                        condition.get(
                            "allowed_business_statuses",
                            (BusinessResultStatus.COMPLETE.value,),
                        )
                    ),
                    allowed_technical_statuses=tuple(
                        condition.get("allowed_technical_statuses", (TaskStatus.SUCCEEDED.value,))
                    ),
                )
            )
            manifest = (
                await session.execute(
                    select(WorkflowTaskManifest).where(
                        WorkflowTaskManifest.task_id == upstream.task_id
                    )
                )
            ).scalar_one_or_none()
            upstream_states[upstream.stage] = (
                upstream.status,
                manifest.result_status if manifest is not None else None,
            )
        if len(specs) != len(dependencies):
            return DependencyDecision.BLOCK
        return dependency_decision(specs, upstream_states)

    async def _activate_pending_row(
        self, session: AsyncSession, row: WorkflowTask, now: datetime
    ) -> int:
        decision = await self._dependency_decision_for_row(session, row)
        if decision == DependencyDecision.WAIT:
            return 0
        target = (
            TaskStatus.READY.value
            if decision == DependencyDecision.READY
            else TaskStatus.BLOCKED.value
        )
        result = await session.execute(
            update(WorkflowTask)
            .where(
                WorkflowTask.task_id == row.task_id,
                WorkflowTask.status == TaskStatus.PENDING.value,
            )
            .values(
                status=target,
                available_at=now if target == TaskStatus.READY.value else None,
                finished_at=now if target == TaskStatus.BLOCKED.value else None,
                error_code="dependency_unavailable" if target == TaskStatus.BLOCKED.value else None,
                error_message="dependency rule was not satisfied"
                if target == TaskStatus.BLOCKED.value
                else None,
                updated_at=now,
            )
        )
        if self._rowcount(result) != 1:
            return 0
        self._event(
            session,
            row,
            WorkflowEventType.BLOCKED.value if target == TaskStatus.BLOCKED.value else "ready",
            now,
            {},
        )
        await self._refresh_run(session, row.run_id, now)
        return 1

    async def activate_ready_tasks(self, *, now: datetime) -> int:
        async with self.session_maker() as session, session.begin():
            rows = (
                (
                    await session.execute(
                        select(WorkflowTask)
                        .where(WorkflowTask.status == TaskStatus.PENDING.value)
                        .with_for_update(skip_locked=True)
                    )
                )
                .scalars()
                .all()
            )
            changed = 0
            for row in rows:
                changed += await self._activate_pending_row(session, row, now)
            return changed

    async def _owned_transition(
        self,
        *,
        task_id: str,
        owner: str,
        lock_token: str,
        target: str,
        values: Mapping[str, Any],
        event_type: str,
        payload: Mapping[str, Any],
        now: datetime,
    ) -> bool:
        async with self.session_maker() as session, session.begin():
            result = await session.execute(
                update(WorkflowTask)
                .where(
                    WorkflowTask.task_id == task_id,
                    WorkflowTask.status == TaskStatus.RUNNING.value,
                    WorkflowTask.owner == owner,
                    WorkflowTask.lock_token == lock_token,
                    WorkflowTask.lease_until > now,
                )
                .values(status=target, **dict(values), updated_at=now)
            )
            if self._rowcount(result) != 1:
                return False
            row = await session.get(WorkflowTask, task_id)
            if row is None:
                return False
            self._event(session, row, event_type, now, payload)
            await self._refresh_run(session, row.run_id, now)
            return True

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
        async with self.session_maker() as session, session.begin():
            current = (
                await session.execute(
                    select(WorkflowTask)
                    .where(
                        WorkflowTask.task_id == task_id,
                        WorkflowTask.status == TaskStatus.RUNNING.value,
                        WorkflowTask.owner == owner,
                        WorkflowTask.lock_token == lock_token,
                        WorkflowTask.lease_until > now,
                    )
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if current is None:
                return False
            terminal = current.attempt >= current.max_attempts
            target = TaskStatus.FAILED.value if terminal else TaskStatus.RETRY_WAITING.value
            result = await session.execute(
                update(WorkflowTask)
                .where(
                    WorkflowTask.task_id == task_id,
                    WorkflowTask.status == TaskStatus.RUNNING.value,
                    WorkflowTask.owner == owner,
                    WorkflowTask.lock_token == lock_token,
                    WorkflowTask.lease_until > now,
                )
                .values(
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
            )
            if self._rowcount(result) != 1:
                return False
            row = await session.get(WorkflowTask, task_id)
            if row is None:
                return False
            terminal_event = (
                WorkflowEventType.FAILURE.value if terminal else WorkflowEventType.RETRY.value
            )
            self._event(
                session,
                row,
                terminal_event,
                now,
                {
                    "error_code": "max_attempts_exceeded" if terminal else error_code,
                    "retryable": not terminal,
                },
            )
            await self._refresh_run(session, row.run_id, now)
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
        return await self._owned_transition(
            task_id=task_id,
            owner=owner,
            lock_token=lock_token,
            target=TaskStatus.FAILED.value,
            values={
                "available_at": None,
                "owner": None,
                "lock_token": _EMPTY_LOCK_TOKEN,
                "lease_until": None,
                "heartbeat_at": None,
                "finished_at": now,
                "error_code": error_code[:128],
                "error_message": error_message[:1024],
            },
            event_type=WorkflowEventType.FAILURE.value,
            payload={"error_code": error_code},
            now=now,
        )

    async def cancel_task(
        self,
        *,
        task_id: str,
        owner: str | None = None,
        lock_token: str | None = None,
        now: datetime,
    ) -> bool:
        if owner is None and lock_token is None:
            async with self.session_maker() as session, session.begin():
                result = await session.execute(
                    update(WorkflowTask)
                    .where(
                        WorkflowTask.task_id == task_id,
                        WorkflowTask.status.in_(
                            [
                                TaskStatus.PENDING.value,
                                TaskStatus.READY.value,
                                TaskStatus.RETRY_WAITING.value,
                                TaskStatus.BLOCKED.value,
                            ]
                        ),
                    )
                    .values(
                        status=TaskStatus.CANCELED.value,
                        available_at=None,
                        finished_at=now,
                        updated_at=now,
                    )
                )
                if self._rowcount(result) != 1:
                    return False
                row = await session.get(WorkflowTask, task_id)
                if row is None:
                    return False
                self._event(session, row, WorkflowEventType.CANCELED.value, now, {})
                await self._refresh_run(session, row.run_id, now)
                return True
        if owner is None or lock_token is None:
            return False
        return await self._owned_transition(
            task_id=task_id,
            owner=owner,
            lock_token=lock_token,
            target=TaskStatus.CANCELED.value,
            values={
                "available_at": None,
                "owner": None,
                "lock_token": _EMPTY_LOCK_TOKEN,
                "lease_until": None,
                "heartbeat_at": None,
                "finished_at": now,
            },
            event_type=WorkflowEventType.CANCELED.value,
            payload={},
            now=now,
        )

    async def finalize_task(
        self,
        *,
        task: WorkflowTaskRecord,
        result: TaskResult,
        now: datetime,
        business_hook: BusinessHook | None = None,
    ) -> bool:
        async with self.session_maker() as session, session.begin():
            current = (
                await session.execute(
                    select(WorkflowTask)
                    .where(WorkflowTask.task_id == task.task_id)
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if current is None:
                raise OwnershipLostError("task no longer exists")
            existing = (
                await session.execute(
                    select(WorkflowTaskManifest).where(WorkflowTaskManifest.task_id == task.task_id)
                )
            ).scalar_one_or_none()
            if current.status == TaskStatus.SUCCEEDED.value and existing is not None:
                if existing.idempotency_key == task.idempotency_key:
                    return False
            updated = await session.execute(
                update(WorkflowTask)
                .where(
                    WorkflowTask.task_id == task.task_id,
                    WorkflowTask.status == TaskStatus.RUNNING.value,
                    WorkflowTask.owner == task.owner,
                    WorkflowTask.lock_token == task.lock_token,
                    WorkflowTask.lease_until > now,
                )
                .values(
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
            )
            if self._rowcount(updated) != 1:
                raise OwnershipLostError("task ownership lost")
            if business_hook is not None:
                await business_hook(session)
            duplicate = (
                await session.execute(
                    select(WorkflowTaskManifest)
                    .where(WorkflowTaskManifest.idempotency_key == task.idempotency_key)
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if duplicate is not None and duplicate.task_id != task.task_id:
                raise IdempotencyConflictError("manifest idempotency key already has an owner")
            if duplicate is None:
                session.add(
                    WorkflowTaskManifest(
                        task_id=task.task_id,
                        run_id=task.run_id,
                        idempotency_key=task.idempotency_key,
                        result_status=result.result_status,
                        result_json=dict(result.result_payload),
                        output_digest=result.output_digest,
                        created_at=now,
                        updated_at=now,
                    )
                )
            await session.flush()
            refreshed = await session.get(WorkflowTask, task.task_id)
            if refreshed is None:
                raise OwnershipLostError("task disappeared during finalization")
            self._event(
                session,
                refreshed,
                WorkflowEventType.FINALIZE.value,
                now,
                {"result_status": result.result_status},
            )
            await session.flush()
            await self._refresh_run(session, task.run_id, now)
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
        async with self.session_maker() as session, session.begin():
            row = (
                await session.execute(
                    select(WorkflowTask)
                    .where(
                        WorkflowTask.run_id == run_id,
                        WorkflowTask.stage == stage,
                    )
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if row is None:
                return None
            target = manual_control_target(action, row.status)
            if target is None:
                return None
            previous_status = row.status
            result = await session.execute(
                update(WorkflowTask)
                .where(
                    WorkflowTask.task_id == row.task_id,
                    WorkflowTask.status == previous_status,
                )
                .values(
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
            )
            if self._rowcount(result) != 1:
                return None
            refreshed = await session.get(WorkflowTask, row.task_id)
            if refreshed is None:
                return None
            self._event(
                session,
                refreshed,
                f"manual_{action}",
                now,
                {
                    "action": action,
                    "from_status": previous_status,
                    "to_status": target,
                    "reason_digest": reason_digest,
                },
            )
            await self._refresh_run(session, run_id, now)
            return {
                "task_id": row.task_id,
                "stage": stage,
                "status": target,
                "action": action,
            }

    async def get_run(self, run_id: str) -> WorkflowRunRecord | None:
        async with self.session_maker() as session:
            row = await session.get(WorkflowRun, run_id)
            return self._run_record(row) if row is not None else None

    async def get_task(self, task_id: str) -> WorkflowTaskRecord | None:
        async with self.session_maker() as session:
            row = await session.get(WorkflowTask, task_id)
            return self._task_record(row) if row is not None else None

    async def get_manifest(self, task_id: str) -> WorkflowManifest | None:
        async with self.session_maker() as session:
            row = (
                await session.execute(
                    select(WorkflowTaskManifest).where(WorkflowTaskManifest.task_id == task_id)
                )
            ).scalar_one_or_none()
            return self._manifest_record(row) if row is not None else None

    async def list_events(self, *, task_id: str | None = None) -> list[WorkflowTaskEventRecord]:
        async with self.session_maker() as session:
            query = select(WorkflowTaskEvent).order_by(
                WorkflowTaskEvent.created_at, WorkflowTaskEvent.event_id
            )
            if task_id is not None:
                query = query.where(WorkflowTaskEvent.task_id == task_id)
            rows = (await session.execute(query)).scalars().all()
            return [
                WorkflowTaskEventRecord(
                    event_id=row.event_id,
                    task_id=row.task_id,
                    run_id=row.run_id,
                    stage=row.stage,
                    event_type=row.event_type,
                    payload=dict(row.payload_json),
                    created_at=row.created_at,
                )
                for row in rows
            ]
