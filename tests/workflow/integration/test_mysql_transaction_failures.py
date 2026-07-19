from __future__ import annotations

import asyncio
from collections.abc import Callable

import pytest
from sqlalchemy import event, text

from tests.workflow.conftest import NOW, claim_time
from piko.workflow.types import TaskResult, TaskSpec, OwnershipLostError, WorkflowDefinition

pytestmark = pytest.mark.integration


def _install_after_statement_failure(engine, predicate: Callable[[str, object], bool]):
    """在指定 SQL 已执行后注入一次异常，模拟提交前的事务故障。"""
    fired = False

    def fail_after_execute(_connection, _cursor, statement, parameters, _context, _executemany):
        nonlocal fired
        if not fired and predicate(statement, parameters):
            fired = True
            raise RuntimeError("injected post-statement transaction failure")

    event.listen(engine.sync_engine, "after_cursor_execute", fail_after_execute)
    return fail_after_execute


async def _claimed_task(backend, workflow_id: str, idempotency_key: str):
    await backend.create_run(
        WorkflowDefinition(workflow_id, idempotency_key, (TaskSpec("stage"),)), now=NOW
    )
    return (
        await backend.claim_ready_tasks(
            worker_id="worker-a", stages=["stage"], lease_until=claim_time(), now=NOW, limit=1
        )
    )[0]


@pytest.mark.asyncio
async def test_mysql_finalization_hook_failure_rolls_back_manifest_and_task(mysql_backend):
    backend, _, _ = mysql_backend
    await backend.create_run(
        WorkflowDefinition("mysql", "transaction", (TaskSpec("stage"),)), now=NOW
    )
    task = (
        await backend.claim_ready_tasks(
            worker_id="a", stages=["stage"], lease_until=claim_time(), now=NOW, limit=1
        )
    )[0]

    async def failing_hook(session):
        await session.execute(
            text("UPDATE workflow_run SET business_result_status='partial' WHERE run_id=:run_id"),
            {"run_id": task.run_id},
        )
        raise RuntimeError("injected transaction failure")

    with pytest.raises(RuntimeError):
        await backend.finalize_task(
            task=task,
            result=TaskResult(result_status="complete"),
            now=NOW,
            business_hook=failing_hook,
        )
    assert (await backend.get_task(task.task_id)).status == "running"
    assert await backend.get_manifest(task.task_id) is None


@pytest.mark.asyncio
async def test_mysql_stale_worker_cannot_leave_manifest(mysql_backend):
    backend, _, _ = mysql_backend
    await backend.create_run(WorkflowDefinition("mysql", "stale", (TaskSpec("stage"),)), now=NOW)
    first = (
        await backend.claim_ready_tasks(
            worker_id="a", stages=["stage"], lease_until=claim_time(1), now=NOW, limit=1
        )
    )[0]
    await backend.recover_expired_running_tasks(now=claim_time(2))
    await backend.recover_retry_waiting_tasks(now=claim_time(2))
    second = (
        await backend.claim_ready_tasks(
            worker_id="b", stages=["stage"], lease_until=claim_time(60), now=claim_time(2), limit=1
        )
    )[0]
    with pytest.raises(OwnershipLostError):
        await backend.finalize_task(
            task=first, result=TaskResult(result_status="complete"), now=claim_time(2)
        )
    assert await backend.get_manifest(first.task_id) is None
    assert await backend.finalize_task(
        task=second, result=TaskResult(result_status="complete"), now=claim_time(3)
    )


@pytest.mark.asyncio
async def test_mysql_event_insert_failure_rolls_back_finalization(mysql_backend):
    backend, _, engine = mysql_backend
    await backend.create_run(
        WorkflowDefinition("mysql", "event-failure", (TaskSpec("stage"),)), now=NOW
    )
    task = (
        await backend.claim_ready_tasks(
            worker_id="a", stages=["stage"], lease_until=claim_time(), now=NOW, limit=1
        )
    )[0]
    sync_engine = engine.sync_engine

    def fail_before_execute(_connection, _cursor, statement, _parameters, _context, _executemany):
        if "INSERT INTO workflow_task_event" in statement:
            raise RuntimeError("injected event failure")

    event.listen(sync_engine, "before_cursor_execute", fail_before_execute)
    try:
        with pytest.raises(RuntimeError):
            await backend.finalize_task(
                task=task, result=TaskResult(result_status="complete"), now=NOW
            )
    finally:
        event.remove(sync_engine, "before_cursor_execute", fail_before_execute)
    assert (await backend.get_task(task.task_id)).status == "running"
    assert await backend.get_manifest(task.task_id) is None


@pytest.mark.asyncio
async def test_mysql_manifest_write_failure_after_insert_rolls_back(mysql_backend):
    backend, _, engine = mysql_backend
    task = await _claimed_task(backend, "mysql", "manifest-after-write")
    listener = _install_after_statement_failure(
        engine,
        lambda statement, _parameters: "insert into workflow_task_manifest" in statement.lower(),
    )
    try:
        with pytest.raises(RuntimeError, match="post-statement"):
            await backend.finalize_task(
                task=task, result=TaskResult(result_status="complete"), now=NOW
            )
    finally:
        event.remove(engine.sync_engine, "after_cursor_execute", listener)

    current = await backend.get_task(task.task_id)
    assert current is not None and current.status == "running"
    assert await backend.get_manifest(task.task_id) is None
    assert not [
        event
        for event in await backend.list_events(task_id=task.task_id)
        if event.event_type == "finalize"
    ]


@pytest.mark.asyncio
async def test_mysql_task_state_update_failure_rolls_back_finalization(mysql_backend):
    backend, _, engine = mysql_backend
    task = await _claimed_task(backend, "mysql", "task-update-failure")
    listener = _install_after_statement_failure(
        engine,
        lambda statement, parameters: (
            "update workflow_task set" in statement.lower()
            and "succeeded" in str(parameters).lower()
        ),
    )
    try:
        with pytest.raises(RuntimeError, match="post-statement"):
            await backend.finalize_task(
                task=task, result=TaskResult(result_status="complete"), now=NOW
            )
    finally:
        event.remove(engine.sync_engine, "after_cursor_execute", listener)

    current = await backend.get_task(task.task_id)
    assert current is not None and current.status == "running"
    assert await backend.get_manifest(task.task_id) is None


@pytest.mark.asyncio
async def test_mysql_run_state_update_failure_rolls_back_finalization(mysql_backend):
    backend, _, engine = mysql_backend
    task = await _claimed_task(backend, "mysql", "run-update-failure")
    listener = _install_after_statement_failure(
        engine,
        lambda statement, _parameters: "update workflow_run set" in statement.lower(),
    )
    try:
        with pytest.raises(RuntimeError, match="post-statement"):
            await backend.finalize_task(
                task=task, result=TaskResult(result_status="complete"), now=NOW
            )
    finally:
        event.remove(engine.sync_engine, "after_cursor_execute", listener)

    current = await backend.get_task(task.task_id)
    run = await backend.get_run(task.run_id)
    assert current is not None and current.status == "running"
    assert run is not None and run.status == "running"
    assert await backend.get_manifest(task.task_id) is None


@pytest.mark.asyncio
async def test_mysql_committed_finalize_is_idempotent_when_worker_loses_response(mysql_backend):
    backend, _, _ = mysql_backend
    task = await _claimed_task(backend, "mysql", "lost-response")

    async def finalize_without_response() -> None:
        assert await backend.finalize_task(
            task=task,
            result=TaskResult(
                result_status="complete", result_payload={"business_id": "lost-response"}
            ),
            now=NOW,
        )
        raise asyncio.TimeoutError("response lost after commit")

    with pytest.raises(asyncio.TimeoutError, match="response lost"):
        await finalize_without_response()
    assert (
        await backend.finalize_task(
            task=task,
            result=TaskResult(
                result_status="complete", result_payload={"business_id": "lost-response"}
            ),
            now=NOW,
        )
        is False
    )
    manifest = await backend.get_manifest(task.task_id)
    assert manifest is not None and manifest.result_payload == {"business_id": "lost-response"}
    assert [
        item
        for item in await backend.list_events(task_id=task.task_id)
        if item.event_type == "finalize"
    ].__len__() == 1


@pytest.mark.asyncio
async def test_mysql_retry_reclaim_and_business_manifest_write_are_idempotent(mysql_backend):
    backend, _, _ = mysql_backend
    task = await _claimed_task(backend, "mysql", "retry-business-write")
    assert await backend.retry_task(
        task_id=task.task_id,
        owner=task.owner or "",
        lock_token=task.lock_token or "",
        error_code="transient",
        error_message="retry once",
        available_at=NOW,
        now=NOW,
    )
    assert await backend.recover_retry_waiting_tasks(now=NOW) == 1
    retried = (
        await backend.claim_ready_tasks(
            worker_id="worker-b", stages=["stage"], lease_until=claim_time(), now=NOW, limit=1
        )
    )[0]
    result = TaskResult(
        result_status="complete", result_payload={"business_id": "retry-business-write"}
    )
    assert await backend.finalize_task(task=retried, result=result, now=NOW)
    assert await backend.finalize_task(task=retried, result=result, now=NOW) is False
    manifest = await backend.get_manifest(task.task_id)
    assert manifest is not None and manifest.result_payload == result.result_payload
    assert [
        item
        for item in await backend.list_events(task_id=task.task_id)
        if item.event_type == "finalize"
    ].__len__() == 1
