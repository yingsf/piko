"""真实 MySQL 下的 PikoApp 启动、Worker 运行和统一停机契约。"""

from __future__ import annotations

import asyncio

import pytest

import piko.app as app_module
import piko.infra.db as db_infra
from piko import PikoApp
from piko.workflow.mysql_repository import MySQLWorkflowRepository
from piko.workflow.types import (
    BusinessResultStatus,
    TaskResult,
    TaskSpec,
    TaskStatus,
    WorkflowDefinition,
)

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_piko_app_real_startup_and_shutdown_uses_mysql_workflow_backend(
    mysql_backend, monkeypatch: pytest.MonkeyPatch
) -> None:
    """验证真实 schema、组件、Worker 和数据库清理贯穿同一生命周期。"""
    await db_infra.reset_db()
    monkeypatch.setattr(app_module.settings, "leader_enabled", False, raising=False)
    monkeypatch.setattr(app_module.settings, "cpu_workers", 1, raising=False)
    monkeypatch.setattr(app_module.settings, "poll_interval_s", 1, raising=False)
    monkeypatch.setattr(app_module.settings, "shutdown_timeout_s", 10, raising=False)
    app = PikoApp(name="mysql-lifecycle-contract")
    handled = asyncio.Event()

    @app.workflow("mysql-stage")
    async def handle_mysql_stage(_task):
        handled.set()
        return TaskResult(result_status=BusinessResultStatus.COMPLETE.value)

    try:
        await app.startup()
        assert app._started
        assert isinstance(app.workflow_repository, MySQLWorkflowRepository)
        assert app.watcher.is_running
        assert app.scheduler.is_running
        assert app.writer.is_running
        assert app.workflow_worker is not None
        assert app._workflow_worker_task is not None

        run = await app.create_workflow_run(
            WorkflowDefinition(
                "mysql-app-lifecycle",
                "mysql-app-lifecycle",
                (TaskSpec("mysql-stage", task_id="mysql-app-lifecycle-task"),),
            )
        )
        assert run.run_id
        await asyncio.wait_for(handled.wait(), timeout=2)
        repository = app.workflow_repository
        assert repository is not None
        for _ in range(100):
            task = await repository.get_task("mysql-app-lifecycle-task")
            if task is not None and task.status == TaskStatus.SUCCEEDED.value:
                break
            await asyncio.sleep(0.01)
        assert task is not None and task.status == TaskStatus.SUCCEEDED.value
    finally:
        await app.shutdown()

    assert not app.watcher.is_running
    assert not app.scheduler.is_running
    assert not app.writer.is_running
    assert app._workflow_worker_task is None
    with pytest.raises(RuntimeError, match="Database not initialized"):
        db_infra.get_session_maker()
