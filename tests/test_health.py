"""验证内置健康检查和 Prometheus 端点的 HTTP 契约"""

import json
import asyncio
import time
from datetime import datetime
from collections.abc import Callable
from typing import Any, cast

import pytest
from fastapi.routing import APIRoute

import piko.app as app_module
from piko import PikoApp
from piko.core.scheduler import SchedulerManager
from piko.core.watcher import ConfigWatcher
from piko.persistence.writer import PersistenceWriter
from piko.workflow.repository import InMemoryWorkflowRepository
from piko.workflow.worker import WorkflowWorker, WorkflowWorkerConfig


def _endpoint_for(app: PikoApp, path: str) -> Callable[[], Any]:
    """获取指定路径的无参数 API 处理函数"""
    for route in app.api_app.routes:
        if isinstance(route, APIRoute) and route.path == path:
            return cast(Callable[[], Any], route.endpoint)
    raise AssertionError(f"route not found: {path}")


async def _database_is_unavailable() -> bool:
    """模拟健康检查时数据库不可用"""
    return False


async def _database_is_available() -> bool:
    """模拟健康检查时数据库可用"""
    return True


async def test_healthz_is_liveness_only() -> None:
    """验证数据库未启动时 liveness 仍表示进程存活"""
    app = PikoApp(name="health-test")
    endpoint = _endpoint_for(app, "/healthz")

    response = endpoint()

    assert response.status_code == 200
    assert json.loads(bytes(response.body)) == {"status": "ok", "shutdown": False}


def test_operational_docs_are_disabled_by_default() -> None:
    """验证默认不暴露 FastAPI 文档和 OpenAPI schema"""
    app = PikoApp(name="docs-security-test")
    paths = {route.path for route in app.api_app.routes if isinstance(route, APIRoute)}

    assert "/docs" not in paths
    assert "/redoc" not in paths
    assert "/openapi.json" not in paths


async def test_readyz_returns_503_when_components_are_not_ready(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """验证未启动或数据库不可用时 readiness 返回 503"""
    monkeypatch.setattr(app_module, "check_database_connection", _database_is_unavailable)
    app = PikoApp(name="readiness-test")
    endpoint = _endpoint_for(app, "/readyz")

    response = await endpoint()
    body = json.loads(bytes(response.body))

    assert response.status_code == 503
    assert body["ready"] is False
    assert body["checks"]["database"] is False


@pytest.mark.asyncio
async def test_readyz_returns_503_when_workflow_worker_exits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """验证工作流 Worker 退出后 readiness 不再报告 ready"""
    monkeypatch.setattr(app_module, "check_database_connection", _database_is_available)
    monkeypatch.setattr(app_module.settings, "leader_enabled", False, raising=False)
    app = PikoApp(name="workflow-worker-readiness-test")
    component = type("RunningComponent", (), {"is_running": True})()
    app._started = True
    app.writer = cast(PersistenceWriter, component)
    app.watcher = cast(ConfigWatcher, component)
    app.scheduler = cast(SchedulerManager, component)
    app.workflow_worker = WorkflowWorker(
        backend=InMemoryWorkflowRepository(),
        handlers={},
        config=WorkflowWorkerConfig(worker_id="readiness-test"),
        now=datetime.now,
    )
    worker_started = asyncio.Event()
    worker_stop = asyncio.Event()

    async def workflow_worker_loop() -> None:
        worker_started.set()
        await worker_stop.wait()

    app._workflow_worker_task = asyncio.create_task(workflow_worker_loop())
    await worker_started.wait()
    endpoint = _endpoint_for(app, "/readyz")

    response = await endpoint()
    body = json.loads(bytes(response.body))

    assert response.status_code == 200
    assert body["ready"] is True
    assert body["checks"]["workflow_worker"] is True

    worker_stop.set()
    await app._workflow_worker_task
    response = await endpoint()
    body = json.loads(bytes(response.body))

    assert response.status_code == 503
    assert body["ready"] is False
    assert body["checks"]["workflow_worker"] is False


async def test_metrics_route_returns_prometheus_response() -> None:
    """验证 metrics 路由直接返回 Prometheus Response"""
    app = PikoApp(name="metrics-test")
    endpoint = _endpoint_for(app, "/metrics")

    response = endpoint()

    assert response.status_code == 200
    assert b"piko_job_run_total" in response.body


@pytest.mark.asyncio
async def test_shutdown_obeys_total_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    """验证组件关闭被总停机预算限制"""
    app = PikoApp(name="shutdown-timeout-test")

    async def blocked_shutdown(_deadline: float | None = None) -> None:
        """模拟无法及时结束的组件关闭"""
        await asyncio.sleep(10)

    monkeypatch.setattr(app, "_shutdown_components", blocked_shutdown)
    monkeypatch.setattr(app_module.settings, "shutdown_timeout_s", 0.01, raising=False)
    started_at = time.monotonic()

    await app.shutdown()

    assert time.monotonic() - started_at < 1
