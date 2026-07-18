import json
import os

import pytest
from sqlalchemy import delete, insert

import piko.infra.db as db_infra
from piko import PikoApp
from piko.infra.db import JobConfig, ScheduledJob
from piko.infra.leader import LeaderMutex


pytestmark = pytest.mark.integration


@pytest.mark.skipif(
    not os.getenv("PIKO_TEST_MYSQL_DSN"),
    reason="需要通过 PIKO_TEST_MYSQL_DSN 指定隔离测试数据库",
)
@pytest.mark.asyncio
async def test_watcher_reconcile(monkeypatch: pytest.MonkeyPatch) -> None:
    """验证应用实例中的 Watcher 同步数据库配置"""
    app = PikoApp(name="watcher-test")
    db_infra.init_db()
    await db_infra.create_all_tables()

    @app.job(job_id="watcher_test_job")
    async def noop(ctx: dict[str, object], ts: object) -> None:
        return None

    assert app.registry.get_job("watcher_test_job") is noop
    monkeypatch.setattr(LeaderMutex, "is_leader", property(lambda _: True))

    async with db_infra.get_session_context() as session:
        await session.execute(delete(ScheduledJob))
        await session.execute(delete(JobConfig))
        await session.execute(
            insert(ScheduledJob).values(
                job_id="watcher_test_job",
                schedule_type="interval",
                schedule_expr=json.dumps({"seconds": 1}),
                timezone="UTC",
                misfire_grace_s=17,
                coalesce=False,
                max_instances=2,
                jitter_s=3,
                executor="io",
                enabled=True,
                version=1,
            )
        )
        await session.execute(
            insert(JobConfig).values(
                job_id="watcher_test_job",
                config_json={"foo": "bar"},
                version=1,
            )
        )
        await session.commit()

    app.scheduler.startup()
    await app.watcher.start()
    await app.watcher.reconcile_once()

    scheduled = app.scheduler.raw_scheduler.get_job("watcher_test_job")
    assert scheduled is not None
    assert scheduled.name == "v1"
    assert scheduled.executor == "io"
    assert scheduled.misfire_grace_time == 17
    assert scheduled.coalesce is False
    assert scheduled.max_instances == 2
    assert str(scheduled.trigger.timezone) == "UTC"
    assert scheduled.trigger.jitter == 3

    cached = app.config_cache.get("watcher_test_job")
    assert cached is not None
    assert cached.config_json == {"foo": "bar"}

    async with db_infra.get_session_context() as session:
        # 通过 ORM 实体更新（而非 Core update），使 SQLAlchemy 的 onupdate
        # 自动刷新 updated_at。watcher 的增量同步以 updated_at >= last_sync
        # 为边界，Core update 不会触发 onupdate，会导致变更被增量过滤漏掉。
        # 真实运维中配置变更通常经 ORM 或显式带 updated_at，此处与之对齐。
        job = await session.get(ScheduledJob, "watcher_test_job")
        assert job is not None
        job.enabled = False
        await session.commit()

    await app.watcher.reconcile_once()
    assert app.scheduler.raw_scheduler.get_job("watcher_test_job") is None

    await app.watcher.stop()
    app.scheduler.shutdown()
