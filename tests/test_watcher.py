import json

import pytest
from sqlalchemy import insert, delete

import piko.infra.db as db_infra
from piko.core.registry import job
from piko.core.scheduler import scheduler_manager
from piko.core.watcher import config_watcher
from piko.infra.db import ScheduledJob, JobConfig


@pytest.mark.asyncio
async def test_watcher_reconcile():
    # 0. 准备环境
    db_infra.init_db()
    await db_infra.create_all_tables()

    # 注册一个白名单任务 (否则 Watcher 会忽略)
    @job(job_id="watcher_test_job")
    async def noop(ctx, ts):
        pass

    # 1. 插入测试数据到 DB
    async with db_infra._session_maker() as session:
        # 清理
        await session.execute(delete(ScheduledJob))
        await session.execute(delete(JobConfig))

        # 插入 Job (每1秒运行一次)
        stmt_job = insert(ScheduledJob).values(
            job_id="watcher_test_job",
            schedule_type="interval",
            schedule_expr=json.dumps({"seconds": 1}),
            enabled=True,
            version=1
        )
        # 插入 Config
        stmt_cfg = insert(JobConfig).values(
            job_id="watcher_test_job",
            config_json={"foo": "bar"},
            version=1
        )
        await session.execute(stmt_job)
        await session.execute(stmt_cfg)
        await session.commit()

    # 2. 启动 Scheduler 和 Watcher
    scheduler_manager.start()
    await config_watcher.start()

    # 3. 等待 Reconcile (poll_interval 默认可能较长，测试中为了快，可以 hack 一下 interval)
    # 但由于我们用了 settings，为了测试稳定性，我们直接手动触发一次 reconcile
    await config_watcher._reconcile()

    # 4. 验证
    # 验证 Job 是否加载进 Scheduler
    ap_job = scheduler_manager.raw_scheduler.get_job("watcher_test_job")
    assert ap_job is not None
    assert str(ap_job.trigger).startswith("interval")

    # 验证 Config 是否加载进 Cache
    from piko.core.cache import config_cache
    cached = config_cache.get("watcher_test_job")
    assert cached is not None
    assert cached.config_json == {"foo": "bar"}

    # 5. 测试动态更新 (Disable Job)
    async with db_infra._session_maker() as session:
        # update enabled=False
        from sqlalchemy import update
        await session.execute(
            update(ScheduledJob)
            .where(ScheduledJob.job_id == "watcher_test_job")
            .values(enabled=False)
        )
        await session.commit()

    # 再次 reconcile
    await config_watcher._reconcile()

    # 验证 Job 是否被移除
    ap_job = scheduler_manager.raw_scheduler.get_job("watcher_test_job")
    assert ap_job is None

    # 清理
    await config_watcher.stop()
    scheduler_manager.shutdown()
