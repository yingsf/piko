import datetime
import os

import pytest
from pydantic import BaseModel
from sqlalchemy import delete, insert, select

import piko.infra.db as db_infra
from piko import PikoApp
from piko.core.types import BackfillPolicy
from piko.infra.db import JobLock, JobRun, ScheduledJob, utcnow
from piko.infra.leader import LeaderMutex


pytestmark = pytest.mark.integration


class RequiredRunnerConfig(BaseModel):
    """验证 Runner 缺少 job_config 时必须触发校验"""

    required_value: int


@pytest.fixture(autouse=True)
def bypass_leader_fencing_for_database_tests(monkeypatch: pytest.MonkeyPatch) -> None:
    """隔离 Runner 集成测试的 fencing 查询，只验证任务执行语义"""

    async def valid_fencing(_: LeaderMutex) -> bool:
        return True

    monkeypatch.setattr(LeaderMutex, "verify_fencing_token", valid_fencing)


@pytest.mark.skipif(
    not os.getenv("PIKO_TEST_MYSQL_DSN"),
    reason="需要通过 PIKO_TEST_MYSQL_DSN 指定隔离测试数据库",
)
@pytest.mark.asyncio
async def test_runner_execution_flow(monkeypatch: pytest.MonkeyPatch) -> None:
    """验证应用实例中的 Runner 执行任务并记录结果"""
    app = PikoApp(name="runner-test")
    db_infra.init_db()
    await db_infra.create_all_tables()

    async with db_infra.get_session_context() as session:
        await session.execute(delete(JobRun))
        await session.execute(delete(JobLock))
        await session.commit()

    def always_leader(_: LeaderMutex) -> bool:
        return True

    monkeypatch.setattr(LeaderMutex, "is_leader", property(always_leader))
    run_flag = {"executed": False}

    @app.job(job_id="runner_test_job")
    async def test_handler(ctx: dict[str, object], ts: datetime.datetime) -> None:
        run_flag["executed"] = True

    assert app.registry.get_job("runner_test_job") is test_handler
    await app.runner.run_job("runner_test_job", datetime.datetime.now())

    assert run_flag["executed"] is True
    async with db_infra.get_session_context() as session:
        result = await session.execute(select(JobRun).where(JobRun.job_id == "runner_test_job"))
        record = result.scalar_one_or_none()
        assert record is not None
        assert record.status == "SUCCESS"
        assert record.start_time is not None


@pytest.mark.skipif(
    not os.getenv("PIKO_TEST_MYSQL_DSN"),
    reason="需要通过 PIKO_TEST_MYSQL_DSN 指定隔离测试数据库",
)
@pytest.mark.asyncio
async def test_runner_rejects_missing_required_job_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """验证有必填 Schema 但没有 job_config 时任务不会执行成功"""
    app = PikoApp(name="missing-job-config-test")
    db_infra.init_db()
    await db_infra.create_all_tables()
    job_id = "missing_job_config_test"
    scheduled_time = utcnow()

    async with db_infra.get_session_context() as session:
        await session.execute(delete(JobRun).where(JobRun.job_id == job_id))
        await session.execute(delete(JobLock).where(JobLock.job_id == job_id))
        await session.commit()

    monkeypatch.setattr(LeaderMutex, "is_leader", property(lambda _: True))
    executed = False

    @app.job(job_id=job_id, schema=RequiredRunnerConfig)
    async def required_config_handler(ctx: dict[str, object], ts: datetime.datetime) -> None:
        nonlocal executed
        executed = True

    assert app.registry.get_job(job_id) is required_config_handler
    await app.runner.run_job(job_id, scheduled_time)

    assert executed is False
    async with db_infra.get_session_context() as session:
        result = await session.execute(select(JobRun).where(JobRun.job_id == job_id))
        record = result.scalar_one()
        assert record.status == "FAILED"
        assert record.error_type == "ValidationError"


@pytest.mark.skipif(
    not os.getenv("PIKO_TEST_MYSQL_DSN"),
    reason="需要通过 PIKO_TEST_MYSQL_DSN 指定隔离测试数据库",
)
@pytest.mark.asyncio
async def test_expired_job_lock_is_recovered() -> None:
    """验证启动回收可以删除过期任务锁"""
    app = PikoApp(name="lock-recovery-test")
    db_infra.init_db()
    await db_infra.create_all_tables()

    job_id = "lock_recovery_test_job"
    scheduled_time = utcnow()
    async with db_infra.get_session_context() as session:
        await session.execute(delete(JobLock).where(JobLock.job_id == job_id))
        await session.execute(
            insert(JobLock).values(
                job_id=job_id,
                scheduled_time=scheduled_time,
                owner="dead-worker",
                owner_token="dead-token",
                acquired_at=scheduled_time - datetime.timedelta(minutes=10),
                expires_at=scheduled_time - datetime.timedelta(minutes=5),
            )
        )
        await session.commit()

    assert await app.runner.recover_expired_locks() == 1
    async with db_infra.get_session_context() as session:
        result = await session.execute(select(JobLock).where(JobLock.job_id == job_id))
        assert result.scalar_one_or_none() is None


@pytest.mark.skipif(
    not os.getenv("PIKO_TEST_MYSQL_DSN"),
    reason="需要通过 PIKO_TEST_MYSQL_DSN 指定隔离测试数据库",
)
@pytest.mark.asyncio
async def test_orphaned_job_run_is_marked_abandoned() -> None:
    """验证超时 RUNNING 记录会进入 ABANDONED 状态"""
    app = PikoApp(name="run-recovery-test")
    db_infra.init_db()
    await db_infra.create_all_tables()

    job_id = "run_recovery_test_job"
    scheduled_time = utcnow()
    async with db_infra.get_session_context() as session:
        await session.execute(delete(JobRun).where(JobRun.job_id == job_id))
        await session.execute(
            insert(JobRun).values(
                job_id=job_id,
                scheduled_time=scheduled_time,
                start_time=scheduled_time - datetime.timedelta(hours=1),
                status="RUNNING",
                attempt=1,
            )
        )
        await session.commit()

    assert await app.runner.recover_orphaned_runs() >= 1
    async with db_infra.get_session_context() as session:
        result = await session.execute(select(JobRun).where(JobRun.job_id == job_id))
        record = result.scalar_one()
        assert record.status == "ABANDONED"
        assert record.error_type == "OrphanedRun"


@pytest.mark.skipif(
    not os.getenv("PIKO_TEST_MYSQL_DSN"),
    reason="需要通过 PIKO_TEST_MYSQL_DSN 指定隔离测试数据库",
)
@pytest.mark.asyncio
async def test_successful_date_job_is_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    """验证一次性任务成功后不会被下一轮重新调度"""
    app = PikoApp(name="date-job-test")
    db_infra.init_db()
    await db_infra.create_all_tables()

    job_id = "date_job_test"
    scheduled_time = utcnow()
    async with db_infra.get_session_context() as session:
        await session.execute(delete(ScheduledJob).where(ScheduledJob.job_id == job_id))
        await session.execute(delete(JobRun).where(JobRun.job_id == job_id))
        await session.execute(
            insert(ScheduledJob).values(
                job_id=job_id,
                schedule_type="date",
                schedule_expr="{}",
                enabled=True,
                version=1,
            )
        )
        await session.commit()

    monkeypatch.setattr(LeaderMutex, "is_leader", property(lambda _: True))

    @app.job(job_id=job_id)
    async def date_handler(ctx: dict[str, object], ts: datetime.datetime) -> None:
        return None

    assert app.registry.get_job(job_id) is date_handler
    await app.runner.run_job(job_id, scheduled_time)

    async with db_infra.get_session_context() as session:
        result = await session.execute(select(ScheduledJob).where(ScheduledJob.job_id == job_id))
        row = result.scalar_one()
        assert row.enabled is False
        assert row.completed_at is not None


@pytest.mark.skipif(
    not os.getenv("PIKO_TEST_MYSQL_DSN"),
    reason="需要通过 PIKO_TEST_MYSQL_DSN 指定隔离测试数据库",
)
@pytest.mark.asyncio
async def test_stateful_success_commits_watermark(monkeypatch: pytest.MonkeyPatch) -> None:
    """验证有状态任务成功后提交水位线"""
    app = PikoApp(name="watermark-test")
    db_infra.init_db()
    await db_infra.create_all_tables()

    job_id = "watermark_test_job"
    scheduled_time = utcnow()
    initial_watermark = scheduled_time - datetime.timedelta(minutes=2)
    async with db_infra.get_session_context() as session:
        await session.execute(delete(ScheduledJob).where(ScheduledJob.job_id == job_id))
        await session.execute(delete(JobRun).where(JobRun.job_id == job_id))
        await session.execute(
            insert(ScheduledJob).values(
                job_id=job_id,
                schedule_type="cron",
                schedule_expr='{"cron": "* * * * *"}',
                is_stateful=True,
                last_data_time=initial_watermark,
                enabled=True,
                version=1,
            )
        )
        await session.commit()

    monkeypatch.setattr(LeaderMutex, "is_leader", property(lambda _: True))

    @app.job(job_id=job_id, stateful=True, backfill_policy=BackfillPolicy.SKIP)
    async def watermark_handler(ctx: dict[str, object], ts: datetime.datetime) -> None:
        return None

    assert app.registry.get_job(job_id) is watermark_handler
    await app.runner.run_job(job_id, scheduled_time)

    async with db_infra.get_session_context() as session:
        result = await session.execute(
            select(ScheduledJob.last_data_time).where(ScheduledJob.job_id == job_id)
        )
        watermark = result.scalar_one()
        assert watermark == scheduled_time
