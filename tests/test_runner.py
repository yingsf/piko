import datetime

import pytest
from sqlalchemy import select, delete

# 关键修改 1: 导入 db 模块本身，而不是导入 _session_maker 变量
import piko.infra.db as db_infra
from piko.core.registry import job
from piko.core.runner import job_runner
from piko.infra.db import JobRun, JobLock
from piko.infra.leader import get_leader_mutex


@pytest.mark.asyncio
async def test_runner_execution_flow():
    # 0. 准备环境
    db_infra.init_db()  # 此时会更新 db_infra._session_maker

    # 确保表存在
    await db_infra.create_all_tables()

    # 强行清理脏数据
    # 关键修改 2: 通过模块访问 _session_maker，确保拿到的是初始化后的对象
    async with db_infra._session_maker() as session:
        await session.execute(delete(JobRun))
        await session.execute(delete(JobLock))
        await session.commit()

    # 1. Mock Leader (假装自己是 Leader)
    leader = get_leader_mutex()
    leader._is_leader = True

    # 2. 注册任务
    run_flag = {"executed": False}

    @job(job_id="runner_test_job")
    async def test_handler(ctx, ts):
        run_flag["executed"] = True
        return "done"

    # 3. 触发执行
    now = datetime.datetime.now()
    await job_runner.run_job("runner_test_job", now)

    # 4. 验证结果
    assert run_flag["executed"] is True

    # 验证 DB 记录
    async with db_infra._session_maker() as session:
        # 查 JobRun
        result = await session.execute(select(JobRun).where(JobRun.job_id == "runner_test_job"))
        record = result.scalar_one_or_none()
        assert record is not None
        assert record.status == "SUCCESS"
        assert record.start_time is not None

        # 查 JobLock
        result = await session.execute(select(JobLock).where(JobLock.job_id == "runner_test_job"))
        lock = result.scalar_one_or_none()
        assert lock is not None

    # 5. 再次触发 (幂等性测试)
    run_flag["executed"] = False
    await job_runner.run_job("runner_test_job", now)

    # 锁冲突拦截
    assert run_flag["executed"] is False
