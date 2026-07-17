"""演示数据库驱动的 CSV 处理任务

示例展示应用实例、任务配置、CPU 计算池和数据库调度记录的基本组合方式。
"""

import datetime
import math
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import cast

from fastapi import FastAPI
from pydantic import BaseModel
from sqlalchemy.dialects.mysql import insert as mysql_insert

from piko.app import PikoApp
from piko.infra.db import JobConfig, ScheduledJob, get_session, utcnow
from piko.infra.logging import get_logger


logger = get_logger(__name__)


class CsvProcessConfig(BaseModel):
    """定义 CSV 处理任务的参数"""

    file_path: str
    row_count: int


piko_app = PikoApp(name="simple_csv_worker")
api_app = piko_app.api_app


@asynccontextmanager
async def extended_lifespan(app_instance: FastAPI) -> AsyncIterator[None]:
    """在应用启动后写入示例调度配置"""
    async with piko_app.lifespan(app_instance):
        await ensure_seed_data()
        yield


api_app.router.lifespan_context = extended_lifespan


def heavy_calculation(row_id: int) -> str:
    """执行一个 CPU 密集型示例计算"""
    math.factorial(1000)
    return f"row_{row_id}_processed"


@piko_app.job(job_id="process_csv_job", schema=CsvProcessConfig)
async def csv_handler(ctx: dict[str, object], scheduled_time: datetime.datetime) -> None:
    """读取任务配置并并行处理 CSV 行"""
    config = cast(CsvProcessConfig, ctx["config"])
    run_id = ctx["run_id"]
    logger.info("csv_job_started", run_id=run_id, file_path=config.file_path)

    rows = range(config.row_count)
    results = await piko_app.cpu_manager.map_reduce(
        map_fn=heavy_calculation,
        items=rows,
        concurrency=4,
    )
    logger.info("csv_job_finished", run_id=run_id, processed=len(results))


async def ensure_seed_data() -> None:
    """写入示例任务的配置和调度记录"""
    async for session in get_session():
        config_payload = {"file_path": "/tmp/test_data.csv", "row_count": 100}
        config_stmt = mysql_insert(JobConfig).values(
            job_id="process_csv_job",
            config_json=config_payload,
            schema_version=1,
            version=1,
            updated_at=utcnow(),
        )
        await session.execute(
            config_stmt.on_duplicate_key_update(
                config_json=config_payload,
                updated_at=utcnow(),
            )
        )

        job_stmt = mysql_insert(ScheduledJob).values(
            job_id="process_csv_job",
            schedule_type="interval",
            schedule_expr='{"seconds": 10}',
            enabled=True,
            executor="cpu",
            max_instances=1,
            version=1,
            updated_at=utcnow(),
        )
        await session.execute(
            job_stmt.on_duplicate_key_update(
                enabled=True,
                schedule_expr='{"seconds": 10}',
                updated_at=utcnow(),
            )
        )
        await session.commit()
        logger.info("csv_job_seeded", interval_s=10)


if __name__ == "__main__":
    piko_app.run()
