import math
from contextlib import asynccontextmanager

from pydantic import BaseModel
from sqlalchemy.dialects.mysql import insert as mysql_insert

from piko.app import PikoApp
from piko.infra.db import ScheduledJob, JobConfig
from piko.infra.db import get_session, utcnow
from piko.infra.logging import get_logger


logger = get_logger(__name__)


# =============================================================================
# 1. 定义业务配置 Schema
# =============================================================================
class CsvProcessConfig(BaseModel):
    file_path: str
    row_count: int


# =============================================================================
# 2. 定义 Piko App 及其扩展启动逻辑
# =============================================================================

# 定义一个包装后的 lifespan，用于在 Piko 启动后自动插入测试数据
@asynccontextmanager
async def extended_lifespan(app_instance):
    # 1. 先执行 Piko 标准启动流程 (启动 DB, Scheduler, Worker 等)
    async with piko_app.lifespan(app_instance):
        # 2. Piko 启动就绪后，插入测试种子数据
        await ensure_seed_data()
        yield


piko_app = PikoApp(name="simple_csv_worker")
# 替换默认的 lifespan，注入我们的测试数据初始化逻辑
piko_app.api_app.router.lifespan_context = extended_lifespan
api_app = piko_app.api_app


# =============================================================================
# 3. 业务逻辑 (MapReduce)
# =============================================================================
def heavy_calculation(row_id: int):
    """模拟 CPU 密集型计算任务 (运行在子进程)"""
    # 模拟耗时计算
    math.factorial(1000)
    return f"row_{row_id}_processed"


@piko_app.job(job_id="process_csv_job", schema=CsvProcessConfig)
async def csv_handler(ctx, _scheduled_time):
    """
    模拟处理一个 CSV 文件
    """
    config: CsvProcessConfig = ctx["config"]
    run_id = ctx["run_id"]

    logger.info(f"[JobStart] RunID={run_id} File={config.file_path}")

    # 模拟待处理数据
    rows = list(range(config.row_count))

    # 调用 CPU MapReduce (Piko写法: 必须通过 app.cpu_manager 调用)
    # 注意：simple_worker.py 是单文件，无法使用 auto_discover，但 @app.job 装饰器在 import 时已经完成了注册，所以没问题
    results = await piko_app.cpu_manager.map_reduce(
        map_fn=heavy_calculation,
        items=rows,
        # 本地测试给小一点的并发
        concurrency=4
    )

    logger.info(f"[JobDone] Processed {len(results)} rows. Sample: {results[0]}")


# =============================================================================
# 4. 测试数据自动初始化工具
# =============================================================================
async def ensure_seed_data():
    """自动插入测试所需的调度配置和任务参数"""
    logger.info("Checking seed data for 'process_csv_job'...")

    async for session in get_session():
        # 1. 插入/更新任务配置 (JobConfig)
        # 对应 CsvProcessConfig 的结构
        config_payload = {
            "file_path": "/tmp/test_data.csv",
            "row_count": 100
        }

        stmt_config = mysql_insert(JobConfig).values(
            job_id="process_csv_job",
            config_json=config_payload,
            schema_version=1,
            version=1,
            updated_at=utcnow()
        )
        # 如果存在则更新配置
        stmt_config = stmt_config.on_duplicate_key_update(
            config_json=config_payload,
            updated_at=utcnow()
        )
        await session.execute(stmt_config)

        # 2. 插入/更新调度规则 (ScheduledJob)
        # 设置为每 10 秒执行一次
        stmt_job = mysql_insert(ScheduledJob).values(
            job_id="process_csv_job",
            schedule_type="interval",
            # Interval 触发器
            schedule_expr='{"seconds": 10}',
            enabled=True,
            executor="cpu",
            max_instances=1,
            version=1,
            updated_at=utcnow()
        )
        # 如果存在则确保它是启用状态
        stmt_job = stmt_job.on_duplicate_key_update(
            enabled=True,
            schedule_expr='{"seconds": 10}',
            updated_at=utcnow()
        )
        await session.execute(stmt_job)

        await session.commit()
        logger.info("Seed data injected: Job runs every 10s.")


if __name__ == "__main__":
    piko_app.run()
