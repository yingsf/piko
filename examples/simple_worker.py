import asyncio
import os
from pydantic import BaseModel

# [修改 1] 使用 Piko 统一封装的 logger，确保日志格式统一
from piko.infra.logging import get_logger

# 1. 导入 Piko 核心组件
from piko.core.registry import job
from piko.app import api_app  # 导出 FastAPI 实例供 uvicorn 使用
from piko.compute.manager import cpu_manager

logger = get_logger(__name__)


# 定义配置模型 (Design 9.2)
class CsvProcessConfig(BaseModel):
    file_path: str
    row_count: int


# 定义一个纯 CPU 函数 (将被 pickle 发送到子进程)
def heavy_calculation(row_id: int):
    import math
    # 模拟 CPU 耗时
    val = math.factorial(500)
    return f"row_{row_id}_processed"


# 2. 注册业务任务
@job(job_id="process_csv_job", schema=CsvProcessConfig)
async def csv_handler(ctx, scheduled_time):
    """
    模拟处理一个 CSV 文件：
    1. 接收参数
    2. 分发 CPU 计算 (MapReduce)
    3. 聚合结果
    """
    config: CsvProcessConfig = ctx["config"]
    run_id = ctx["run_id"]

    logger.info("business_logic_start", run_id=run_id, file=config.file_path)

    # 模拟读取文件行
    rows = list(range(config.row_count))

    # 3. 调用 CPU MapReduce (Design 6)
    results = await cpu_manager.map_reduce(
        map_fn=heavy_calculation,
        items=rows,
        # [修改 2] 核心修复：chunk_size 已废弃，改为 concurrency
        # concurrency=10 意味着同时最多有 10 个子任务在并行跑，
        # 完成一个补一个，而不是之前的凑齐 10 个一批。
        concurrency=10
    )

    logger.info("business_logic_done", count=len(results), sample=results[0])

    # 这里可以继续调用 persistence_writer 写入结果
    # await persistence_writer.enqueue(...)