import math

from pydantic import BaseModel

from piko.app import PikoApp
from piko.infra.logging import get_logger

logger = get_logger(__name__)

# 初始化
app = PikoApp(name="simple_csv_worker")
api_app = app.api_app


# 业务逻辑定义
class CsvProcessConfig(BaseModel):
    file_path: str
    row_count: int


def heavy_calculation(row_id: int):
    """模拟 CPU 密集型计算任务"""
    # 仅执行计算以消耗 CPU 资源，不赋值给变量
    math.factorial(500)

    return f"row_{row_id}_processed"


@app.job(job_id="process_csv_job", schema=CsvProcessConfig)
async def csv_handler(ctx, _scheduled_time):
    """
    模拟处理一个 CSV 文件

    Args:
        ctx: 任务上下文
        _scheduled_time: 计划执行时间 (使用下划线前缀标识该参数在本函数中未被使用)
    """
    config: CsvProcessConfig = ctx["config"]
    run_id = ctx["run_id"]

    logger.info("business_logic_start", run_id=run_id, file=config.file_path)

    # 模拟读取文件行
    rows = list(range(config.row_count))

    # 调用 CPU MapReduce
    results = await app.cpu_manager.map_reduce(
        map_fn=heavy_calculation,
        items=rows,
        concurrency=10
    )

    logger.info("business_logic_done", count=len(results), sample=results[0])


# 运行入口
if __name__ == "__main__":
    app.run()
