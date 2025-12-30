# Piko: Data-Oriented Async Task Orchestrator

[![PyPI version](https://img.shields.io/pypi/v/piko.svg)](https://pypi.org/project/piko-cucc/)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**Piko** 是一个专为数据工程设计的微内核异步任务编排框架。它不仅仅是一个定时任务调度器，更是一个基于 `asyncio` 的高并发流水线引擎。

与传统调度器不同，Piko 旨在解决**高并发 I/O** 与**复杂资源管理**之间的矛盾，通过**微内核设计**与**依赖注入**机制，让开发者能够轻松构建支撑数万 QPS 的数据抓取、清洗与同步服务。

---

## 前置要求 (Prerequisites)

Piko 依赖 **MySQL** (5.7 或 8.0+) 作为核心组件，用于存储任务元数据、状态回填进度以及实现分布式锁。

在启动 Piko 之前，请确保您拥有一个可用的 MySQL 实例，并创建好数据库。

```sql
# 示例：创建数据库
CREATE DATABASE piko_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
```

## 核心特性 (Key Features)

- **异步微内核 (Async Micro-kernel)**: 基于 `asyncio` + `uvloop` (可选) 构建，原生支持协程，单节点即可处理数万并发任务。
- **资源依赖注入 (Dependency Injection)**: 告别全局变量与连接泄露。通过 `@job(resources=...)` 声明依赖，框架自动在并发任务间管理连接池的借用与归还。
- **智能回填 (Stateful Backfill)**: 系统停机或逻辑错误？Piko 会自动计算数据窗口（Data Interval），精准补录每一份丢失的数据。
- **类型安全 Sink (Typed Sinks)**: 基于 Python 类型系统的自动路由分发，让异构数据的写入逻辑清晰可维护。

------

## 📦 安装 (Installation)

```bash
pip install piko-cucc
```

------

## 🚀 架构范式 (Architectural Patterns)

Piko 的强大之处在于其对**异步**与**并发**的原生支持。以下三个例子展示了 Piko 在不同场景下的最佳实践。

### 场景一：高并发网络 I/O (The "C10K" Crawler)

在这个场景中，我们需要极高频地抓取 API。Piko 利用 `asyncio` 的非阻塞特性，可以在单个进程内同时挂起数千个网络请求，最大化 I/O 吞吐量。

```python
import asyncio
import aiohttp
from piko.core.registry import job
from piko.core.runner import job_runner

# 1. 定义一个高频任务
@job(
    job_id="fetch_stock_price",
    cron="* * * * *",       # 每分钟触发
    misfire_grace_time=10   # 允许一定的延迟
)
async def fetch_handler(ctx, scheduled_time):
    """
    这是一个纯异步的 Handler。
    Piko 不会因为 await 而阻塞，它会立即切换去执行其他任务。
    """
    symbol = ctx["config"].get("symbol", "AAPL")
    
    async with aiohttp.ClientSession() as session:
        async with session.get(f"[https://api.stocks.com/](https://api.stocks.com/){symbol}") as resp:
            data = await resp.json()
            print(f"[{symbol}] Price: {data['price']} at {scheduled_time}")

# 2. 模拟高并发触发
# 在生产环境中，Piko Runner 会自动调度。
# 这里演示如何手动触发 1000 个并发任务。
async def main():
    # 瞬间生成 1000 个协程任务
    tasks = [
        job_runner.run_job("fetch_stock_price", config={"symbol": f"STK_{i}"}) 
        for i in range(1000)
    ]
    # Piko 轻松处理并发
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
```

### 场景二：受控并发与资源池化 (The "Shared Pool" Pattern)

当并发量很大时，数据库连接往往成为瓶颈。Piko 的 **资源注入 (DI)** 机制能确保成千上万个任务共享一个有限的连接池，既保证了并发度，又防止了数据库被打挂。

```python
from contextlib import asynccontextmanager
from piko.core.resource import Resource
from piko.core.registry import job

# 1. 定义资源：一个带有连接池的数据库客户端
class DBPoolResource(Resource):
    def __init__(self):
        # 假设这是一个连接池，最大连接数 50
        self.pool = MyAsyncDBPool(max_size=50)

    @asynccontextmanager
    async def acquire(self, ctx):
        # 当任务执行时，从池中借出一个连接
        async with self.pool.acquire() as conn:
            yield conn
        # 任务结束，连接自动归还回池中

# 2. 注册任务：声明我需要 "db" 资源
@job(
    job_id="heavy_etl_task",
    cron="*/5 * * * *",
    resources={"db": DBPoolResource}  # <-- 注入声明
)
async def etl_handler(ctx, scheduled_time, db):
    """
    db参数: 不是整个连接池，而是一个已经 connected 的连接对象。
    
    即便 Piko 同时拉起了 5000 个 etl_handler，
    由于 DBPoolResource 的限制，它们会排队复用那 50 个数据库连接，
    实现了"高并发调度"与"有限资源保护"的完美平衡。
    """
    await db.execute("INSERT INTO logs ...")
    print("Write success")
```

#### 场景三：CPU 密集型任务卸载 (The "Multiprocessing" Pattern)

当业务包含复杂的数学计算、图像处理或超大文件解析（例如解压 1GB 的 gzip 文件）时，直接在 Handler 中运行会阻塞 Piko 的事件循环（Event Loop），导致心跳超时。

标准做法是将这些“重活”卸载到 **进程池 (Process Pool)** 中。

```python
import asyncio
from concurrent.futures import ProcessPoolExecutor
from piko.core.resource import Resource
from piko.core.registry import job

# 1. 定义纯函数 (必须是顶层函数，以便 Pickle 序列化)
def heavy_calculation(data_chunk: bytes) -> int:
    """模拟一个耗时 10 秒的 CPU 密集型计算"""
    # 比如：图像转码、加解密、复杂数据清洗
    import time
    time.sleep(10) # 模拟 CPU 满载
    return len(data_chunk)

# 2. 定义资源：进程池
class CpuPoolResource(Resource):
    def __init__(self):
        # 创建一个包含 4 个工人的进程池
        self.pool = ProcessPoolExecutor(max_workers=4)

    async def acquire(self, ctx):
        # 将池子本身交给 Handler
        yield self.pool
        # Piko 退出时不需要手动 shutdown，Python 解释器会处理，
        # 或者在这里实现更优雅的关闭逻辑

# 3. 注册任务：注入 CPU 资源
@job(
    job_id="process_large_file",
    cron="0 0 * * *",
    resources={"cpu_pool": CpuPoolResource}
)
async def data_mining_handler(ctx, scheduled_time, cpu_pool):
    """
    注意：Handler 本身依然是异步的，但他通过 run_in_executor 将
    计算任务“扔”给了子进程。
    """
    loop = asyncio.get_running_loop()
    
    # 模拟读取数据
    huge_data = b"0" * 1024 * 1024 * 100 

    print(f"[{scheduled_time}] Start calculation...")
    
    # 关键点：await run_in_executor
    # 1. Piko 主线程立即释放控制权，继续处理心跳和其他短任务
    # 2. heavy_calculation 在独立的子进程中运行，独占一个 CPU 核心
    # 3. 计算完成后，结果自动传回这里
    result = await loop.run_in_executor(
        cpu_pool, 
        heavy_calculation, 
        huge_data
    )
    
    print(f"Calculation done. Result: {result}")
```

通过这三个场景，Piko 覆盖了数据工程的完整场景：

1. **I/O 密集** → 用 `asyncio` 原生并发（场景一）。
2. **资源受限** → 用 `Resource` 注入实现池化管理（场景二）。
3. **CPU 密集** → 用 `run_in_executor` + 注入进程池实现计算卸载（场景三）。

------

## 配置 (Configuration)

Piko 采用分层配置策略，优先级顺序为：**环境变量 > `settings.toml` 配置文件 > 默认值**。

### 1. 配置文件 (`settings.toml`)

项目根目录下的 `settings.toml` 是推荐的配置方式。

```toml
[piko]
log_level = "INFO"
log_json = false  # 开发模式开启彩色日志

[mysql]
dsn = "mysql+aiomysql://user:pass@localhost/piko_db"
pool_size = 20
pool_recycle = 3600
```

### 2. 环境变量 (Environment Variables)

推荐在 Docker/Kubernetes 中使用。所有变量必须以 `PIKO_` 开头，并使用双下划线 `__` 分隔层级。

**示例映射：**

| TOML 配置          | 对应的环境变量    | 说明                    |
| ------------------ | ----------------- | ----------------------- |
| `[mysql] dsn`      | `PIKO_MYSQL__DSN` | **[必须]** MySQL 连接串 |
| `[piko] log_level` | `PIKO_LOG_LEVEL`  | 日志级别 (DEBUG/INFO)   |
| `[piko] log_json`  | `PIKO_LOG_JSON`   | 生产环境建议设为 `true` |
