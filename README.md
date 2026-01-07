# Piko - Data-Oriented Async Task Orchestrator

[![PyPI version](https://img.shields.io/pypi/v/piko-cucc.svg)](https://pypi.org/project/piko-cucc/)
[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

> 一个面向 **数据任务（ETL / 同步 / 扫描 / 归档 / 监控）** 的异步并发编排框架： 
> 
> **代码里写任务（Job），数据库里配调度与参数（Schedule/Config），运行时自动热更新，天然支持异步高并发与可观测性。**

---

## 目录

- [为什么是 Piko](#为什么是-piko)
- [核心特性](#核心特性)
- [30 秒上手](#30-秒上手)
- [基础概念速览](#基础概念速览)
- [大量示例：异步 / 并发 / 异步并发](#大量示例异步--并发--异步并发)
  - [示例 1：最小 Job + DB 调度](#示例-1最小-job--db-调度)
  - [示例 2：并发抓取 HTTP（有界并发 + 超时）](#示例-2并发抓取-http有界并发--超时)
  - [示例 3：并发扇出/扇入（fan-out / fan-in）](#示例-3并发扇出扇入fan-out--fan-in)
  - [示例 4：生产者-消费者（asyncio.Queue 背压）](#示例-4生产者-消费者asyncioqueue-背压)
  - [示例 5：异步 I/O + CPU 混合流水线（ProcessPool MapReduce）](#示例-5异步-io--cpu-混合流水线processpool-mapreduce)
  - [示例 6：把同步阻塞库变成“异步可并发”](#示例-6把同步阻塞库变成异步可并发)
  - [示例 7：有状态任务（水位线 + 自动补跑）](#示例-7有状态任务水位线--自动补跑)
  - [示例 8：资源依赖注入 Resource（连接池/客户端自动释放）](#示例-8资源依赖注入-resource连接池客户端自动释放)
  - [示例 9：自定义 Resource（你想注入什么都行）](#示例-9自定义-resource你想注入什么都行)
  - [示例 10：持久化写入（队列缓冲 + 批量写 + 磁盘兜底）](#示例-10持久化写入队列缓冲--批量写--磁盘兜底)
  - [示例 11：TypedSink 类型路由（不同模型不同写法）](#示例-11typedsink-类型路由不同模型不同写法)
  - [示例 12：定时任务三种触发器（cron/interval/date）](#示例-12定时任务三种触发器cronintervaldate)
  - [示例 13：动态配置热更新（灰度生效 effective_from）](#示例-13动态配置热更新灰度生效-effective_from)
  - [示例 14：多实例部署（Leader Election）](#示例-14多实例部署leader-election)
- [配置](#配置)
- [运维端点与可观测性](#运维端点与可观测性)
- [项目结构建议](#项目结构建议)
- [FAQ](#faq)

---

## 为什么是 Piko

当你在生产里做“数据任务/同步任务”时，通常会遇到这些痛点：

- 任务是 **asyncio** 的，但调度、并发、资源生命周期、幂等锁、补跑、观测……要自己拼很多代码
- 任务参数经常变，想做到 **在线改配置、灰度生效、无需发版**
- 多实例部署时，需要 **只让一个实例真正执行调度**（Leader/Follower）
- 高并发抓取/同步时，需要 **有界并发**（不炸库、不打爆下游、不 OOM）
- 任务写入落库/发 MQ 等常常成为瓶颈，需要 **队列缓冲、批量写、背压、兜底**

Piko 把这些能力做成“框架默认能力”，你只需要专注写业务 Job。

---

## 核心特性

**Piko 的核心设计**： 
✅ **代码注册任务**（白名单模式） + ✅ **数据库配置调度/参数** + ✅ **运行时 Reconcile 热更新**

- **异步任务模型**：任务必须是 `async def`，天然支持 asyncio 高并发
- **DB 驱动调度**：`scheduled_job` 表配置 cron/interval/date，`job_config` 表配置参数（支持版本、灰度生效）
- **运行时自动热更新**：`ConfigWatcher` 周期性 reconcile DB → 内存缓存 → APScheduler（无需重启）
- **幂等锁**：`job_lock` 防止同一 job 在同一 scheduled_time 上重复执行
- **有状态任务**：维护水位线 `last_data_time`，支持 **自动补跑**（Backfill）
- **资源依赖注入（Resource）**：用 `asynccontextmanager` 管理连接池/客户端，任务结束自动释放
- **CPU 计算池（多进程）**：`CpuManager` 支持 `submit` / `map_reduce`，绕过 GIL 做 CPU 并行
- **持久化写入引擎**：`PersistenceWriter` 队列缓冲 + 批量聚合 + 背压 + 磁盘兜底恢复
- **内置运维 API**：`/healthz` `/readyz` `/metrics`（FastAPI + Prometheus）
- **分布式 Leader Election**：基于 DB 租约 + CAS 乐观锁，多实例下只有 Leader 执行调度

---

## 30 秒上手

> 你需要一个 MySQL（Piko 用它存调度、配置、幂等锁、运行记录等元数据）。

### 1) 安装

```bash
# uv（推荐）
uv pip install piko-cucc
```

### 2) 配置 MySQL DSN

```bash
export PIKO_MYSQL_DSN="mysql+asyncmy://user:pass@127.0.0.1:3306/piko?charset=utf8mb4"
```

> 也可以写到 `settings.toml / piko.toml`，或用 `PIKO_SETTINGS_PATH` 指向自定义配置文件。

### 3) 写一个 Job，然后跑起来

```python
# app.py
from piko import PikoApp

app = PikoApp(name="demo")
api_app = app.api_app

@app.job(job_id="hello_job")
async def hello(ctx, scheduled_time):
    print("hello", ctx["run_id"], scheduled_time)

if __name__ == "__main__":
    app.run()
```

启动：

```bash
python app.py
# 或 ASGI：
# uvicorn app:api_app --reload
```

然后在 DB 插入一条调度：

```sql
INSERT INTO scheduled_job(job_id, schedule_type, schedule_expr, enabled, version)
VALUES ("hello_job", "interval", '{"seconds": 10}', 1, 1)
ON DUPLICATE KEY UPDATE enabled=1, schedule_expr='{"seconds":10}', version=version+1;
```

---

## 基础概念速览

### Job（任务）

- 用 `@app.job(job_id=...)` 注册（白名单）
- 函数签名：`async def handler(ctx, scheduled_time, **resources)`
- `ctx` 里至少包含：
  - `ctx["run_id"]`：本次执行记录 ID（job_run.run_id）
  - `ctx["job_id"]`：任务 ID
  - `ctx["config"]`：任务配置（dict 或 Pydantic Model）
  - 若是有状态任务：`ctx["data_interval"]`（`DataInterval`）

### 调度与参数（DB）

- `scheduled_job`：配置触发器（cron/interval/date）
- `job_config`：配置参数（版本化、灰度生效 `effective_from`）
- `job_run`：执行记录（状态、耗时、错误）
- `job_lock`：幂等锁（同 job_id + scheduled_time 只允许一个实例执行）

---

# 示例：异步 / 并发 / 异步并发

下面的示例尽量都遵循同一个模式：**你写 job，Piko 负责调度/幂等/资源/观测**。你可以直接复制这些片段到自己的 `jobs.py` 中使用。

---

## 示例 1：最小 Job + DB 调度

```python
from piko import PikoApp

app = PikoApp(name="mini")
api_app = app.api_app

@app.job(job_id="mini_job")
async def mini_job(ctx, scheduled_time):
    # ctx["config"] 默认为 {}，如果你没在 job_config 表里配置
    print("run_id=", ctx["run_id"], "scheduled_time=", scheduled_time, "config=", ctx["config"])
```

DB：

```sql
-- 每 5 秒触发一次
INSERT INTO scheduled_job(job_id, schedule_type, schedule_expr, enabled, version)
VALUES ("mini_job", "interval", '{"seconds": 5}', 1, 1)
ON DUPLICATE KEY UPDATE enabled=1, schedule_expr='{"seconds":5}', version=version+1;
```

---

## 示例 2：并发抓取 HTTP（有界并发 + 超时）

场景：你要扫一堆 URL，**并发抓取**，但要避免瞬间打爆下游（有界并发）。

```python
import asyncio
import httpx
from piko import PikoApp

app = PikoApp("http_sweeper")
api_app = app.api_app

URLS = [
    "https://example.com",
    "https://www.python.org",
    # ...
]

@app.job(job_id="sweep_http")
async def sweep_http(ctx, scheduled_time):
    concurrency = 50
    sem = asyncio.Semaphore(concurrency)

    async with httpx.AsyncClient(timeout=10) as client:
        async def fetch(url: str):
            async with sem:
                r = await client.get(url)
                return url, r.status_code, len(r.content)

        # 关键点：gather + semaphore = 有界并发
        results = await asyncio.gather(*(fetch(u) for u in URLS), return_exceptions=True)

    ok = [x for x in results if not isinstance(x, Exception)]
    print("ok=", len(ok), "total=", len(results))
```

要点：

- **Semaphore** 控制并发度（避免把带宽/连接池/下游打爆）
- httpx 是异步 I/O，`gather` 会把等待 I/O 的时间“让出”给别的协程

---

## 示例 3：并发扇出/扇入（fan-out / fan-in）

场景：一条任务输入 → 并发处理 N 份子任务 → 汇总结果。

```python
import asyncio
from piko import PikoApp

app = PikoApp("fanout")
api_app = app.api_app

@app.job(job_id="fanout_fanin")
async def fanout_fanin(ctx, scheduled_time):
    items = list(range(1, 501))

    async def work(x: int) -> int:
        # 模拟 I/O
        await asyncio.sleep(0.01)
        return x * x

    # 扇出：并发执行
    results = await asyncio.gather(*(work(x) for x in items))

    # 扇入：汇总
    total = sum(results)
    print("sum=", total)
```

---

## 示例 4：生产者-消费者（asyncio.Queue 背压）

场景：抓取（快）+ 处理（慢），需要 **队列缓冲** + **背压**（Queue 有界）。

```python
import asyncio
from piko import PikoApp

app = PikoApp("queue_pipeline")
api_app = app.api_app

@app.job(job_id="producer_consumer")
async def producer_consumer(ctx, scheduled_time):
    # 有界队列 = 背压点
    q: asyncio.Queue[int] = asyncio.Queue(maxsize=200)
    concurrency = 20

    async def producer():
        for i in range(5000):
            # 队列满会阻塞（异步阻塞，不占线程）
            await q.put(i)
        for _ in range(concurrency):
            # 结束信号
            await q.put(-1)

    async def consumer(worker_id: int):
        processed = 0
        while True:
            x = await q.get()
            try:
                if x == -1:
                    return processed
                # 模拟 I/O 或业务处理
                await asyncio.sleep(0.002)
                processed += 1
            finally:
                q.task_done()

    consumers = [asyncio.create_task(consumer(i)) for i in range(concurrency)]
    prod_task = asyncio.create_task(producer())

    await prod_task
    # 等待队列处理完
    await q.join()
    stats = await asyncio.gather(*consumers)

    print("total_processed=", sum(stats))
```

要点：

- `Queue(maxsize=N)` = **背压**：生产太快会自动阻塞，防止 OOM
- `q.join()` + `task_done()` = 可靠等待“处理完”

---

## 示例 5：异步 I/O + CPU 混合流水线（ProcessPool MapReduce）

场景：先异步下载/读取数据，再做 CPU 重计算（比如解压、解析、特征提取）。

Piko 内置 `CpuManager`（多进程）：

```python
import math
from piko import PikoApp

app = PikoApp("io_cpu_mix")
api_app = app.api_app

def heavy_cpu(x: int) -> int:
    # CPU 密集：会占满 GIL（所以要多进程）
    math.factorial(2000)
    return x * x

@app.job(job_id="io_plus_cpu")
async def io_plus_cpu(ctx, scheduled_time):
    items = list(range(1000))

    # MapReduce：在多个子进程并行执行 heavy_cpu
    results = await app.cpu_manager.map_reduce(
        map_fn=heavy_cpu,
        items=items,
        # 控制并行进程任务数
        concurrency=4,
    )

    print("done:", len(results), "sample:", results[0])
```

> 这个场景经常遇到：**异步 I/O 把数据拉回来 → 多进程做 CPU 重活 → 异步写出去**。

---

## 示例 6：把同步阻塞库变成“异步可并发”

场景：你依赖一个同步 SDK（例如某些老库/驱动/算法），但你想在 asyncio 下并发调用。

两种办法：

### 6.1 用 `asyncio.to_thread`（适合 I/O 或轻 CPU）

```python
import asyncio
from piko import PikoApp

app = PikoApp("sync_to_async")
api_app = app.api_app

def blocking_call(x: int) -> int:
    # 模拟同步阻塞
    import time
    time.sleep(0.05)
    return x + 1

@app.job(job_id="to_thread_demo")
async def to_thread_demo(ctx, scheduled_time):
    sem = asyncio.Semaphore(100)

    async def run_one(x: int):
        async with sem:
            return await asyncio.to_thread(blocking_call, x)

    results = await asyncio.gather(*(run_one(i) for i in range(1000)))
    print("done", len(results))
```

### 6.2 用 `app.cpu_manager.submit`（适合 CPU 重活，需要绕过 GIL）

```python
from piko import PikoApp

app = PikoApp("cpu_submit")
api_app = app.api_app

def cpu_heavy(x: int) -> int:
    import math
    math.factorial(3000)
    return x * 2

@app.job(job_id="cpu_submit_demo")
async def cpu_submit_demo(ctx, scheduled_time):
    res = await app.cpu_manager.submit(cpu_heavy, 21)
    print("res=", res)
```

---

## 示例 7：有状态任务（水位线 + 自动补跑）

场景：每小时同步一次数据，服务停机 3 小时后重启，要补齐漏掉的数据窗口。

Piko 通过以下手段来实现：

- `@app.job(..., stateful=True, backfill_policy=...)`
- `scheduled_job.last_data_time` 水位线（成功后自动更新）

```python
from piko import PikoApp
from piko.core.types import BackfillPolicy, DataInterval

app = PikoApp("stateful")
api_app = app.api_app

@app.job(job_id="sync_orders", stateful=True, backfill_policy=BackfillPolicy.CATCH_UP)
async def sync_orders(ctx, scheduled_time):
    interval: DataInterval = ctx["data_interval"]
    print("sync window:", interval.start, "->", interval.end)

    # 你的增量逻辑：WHERE updated_at >= start AND updated_at < end
    # await do_incremental_sync(interval.start, interval.end)
```

调度（每小时一次）：

```sql
INSERT INTO scheduled_job(job_id, schedule_type, schedule_expr, enabled, version)
VALUES ("sync_orders", "cron", '{"minute": 0}', 1, 1)
ON DUPLICATE KEY UPDATE enabled=1, schedule_expr='{"minute":0}', version=version+1;
```

补跑策略说明：

- `BackfillPolicy.CATCH_UP`：补齐所有漏掉的窗口（数据完整性优先）
- `BackfillPolicy.SKIP`：只跑最新窗口（实时性优先）

---

## 示例 8：资源依赖注入 Resource（连接池/客户端自动释放）

Resource 的本质：**一个异步上下文管理器工厂**。 Piko 在每次 job 执行时会用 `AsyncExitStack` 自动 enter/exit，确保资源释放。

### 8.1 定义一个 Resource（例如 HTTP Client）

```python
from contextlib import asynccontextmanager
import httpx
from piko.core.resource import resource

@resource
class HttpClientResource:
    @asynccontextmanager
    async def acquire(self, ctx):
        async with httpx.AsyncClient(timeout=10) as client:
            yield client
```

### 8.2 在 job 里声明并注入

```python
import asyncio
from piko import PikoApp

app = PikoApp("resource_di")
api_app = app.api_app

@app.job(
    job_id="fetch_with_resource",
    resources={"client": HttpClientResource},
)
async def fetch_with_resource(ctx, scheduled_time, client):
    # client 是被注入的 httpx.AsyncClient
    urls = ["https://example.com"] * 100
    sem = asyncio.Semaphore(50)

    async def fetch(url):
        async with sem:
            r = await client.get(url)
            return r.status_code

    codes = await asyncio.gather(*(fetch(u) for u in urls))
    print("200_count=", sum(1 for c in codes if c == 200))
```

---

## 示例 9：自定义 Resource（你想注入什么都行）

下面是三个最常见的 Resource 形态：**连接池 / SDK Client / 共享缓存**。

### 9.1 注入 Redis（示意）

```python
from contextlib import asynccontextmanager
from piko.core.resource import resource

@resource
class RedisResource:
    @asynccontextmanager
    async def acquire(self, ctx):
        # 这里用伪代码示意
        # import redis.asyncio as redis
        # client = redis.Redis.from_url("redis://127.0.0.1:6379/0")
        # try:
        #     yield client
        # finally:
        #     await client.close()
        yield object()
```

### 9.2 注入 Mongo / ES / MQ / 任何你自己的 Client

你只要保证：

- `acquire(ctx)` 返回一个 async contextmanager
- `yield` 出你希望注入到 job 的实例
- `finally` 里把连接关闭/释放即可

---

## 示例 10：持久化写入（队列缓冲 + 批量写 + 磁盘兜底）

Piko 的 `PersistenceWriter` 是一个“生产者-消费者”写入引擎：

- job 里 enqueue（快）
- writer 后台批量 flush 到 sink（可控）
- 写失败会 dump 到磁盘，启动时自动恢复

### 10.1 写一个最小 Sink

```python
from piko.persistence.sink_base import ResultSink
from piko.persistence.intent import WriteIntent

class PrintSink(ResultSink):
    def __init__(self):
        super().__init__(name="print")

    async def write_batch(self, batch: list[WriteIntent]):
        for intent in batch:
            print("[SINK]", intent.key, intent.payload)
```

### 10.2 注册 Sink，并在 job 中 enqueue

```python
from piko import PikoApp
from piko.persistence.intent import WriteIntent

app = PikoApp("persist_demo")
api_app = app.api_app

# 在应用启动时注册 sink（建议写在 main 模块里）
app.writer.register_sink(PrintSink())

@app.job(job_id="produce_intents")
async def produce_intents(ctx, scheduled_time):
    for i in range(1000):
        intent = WriteIntent(
            sink="print",
            key=str(i),
            payload={"i": i},
            job_id=ctx["job_id"],
            run_id=ctx["run_id"],
            scheduled_time=scheduled_time,
        )
        # 队列满会背压阻塞（异步阻塞）
        await app.writer.enqueue(intent)
```

---

## 示例 11：TypedSink 类型路由（不同模型不同写法）

如果你的 payload 是不同的 Pydantic Model，希望不同类型走不同写入逻辑：

```python
from pydantic import BaseModel
from piko.persistence.sink_base import TypedSink, on
from piko.persistence.intent import WriteIntent

class User(BaseModel):
    id: int
    name: str

class Order(BaseModel):
    id: int
    amount: float

class MyTypedSink(TypedSink):
    def __init__(self):
        super().__init__(name="typed")

    @on(User)
    async def write_users(self, users: list[User]):
        print("users:", len(users))

    @on(Order)
    async def write_orders(self, orders: list[Order]):
        print("orders:", len(orders))
```

在 job 里 enqueue：

```python
from piko import PikoApp
from piko.persistence.intent import WriteIntent

app = PikoApp("typed_sink_demo")
api_app = app.api_app
app.writer.register_sink(MyTypedSink())

@app.job(job_id="emit_models")
async def emit_models(ctx, scheduled_time):
    await app.writer.enqueue(WriteIntent(
        sink="typed",
        key="u1",
        payload=User(id=1, name="alice"),
        job_id=ctx["job_id"],
        run_id=ctx["run_id"],
        scheduled_time=scheduled_time,
    ))
    await app.writer.enqueue(WriteIntent(
        sink="typed",
        key="o1",
        payload=Order(id=1, amount=9.9),
        job_id=ctx["job_id"],
        run_id=ctx["run_id"],
        scheduled_time=scheduled_time,
    ))
```

---

## 示例 12：定时任务三种触发器（cron/interval/date）

`scheduled_job.schedule_type` 支持：

- `cron`：类似 crontab
- `interval`：固定间隔
- `date`：单次触发

```sql
-- cron：每天 02:30
INSERT INTO scheduled_job(job_id, schedule_type, schedule_expr, enabled, version)
VALUES ("daily_job", "cron", '{"hour": 2, "minute": 30}', 1, 1);

-- interval：每 10 秒
INSERT INTO scheduled_job(job_id, schedule_type, schedule_expr, enabled, version)
VALUES ("fast_job", "interval", '{"seconds": 10}', 1, 1);

-- date：2026-01-08 10:00 触发一次（注意时区由 settings.timezone 决定）
INSERT INTO scheduled_job(job_id, schedule_type, schedule_expr, enabled, version)
VALUES ("one_shot", "date", '{"run_date": "2026-01-08 10:00:00"}', 1, 1);
```

---

## 示例 13：动态配置热更新（灰度生效 effective_from）

在代码里声明 schema：

```python
from pydantic import BaseModel
from piko import PikoApp

app = PikoApp("cfg_demo")
api_app = app.api_app

class SweepConfig(BaseModel):
    concurrency: int = 50
    timeout_s: float = 10

@app.job(job_id="sweep_cfg", schema=SweepConfig)
async def sweep_cfg(ctx, scheduled_time):
    cfg: SweepConfig = ctx["config"]  # 已被 Pydantic 校验 & 类型化
    print("cfg:", cfg.concurrency, cfg.timeout_s)
```

在 DB 里更新参数（立即生效）：

```sql
INSERT INTO job_config(job_id, schema_version, config_json, version)
VALUES ("sweep_cfg", 1, '{"concurrency": 200, "timeout_s": 3}', 1)
ON DUPLICATE KEY UPDATE config_json='{"concurrency":200,"timeout_s":3}', version=version+1;
```

灰度生效（未来时间生效）：

```sql
UPDATE job_config
SET config_json='{"concurrency":100,"timeout_s":5}',
    effective_from = DATE_ADD(UTC_TIMESTAMP(6), INTERVAL 10 MINUTE),
    version=version+1
WHERE job_id="sweep_cfg";
```

---

## 示例 14：多实例部署（Leader Election）

多实例部署时，Piko 默认启用 Leader Election（基于 DB 租约）：

- Leader 执行调度与 job run
- Follower 返回 `/readyz: standby`

配置项（可在 `settings.toml` 或环境变量中设置）：

```toml
[default]
leader_enabled = true
leader_name = "default"
leader_lease_s = 30
leader_renew_interval_s = 10
```

常见部署方式：

- Kubernetes 部署 2~3 个副本
- Prometheus 抓取每个副本的 `/metrics`
- 只有 Leader 的 job_run 会增长（Follower standby）

---

## 配置

Piko 使用 Dynaconf，默认读取：

- `defaults.toml`（库内置）
- `settings.toml` / `piko.toml` / `.secrets.toml`
- 或者设置 `PIKO_SETTINGS_PATH=/path/to/your.toml`

最低必配：

- `mysql_dsn`（必须，负责存元数据）

环境变量示例：

```bash
export PIKO_MYSQL_DSN="mysql+asyncmy://user:pass@host:3306/piko?charset=utf8mb4"
export PIKO_TIMEZONE="Asia/Shanghai"
export PIKO_DEBUG="false"
```

---

## 运维端点与可观测性

Piko 内置 FastAPI 运维端点（无需你自己写）：

- `GET /healthz`：存活探针（liveness）
- `GET /readyz`：就绪探针（readiness；Follower 会是 standby）
- `GET /metrics`：Prometheus 指标

常见指标（示意）：

- job 成功/失败计数：`JOB_RUN_TOTAL{job_id=..., status=...}`
- job 耗时直方图：`JOB_DURATION_SECONDS{job_id=...}`
- leader 状态：`LEADER_STATUS{host=...}`
- 持久化队列长度：`PERSISTENCE_QUEUE_SIZE`

---

## 项目结构建议

一个推荐的业务项目结构：

```
my_project/
  app.py                # 创建 PikoApp、注册 sink、启动
  my_project/
    __init__.py
    jobs.py             # 简单场景：集中放 job
    jobs/
      __init__.py
      user_sync/
        __init__.py
        jobs.py         # 复杂场景：分目录，配合 autodiscover
      report/
        __init__.py
        jobs.py
```

在 `app.py` 中：

```python
from piko import PikoApp, autodiscover

app = PikoApp("my_project")
# 自动导入所有 *.jobs.py，触发注册
autodiscover("my_project", module_name="jobs")
api_app = app.api_app
```

---

## FAQ

### 1) 我的任务里怎么拿到配置？

- 给 job 声明 `schema=YourConfigModel`
- 然后 `cfg: YourConfigModel = ctx["config"]`

### 2) 如何控制并发？

- I/O 并发：`asyncio.Semaphore` + `gather`
- 生产者/消费者：`asyncio.Queue(maxsize=...)`
- CPU 并行：`app.cpu_manager.map_reduce(..., concurrency=N)`

### 3) 如何注入数据库/Redis/Mongo 等资源？

用 `Resource`：

- `@resource` 标记资源类
- 在 `acquire(ctx)` 里创建连接/客户端并 `yield`
- 在 job 的 `resources={...}` 里声明需要注入的资源
