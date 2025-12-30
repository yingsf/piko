from fastapi import Response
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST

# ============================================================================
# --- Metrics Definition ---
# ============================================================================

"""任务执行总次数（Counter）

记录每个任务的执行次数，按任务 ID 和执行状态分组，用于监控任务的执行频率、成功率和失败率

标签（Labels）:
    job_id (str): 任务的唯一标识符（如 "sync_user_data"）
    status (str): 任务执行状态，可能的值包括：
        - "success": 任务成功完成
        - "failed": 任务执行失败（业务异常或代码错误）
        - "timeout": 任务超时
        - "cancelled": 任务被取消
        - "skipped": 任务被跳过（如补跑策略为 SKIP）

使用场景：
    - 计算任务成功率：`rate(piko_job_run_total{status="success"}[5m]) / rate(piko_job_run_total[5m])`
    - 监控特定任务的执行频率：`rate(piko_job_run_total{job_id="my_task"}[1h])`
    - 告警规则：如果某任务失败率超过 5%，触发告警

Example (代码中增加计数):
    ```python
    JOB_RUN_TOTAL.labels(job_id="sync_data", status="success").inc()
    ```

Example (Prometheus 查询):
    ```promql
    # 过去 1 小时各任务的执行次数
    sum by (job_id) (increase(piko_job_run_total[1h]))
    ```
"""
JOB_RUN_TOTAL = Counter(
    "piko_job_run_total",
    "Total number of job runs",
    ["job_id", "status"]
)

"""任务执行时间分布（Histogram）

记录每个任务的执行耗时，自动计算各分位数（如 P50、P95、P99），用于性能分析和 SLA 监控

标签（Labels）:
    job_id (str): 任务的唯一标识符

Example (代码中记录耗时):
    ```python
    import time
    start = time.time()
    # ... 执行任务 ...
    duration = time.time() - start
    JOB_DURATION_SECONDS.labels(job_id="my_task").observe(duration)
    ```

Example (Prometheus 查询):
    ```promql
    # P99 执行时间（过去 5 分钟）
    histogram_quantile(0.99, 
      rate(piko_job_duration_seconds_bucket{job_id="sync_data"}[5m])
    )
    ```

Note:
    - Histogram 会自动生成以下时间序列：
        - `piko_job_duration_seconds_bucket{le="1.0"}`: 执行时间 ≤ 1 秒的次数
        - `piko_job_duration_seconds_sum`: 所有执行时间的总和
        - `piko_job_duration_seconds_count`: 执行次数（等价于 JOB_RUN_TOTAL）
    - 使用 `histogram_quantile` 函数计算分位数时，需要 `rate()` 或 `increase()` 包裹
"""
JOB_DURATION_SECONDS = Histogram(
    "piko_job_duration_seconds",
    "Job execution duration in seconds",
    ["job_id"],
    buckets=[0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 300.0]
)

"""当前实例是否为 Leader（Gauge）

记录每个实例的 Leader 状态，用于监控分布式选主的健康状态，在高可用部署中，确保有且仅有一个实例为 Leader

标签（Labels）:
    host (str): 实例的主机标识（如 "hostname:pid"），用于区分不同实例

取值：
    - 1: 当前实例是 Leader（正在调度任务）
    - 0: 当前实例是 Standby（等待接管）

Example (代码中更新状态):
    ```python
    LEADER_STATUS.labels(host="server1:12345").set(1)  # 成为 Leader
    LEADER_STATUS.labels(host="server1:12345").set(0)  # 降级为 Standby
    ```

Example (Prometheus 查询):
    ```promql
    # 检查当前 Leader 数量
    sum(piko_leader_status)

    # 查看当前 Leader 是哪个实例
    piko_leader_status{piko_leader_status="1"}
    ```

Note:
    - 在 Leader 选举失败时，应立即将状态设为 0，避免监控数据滞后
    - 结合 `LEADER_CHANGES_TOTAL` Counter 可以监控 Leader 切换频率（如果频繁切换，说明网络或配置有问题）
"""
LEADER_STATUS = Gauge(
    "piko_leader_status",
    "Whether this instance is the leader (1=Leader, 0=Standby)",
    ["host"]
)


"""持久化队列的当前长度（Gauge）

记录待写入数据库的任务运行记录（JobRun）的积压数量，用于监控持久化组件的健康状态和性能瓶颈

取值：
    - 队列中待处理的 JobRun 对象数量（非负整数）

Example (代码中更新队列长度):
    ```python
    # 入队时
    queue.append(job_run)
    PERSISTENCE_QUEUE_SIZE.set(len(queue))

    # 出队时
    batch = queue[:100]
    await db.insert_many(batch)
    queue = queue[100:]
    PERSISTENCE_QUEUE_SIZE.set(len(queue))
    ```

Example (Prometheus 查询):
    ```promql
    # 过去 10 分钟队列长度的平均值
    avg_over_time(piko_persistence_queue_size[10m])

    # 告警：队列积压超过 1000
    piko_persistence_queue_size > 1000
    ```

Note:
    - 队列长度应定期更新（如每次批量写入后），确保监控数据的实时性
    - 如果队列长度持续为 0，说明没有任务在执行（正常）或持久化速度极快（罕见）
"""
PERSISTENCE_QUEUE_SIZE = Gauge(
    "piko_persistence_queue_size",
    "Current number of items in the persistence queue"
)

"""配置同步周期的执行次数（Counter）

记录 Scheduler 从数据库同步任务配置的次数，按同步结果分组，用于监控配置同步的健康状态和频率

标签（Labels）:
    result (str): 同步结果，可能的值包括：
        - "success": 同步成功，配置已更新
        - "no_change": 配置未变化，无需更新
        - "failed": 同步失败（如数据库连接失败）

Example (代码中增加计数):
    ```python
    try:
        changed = await reconcile_config()
        if changed:
            CONFIG_RECONCILE_TOTAL.labels(result="success").inc()
        else:
            CONFIG_RECONCILE_TOTAL.labels(result="no_change").inc()
    except Exception:
        CONFIG_RECONCILE_TOTAL.labels(result="failed").inc()
    ```

Example (Prometheus 查询):
    ```promql
    # 过去 1 小时同步次数
    increase(piko_config_reconcile_total[1h])
    ```
"""
CONFIG_RECONCILE_TOTAL = Counter(
    "piko_config_reconcile_total",
    "Total number of config reconciliation cycles",
    ["result"]
)


def metrics_endpoint():
    """生成 Prometheus 指标的 HTTP 端点响应

    本函数生成符合 Prometheus 文本格式（Text Exposition Format）的指标数据，
    并设置正确的 Content-Type 响应头供 Prometheus Server 定期抓取

    Returns:
        Response: FastAPI Response 对象，包含：
            - content: Prometheus 文本格式的指标数据（字符串）
            - media_type: `text/plain; version=0.0.4; charset=utf-8`（Prometheus 标准格式）

    Example (FastAPI 路由):
        ```python
        from fastapi import FastAPI
        from piko.infra.observability import metrics_endpoint

        app = FastAPI()

        @app.get("/metrics")
        def get_metrics():
            return metrics_endpoint()
        ```

    Example (Prometheus 配置):
        ```yaml
        scrape_configs:
          - job_name: 'piko'
            scrape_interval: 15s
            static_configs:
              - targets: ['localhost:8000']
            metrics_path: '/metrics'
        ```
    """
    # 生成 Prometheus 文本格式的指标数据
    # generate_latest() 会遍历所有已注册的 Collector（Counter、Histogram、Gauge 等），调用它们的 collect() 方法，序列化为文本格式
    content = generate_latest()

    # 返回 FastAPI Response 对象
    return Response(content=content, media_type=CONTENT_TYPE_LATEST)
