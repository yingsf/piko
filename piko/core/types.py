from dataclasses import dataclass
from datetime import datetime, date
from enum import Enum


class BackfillPolicy(str, Enum):
    """数据回填策略（补跑策略）

    定义了任务漏跑后的补跑行为，适用于有状态任务（stateful=True），无状态任务不受此策略影响（每次触发都是独立的）

    策略说明：
        - CATCH_UP（追赶模式）*：补齐所有漏掉的执行周期
          适用于数据完整性要求高的场景（如数据同步、ETL 任务）
          例如：每小时同步一次数据，如果停机 3 小时，重启后会依次补跑这 3 小时的数据

        - SKIP（跳过模式）：忽略过去漏掉的周期，只执行最新的一次
          适用于实时性优先的场景（如监控告警、报表生成）
          类似于 Airflow 的 `catchup=False` 行为
          例如：每天生成一次报表，如果停机 3 天，重启后只生成今天的报表，跳过前 3 天

    Attributes:
        CATCH_UP (str): 追赶模式，补齐所有漏掉的周期
        SKIP (str): 跳过模式，忽略过去，只跑最新的周期

    Example:
        ```python
        from piko.core.registry import job
        from piko.core.types import BackfillPolicy

        # 数据同步任务：使用 CATCH_UP 策略
        @job("sync_users", stateful=True, backfill_policy=BackfillPolicy.CATCH_UP)
        async def sync_users(ctx, scheduled_time):
            interval = ctx["data_interval"]
            # 同步 interval.start 到 interval.end 之间的数据
            pass

        # 监控告警任务：使用 SKIP 策略
        @job("health_check", stateful=True, backfill_policy=BackfillPolicy.SKIP)
        async def health_check(ctx, scheduled_time):
            # 只检查当前状态，不关心历史
            pass
        ```

    Note:
        - 补跑策略仅对有状态任务（stateful=True）生效
        - 无状态任务每次触发都是独立的，不涉及补跑概念
        - 如果任务调度频率很高（如每分钟一次），CATCH_UP 可能导致长时间的补跑，
          应谨慎使用或设置 backfill_max_loops 限制补跑次数
    """
    # 追赶模式：补齐所有漏掉的周期 (默认)
    CATCH_UP = "catch_up"
    # 跳过模式：忽略过去，只跑最新的 (类似 Airflow catchup=False)
    SKIP = "skip"


@dataclass
class DataInterval:
    """数据处理的时间窗口（左闭右开区间）

    表示一个数据处理的时间范围 [start, end)，遵循"左闭右开"约定，适用于有状态任务（stateful=True）的增量数据处理

    使用场景：
        - 增量同步：每小时同步一次数据，时间窗口为 [上次水位线, 本次触发时间)
        - 按天处理：每天凌晨处理前一天的数据，时间窗口为 [昨天 00:00, 今天 00:00)
        - 滑动窗口：每分钟处理最近 5 分钟的数据，时间窗口为 [now - 5min, now)

    Attributes:
        start (datetime): 时间窗口的起始时间（包含，Inclusive）
            表示数据处理的开始边界，数据的时间戳 >= start

        end (datetime): 时间窗口的结束时间（不包含，Exclusive）
            表示数据处理的结束边界，数据的时间戳 < end

    Properties:
        biz_date (date): 业务日期（基于窗口开始时间）
            适用于按天处理数据的场景（如 T+1 报表）

    Example:
        ```python
        from datetime import datetime
        from piko.core.types import DataInterval

        # 处理 2025-12-30 一天的数据
        interval = DataInterval(
            start=datetime(2025, 12, 30, 0, 0, 0),
            end=datetime(2025, 12, 31, 0, 0, 0)
        )
        print(interval.biz_date)  # 2025-12-30

        # 处理最近一小时的数据
        now = datetime.utcnow()
        interval = DataInterval(
            start=now - timedelta(hours=1),
            end=now
        )
        ```

    Note:
        - 时间窗口的起止时间均为 Naive UTC（无时区信息），与 Piko 的数据库约定一致
        - 对于无状态任务，start == end，表示这是一个触发时间点而非时间段
        - 建议在数据查询时使用 `WHERE timestamp >= start AND timestamp < end`，确保数据不重复不遗漏
    """
    # 包含
    start: datetime
    # 不包含
    end: datetime

    @property
    def biz_date(self) -> date:
        """获取业务日期（基于窗口开始时间）

        业务日期通常用于数据分区、报表命名等场景，定义为时间窗口的开始日期（忽略时分秒）

        Returns:
            date: 时间窗口起始时间的日期部分

        Example:
            ```python
            from datetime import datetime
            from piko.core.types import DataInterval

            # 处理 2025-12-30 00:00 到 2025-12-31 00:00 的数据
            interval = DataInterval(
                start=datetime(2025, 12, 30, 0, 0, 0),
                end=datetime(2025, 12, 31, 0, 0, 0)
            )

            # 业务日期为 2025-12-30
            assert interval.biz_date == date(2025, 12, 30)

            # 生成报表文件名
            filename = f"report_{interval.biz_date}.csv"
            # filename = "report_2025-12-30.csv"
            ```

        Note:
            - 对于跨天的时间窗口（如 2025-12-30 18:00 到 2025-12-31 06:00），业务日期为窗口起始日期（2025-12-30），而非窗口结束日期
            - 如果需要窗口结束日期，可以使用 `self.end.date()`
        """
        return self.start.date()

    def __repr__(self):
        """返回时间窗口的字符串表示（便于调试和日志）

        Returns:
            str: 格式为 "DataInterval[起始时间 -> 结束时间]" 的字符串

        Example:
            ```python
            interval = DataInterval(
                start=datetime(2025, 12, 30, 10, 0, 0),
                end=datetime(2025, 12, 30, 11, 0, 0)
            )
            print(interval)
            # DataInterval[2025-12-30T10:00:00 -> 2025-12-30T11:00:00]
            ```

        Note:
            - 使用 ISO 8601 格式（`isoformat()`）确保时间表示清晰且易解析
            - 在日志中打印 DataInterval 时会自动调用此方法
        """
        return f"DataInterval[{self.start.isoformat()} -> {self.end.isoformat()}]"
    