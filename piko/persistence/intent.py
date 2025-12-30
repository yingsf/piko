from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

from pydantic import BaseModel, Field, field_validator


class WriteIntent(BaseModel):
    """持久化写入意图

    描述一次数据持久化操作的完整上下文，包括目标 Sink、数据载荷、幂等性保证、以及用于审计和溯源的元数据
    支持 Pydantic 模型和字典两种 payload 格式

    Attributes:
        sink (str): 目标 Sink 名称（如 'mysql_orders'、'kafka_events'）
        key (str): 数据的业务主键或唯一标识符
        payload (Any): 待写入的数据载荷，可以是 Pydantic Model 实例或字典
        model_ref (str | None): Payload 的类路径（如 'myapp.models.User'），用于磁盘序列化后的类型还原（Rehydration）
        idempotency_key (str | None): 幂等性键，用于去重表防止重复写入
        op_type (str): 操作类型，如 'insert'、'update'、'upsert'、'delete'
        job_id (str): 关联的任务 ID（用于溯源）
        run_id (int): 关联的执行记录 ID（用于溯源）
        scheduled_time (datetime): 任务的计划执行时间（用于溯源）
        created_at (datetime): Intent 创建时间（UTC naive datetime）
    """

    sink: str
    key: str

    # 使用 Any + mode="before" validator 允许直接传入对象实例
    payload: Any

    # 记录 Payload 的类路径（用于磁盘恢复后的类型还原）
    model_ref: Optional[str] = None

    idempotency_key: Optional[str] = None
    op_type: str = "upsert"

    # 元数据：用于审计和溯源
    job_id: str
    run_id: int
    scheduled_time: datetime

    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc).replace(tzinfo=None)
    )

    @field_validator("payload", mode="before")
    @classmethod
    def validate_payload(cls, v: Any) -> Any:
        """拦截 payload 校验，保持 Pydantic 对象的原始状态

        Args:
            v (Any): 待校验的 payload 值

        Returns:
            Any: 原始值（不做类型转换）

        Raises:
            ValueError: 当 payload 为 None 时

        Note:
            Pydantic Trick：
            - 默认情况下，Pydantic 会在校验阶段将嵌套对象转换为字典
            - mode="before" 使得该 validator 在类型转换之前执行
            - 直接返回原始值，保持 Pydantic Model 实例在内存中的对象状态
            - 这样传递给 TypedSink 时仍然是对象，可以使用 isinstance 判断类型
        """
        # 增加非空校验，防止 None 值导致下游逻辑出错
        if v is None:
            raise ValueError("Payload cannot be None")
        return v

    def get_payload_type_str(self) -> str:
        """获取 payload 的类型字符串（用于日志和调试）

        Returns:
            str: 类型的完整路径（如 'myapp.models.User'）或 'dict'

        Note:
            优先使用 model_ref（已记录的类路径），其次通过反射获取运行时类型
        """
        # 优先使用预存的类路径（磁盘恢复场景）
        if self.model_ref:
            return self.model_ref

        val = self.payload
        # 运行时类型推断：仅对 Pydantic Model 有效
        if isinstance(val, BaseModel):
            return f"{val.__class__.__module__}.{val.__class__.__name__}"

        # 兜底：纯字典类型
        return "dict"
    