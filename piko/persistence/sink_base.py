from abc import ABC, abstractmethod
from collections import defaultdict
from typing import List, Any, Type, Dict, Callable, Awaitable, TypeVar

from piko.infra.logging import get_logger
from piko.persistence.intent import WriteIntent

logger = get_logger(__name__)

T = TypeVar("T")


class ResultSink(ABC):
    """数据持久化 Sink 抽象基类（协议接口）

    定义所有 Sink 实现必须遵守的契约：接收一批 WriteIntent 并写入目标存储
    PersistenceWriter 通过该接口与各种具体 Sink（MySQL、Kafka、API 等）解耦

    Attributes:
        name (str): Sink 的唯一标识符（用于路由和日志）

    Methods:
        write_batch: 核心写入接口，接收一批 Intent 并执行批量写入

    Warning:
        - write_batch 必须是幂等的（相同 Intent 多次调用结果一致）
        - 不应在 write_batch 中阻塞过长时间（建议使用批量 API 或异步 I/O）
    """

    def __init__(self, name: str):
        """初始化 Sink

        Args:
            name (str): Sink 的唯一标识符，需与 WriteIntent.sink 字段匹配
        """
        self.name = name

    @abstractmethod
    async def write_batch(self, batch: List[WriteIntent]):
        """批量写入数据到目标存储（抽象方法）

        Args:
            batch (List[WriteIntent]): 待写入的意图列表，已按 Sink 分组

        Raises:
            Exception: 写入失败时抛出异常，触发 Writer 的磁盘兜底机制

        Note:
            实现要点：
            1. 遍历 batch，提取 payload 和 idempotency_key
            2. 查询去重表，过滤已写入的数据
            3. 执行批量写入（使用目标存储的批量 API）
            4. 更新去重表状态
        """
        pass


class TypedSink(ResultSink):
    """支持类型分发路由的 Sink 基类

    通过装饰器 @on(ModelType) 注册类型处理函数，自动根据 payload 的运行时类型分发到对应处理器
    消除了繁琐的 if-elif 类型判断，提升代码可读性和可维护性

    Attributes:
        self._handlers (Dict[Type, Callable]): 类型到处理函数的路由表

    Methods:
        on: 装饰器，注册特定类型的处理函数
        write_batch: 重写的批量写入方法，实现自动分组和分发逻辑

    Example:
        ```python
        class MySink(TypedSink):
            def __init__(self):
                super().__init__("my_sink")

            @self.on(User)
            async def save_users(self, users: List[User]):
                # 批量写入用户表
                await db.execute(insert(User).values([u.dict() for u in users]))

            @self.on(Order)
            async def save_orders(self, orders: List[Order]):
                # 批量写入订单表
                await db.execute(insert(Order).values([o.dict() for o in orders]))
        ```
    """

    def __init__(self, name: str):
        """初始化 TypedSink

        Args:
            name (str): Sink 的唯一标识符
        """
        super().__init__(name)
        # 路由表：类型 -> 处理函数
        # 键为 Python 类型对象（如 User、Order），值为 async 函数
        self._handlers: Dict[Type, Callable[[List[Any]], Awaitable[None]]] = {}

    def on(self, model_type: Type[T]):
        """装饰器：注册特定类型的处理函数

        Args:
            model_type (Type[T]): 待处理的数据类型（通常为 Pydantic Model）

        Returns:
            Callable: 装饰器函数，返回原函数（不改变函数本身）
        """

        def decorator(func: Callable[[List[T]], Awaitable[None]]):
            # 重复注册检测：防止误覆盖已有处理器
            if model_type in self._handlers:
                logger.warning(
                    f"TypedSink '{self.name}': Overwriting handler for type {model_type}. "
                    f"Old: {self._handlers[model_type].__name__}, New: {func.__name__}"
                )
            # 注册到路由表
            self._handlers[model_type] = func

            # 装饰器返回原函数（不修改函数本身）
            return func

        return decorator

    async def write_batch(self, batch: List[WriteIntent]):
        """自动分组并分发到对应的类型处理器

        Args:
            batch (List[WriteIntent]): 待写入的意图列表（已按 Sink 分组）

        Note:
            算法步骤：
            1. 分组（Grouping）：遍历 batch，按 payload 的运行时类型分组
               - 使用 defaultdict 自动创建分组列表
               - 键为 type(payload)，值为该类型的所有 payload 实例

            2. 分发（Dispatching）：遍历分组，查找注册的处理器
               - 精确匹配：type(payload) 在 _handlers 中
               - MRO 匹配：沿继承链查找最近的父类处理器
               - 无匹配：记录错误，跳过该分组（避免 crash 整个 batch）

            3. 调用处理器：await handler(items)

            MRO 匹配逻辑（处理继承关系）：
            - 使用 type.mro() 获取方法解析顺序（从子类到父类到 object）
            - 优先匹配最具体的类型（子类优先级高于父类）
            - 示例：Dog.mro() = [Dog, Animal, object]
              - 若注册了 @on(Dog)，直接匹配
              - 若只注册了 @on(Animal)，沿 MRO 查找匹配到 Animal
              - 若都没注册，记录错误
        """
        # 第一步：按 payload 类型分组
        grouped = defaultdict(list)

        for intent in batch:
            payload = intent.payload

            # 获取 Payload 的运行时类型
            # 注意：磁盘恢复的数据可能是 dict（model_ref 为空时）
            p_type = type(payload)
            grouped[p_type].append(payload)

        # 第二步：遍历分组，查找并调用处理器
        for p_type, items in grouped.items():
            # MRO 智能匹配：沿继承链查找最近的处理器
            handler = None

            for cls in p_type.mro():
                if cls in self._handlers:
                    handler = self._handlers[cls]
                    # 找到第一个匹配即停止（最具体的处理器）
                    break

            if handler:
                # 找到处理器，调用批量写入
                await handler(items)
            else:
                logger.error(
                    f"TypedSink '{self.name}' received unhandled type: {p_type}. "
                    f"Registered types: {list(self._handlers.keys())}"
                )
                