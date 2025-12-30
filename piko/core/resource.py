from abc import ABC, abstractmethod
from typing import Any


class Resource(ABC):
    """外部资源的抽象基类

    本类定义了 Piko 框架中外部资源（如数据库连接、Redis 客户端、FTP 会话等）的统一接口。所有自定义资源必须继承此类并实现 `acquire` 方法

    设计目标：
        1. 依赖注入：通过声明式的资源依赖，实现控制反转（IoC），避免硬编码连接信息
        2. 生命周期管理：Runner 自动管理资源的获取和释放，防止连接泄漏
        3. 可测试性：测试时可用 Mock 资源替换真实资源，无需修改业务代码
        4. 可扩展性：用户可轻松添加新的资源类型（如 Kafka、S3 等）

    使用场景：
        - 数据库连接池（如 PostgreSQL、MySQL、MongoDB）
        - 缓存客户端（如 Redis、Memcached）
        - 消息队列（如 RabbitMQ、Kafka）
        - 文件系统（如 FTP、S3、NFS）
        - 外部 API 客户端（如 HTTP 会话）

    扩展方式：
        用户需继承 `Resource` 并实现 `acquire` 方法，返回一个异步上下文管理器。示例见 `acquire` 方法的 docstring

    Attributes:
        __resource_name__ (str | None): 可选的资源名称，用于调试日志和错误提示。子类可覆盖此属性以提供更友好的资源标识

    Note:
        - 本类是抽象基类（ABC），不能直接实例化
        - 子类必须实现 `acquire` 方法，否则实例化时会抛出 TypeError
        - `__resource_name__` 在基类中显式定义为 None，消除 IDE 关于"未定义属性"的警告
    """

    __resource_name__: str | None = None

    @abstractmethod
    def acquire(self, ctx: dict) -> Any:
        """获取资源的异步上下文管理器

        本方法是资源协议的核心，定义了如何获取和释放外部资源。必须返回一个实现了 `__aenter__` 和 `__aexit__` 的异步上下文管理器

        典型实现方式：
            使用 `@asynccontextmanager` 装饰器，在 `yield` 前获取资源，在 `yield` 后释放资源

        Args:
            ctx (dict): 当前任务的上下文信息，包含但不限于：
                - config: 任务的配置字典（已通过 Pydantic 验证）
                - run_id: 本次运行的唯一标识符（UUID 格式）
                - job_id: 任务的唯一标识符
                - timestamp: 任务触发的时间戳
            用户可根据 ctx 中的信息动态配置资源（如选择不同的数据库实例）

        Returns:
            AsyncContextManager: 异步上下文管理器
                - `__aenter__` 返回的对象会被注入到任务处理函数中
                - `__aexit__` 会在任务完成（或异常）后自动调用，用于清理资源

        Raises:
            NotImplementedError: 如果子类未实现此方法

        Example:
            ```python
            from contextlib import asynccontextmanager
            from piko.core.resource import Resource

            class DatabaseResource(Resource):
                __resource_name__ = "database"

                def acquire(self, ctx: dict):
                    @asynccontextmanager
                    async def _context():
                        # 从配置中获取数据库连接信息
                        db_url = ctx["config"].get("db_url", "postgresql://localhost/mydb")

                        # 获取连接（这里简化为伪代码，实际应使用连接池）
                        conn = await create_connection(db_url)

                        try:
                            # 将连接注入到任务处理函数
                            yield conn
                        finally:
                            # 无论任务成功还是失败，都释放连接
                            await conn.close()

                    return _context()
            ```

        Note:
            - Runner 会在任务执行前调用 `__aenter__`，在任务完成后调用 `__aexit__`
            - 如果任务抛出异常，`__aexit__` 仍会被调用，确保资源不泄漏
            - 返回的上下文管理器应是轻量级的，避免在 `acquire` 中执行耗时操作
              耗时的初始化逻辑应放在 `__aenter__` 中
        """
        pass


class SimpleResource(Resource):
    """辅助类：通过简单的函数定义资源（适配器模式）

    本类允许用户通过普通函数（而非继承 Resource 类）定义资源，降低了定义简单资源的门槛，类似于 pytest 的 fixture 机制

    设计模式：
        - 适配器模式：将函数适配为 Resource 接口
        - 函数式编程风格：对于无状态的资源，函数比类更简洁

    使用场景：
        - 定义简单的资源（如读取配置文件、创建临时目录）
        - 快速原型开发，避免创建完整的 Resource 子类
        - 与 `@resource` 装饰器配合使用，提供声明式的资源定义

    Attributes:
        _func (Callable): 被包装的函数，应返回异步上下文管理器

    Example:
        ```python
        from contextlib import asynccontextmanager
        from piko.core.resource import SimpleResource

        @asynccontextmanager
        async def get_temp_dir(ctx):
            import tempfile
            temp_dir = tempfile.mkdtemp()
            try:
                yield temp_dir
            finally:
                import shutil
                shutil.rmtree(temp_dir)

        # 使用 SimpleResource 包装函数
        temp_dir_resource = SimpleResource(get_temp_dir)
        ```

    Note:
        通常不直接使用此类，而是通过 `@resource` 装饰器间接使用
    """

    def __init__(self, func):
        """初始化 SimpleResource

        Args:
            func (Callable): 接受 ctx 参数并返回异步上下文管理器的函数
        """
        # 保存被包装的函数
        # 不在此处调用函数，而是延迟到 acquire 调用时，这样可以在 Runner 调用时传入最新的 ctx
        self._func = func

    def acquire(self, ctx: dict) -> Any:
        """调用被包装的函数以获取资源

        Args:
            ctx (dict): 任务上下文

        Returns:
            AsyncContextManager: 函数返回的异步上下文管理器

        Note:
            此方法简单地将调用转发给被包装的函数，实现了 Resource 协议
        """
        return self._func(ctx)


def resource(name: str | None = None):
    """装饰器：将函数转换为 Resource 类（装饰器工厂模式）

    本装饰器提供了一种声明式的方式来定义资源，无需手动继承 Resource 类。它在内部创建一个 SimpleResource 的子类，并将函数包装其中

    Args:
        name (str | None): 可选的资源名称，用于日志和调试。如果不提供，资源将没有友好的名称标识

    Returns:
        Callable: 装饰器函数，接受被装饰的函数并返回 Resource 类

    Example:
        ```python
        from contextlib import asynccontextmanager
        from piko.core.resource import resource

        @resource(name="redis_cache")
        @asynccontextmanager
        async def redis(ctx):
            import aioredis
            redis_url = ctx["config"].get("redis_url", "redis://localhost")
            client = await aioredis.from_url(redis_url)
            try:
                yield client
            finally:
                await client.close()

        # 在任务注册时使用
        from piko.core.registry import job

        @job("my_task", resources={"cache": redis})
        # cache 参数会自动注入 Redis 客户端
        async def my_task(config, cache):
            await cache.set("key", "value")
        ```

    Note:
        - 被装饰的函数应使用 `@asynccontextmanager` 装饰器，确保返回异步上下文管理器
        - 返回的是一个类（Resource 的子类），而非实例。Registry 会在需要时实例化它
        - `name` 参数存储在类的 `__resource_name__` 属性中，可用于日志输出
    """

    def wrapper(func):
        """真正地装饰器函数

        Args:
            func (Callable): 被装饰的函数，应返回异步上下文管理器

        Returns:
            Type[Resource]: 动态创建的 Resource 子类

        Note:
            使用类而非实例的原因：
                - Registry 存储的是类（Type[Resource]），在每次任务执行时才实例化
                - 这样可以在实例化时传入不同的参数（虽然当前版本未使用）
                - 符合"依赖注入容器"的常见设计模式（类作为"蓝图"，容器负责创建实例）
        """

        # 动态创建一个 SimpleResource 的子类
        #   1. 使用动态类而非直接返回 SimpleResource 实例，是为了与 Resource 协议保持一致（Registry 期望存储的是类，而非实例）
        #   2. 每次调用 @resource 都会创建一个新的类，避免多个资源共享同一个类导致的元数据污染
        class _Wrapped(SimpleResource):
            __resource_name__ = name

            def __init__(self):
                """初始化包装后的资源类

                调用父类的 __init__，将被装饰的函数传递给 SimpleResource
                """
                super().__init__(func)

        # 返回动态创建的类（而非实例）
        # Registry 会在任务执行时调用 _Wrapped() 创建实例，然后调用实例的 acquire(ctx) 获取资源
        return _Wrapped

    return wrapper
