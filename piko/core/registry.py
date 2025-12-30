import inspect
from typing import Any, Callable, Type, Awaitable, Dict, TypedDict

from pydantic import BaseModel, ValidationError

from piko.core.resource import Resource
from piko.core.types import BackfillPolicy
from piko.infra.logging import get_logger

logger = get_logger(__name__)

# JobHandler: 任务处理函数的类型签名，必须是异步函数（协程函数），参数和返回值类型可变
JobHandler = Callable[..., Awaitable[Any]]


class JobOptions(TypedDict):
    """任务元数据的类型定义（TypedDict）

    Attributes:
        stateful (bool): 是否为有状态任务
            - True: 任务依赖于上一次运行的状态（如增量同步），失败后重试会从上次成功的检查点继续
            - False: 任务是无状态的（如数据转换、报表生成），失败后重试会从头开始

        backfill_policy (BackfillPolicy): 补跑策略，控制任务漏跑后的行为
            - SKIP: 跳过漏跑的调度（适用于实时性要求不高的任务）
            - RUN_ONCE: 仅补跑一次（适用于幂等任务）
            - RUN_ALL: 补跑所有漏跑的调度（适用于数据完整性要求高的任务）

        resources (Dict[str, Type[Resource]]): 资源依赖声明
            键为资源的注入名称（会作为任务处理函数的参数名），值为 Resource 类（Runner 会实例化并调用 acquire）
            示例：{"db": PostgresResource, "cache": RedisResource}

    Example:
        ```python
        options: JobOptions = {
            "stateful": True,
            "backfill_policy": BackfillPolicy.RUN_ONCE,
            "resources": {"db": MyDatabaseResource}
        }
        ```

    Note:
        - TypedDict 仅在类型检查时生效，运行时等价于普通字典
        - 未来如需添加新字段（如 retry_policy、timeout 等），直接在此处添加即可，无需修改使用处的代码（向后兼容）
    """
    stateful: bool
    backfill_policy: BackfillPolicy
    resources: Dict[str, Type[Resource]]  # [新增 v0.3]


class JobRegistry:
    """任务注册中心（白名单模式）

    本类负责管理所有已注册的任务（job），提供装饰器语法进行任务注册，以及运行时的任务查询、配置验证等功能

    设计模式：
        - 注册表模式：集中管理所有任务，避免硬编码和分散定义
        - 白名单安全模式：只有显式注册的任务才能被调度，防止未知任务被执行
        - 装饰器语法：使用 `@job` 装饰器进行声明式注册，代码简洁且易读

    核心职责：
        1. 任务注册：通过 `register` 装饰器将函数注册为任务
        2. 元数据存储：存储任务的配置 Schema、状态属性、补跑策略、资源依赖等
        3. 配置验证：使用 Pydantic 对任务配置进行运行时验证，提前发现配置错误
        4. 任务查询：根据 job_id 查询任务处理函数和元数据

    安全性考量：
        - 白名单机制：未注册的 job_id 无法被调度，防止恶意任务注入
        - 配置验证：Pydantic Schema 确保配置数据的类型和格式正确，防止运行时错误
        - 覆盖警告：注册同名任务时记录警告日志，便于发现配置错误或代码冲突

    扩展性设计：
        - 通过 JobOptions 扩展任务元数据，无需修改核心逻辑
        - 支持资源依赖注入，后续可扩展为更复杂的依赖图

    Attributes:
        _jobs (Dict[str, JobHandler]): job_id -> 任务处理函数的映射
        _schemas (Dict[str, Type[BaseModel]]): job_id -> Pydantic 配置 Schema 的映射
        _options (Dict[str, JobOptions]): job_id -> 任务元数据的映射

    Example:
        ```python
        from piko.core.registry import job
        from pydantic import BaseModel

        class MyTaskConfig(BaseModel):
            input_path: str
            output_path: str

        @job(
            "etl_task",
            schema=MyTaskConfig,
            stateful=True,
            backfill_policy=BackfillPolicy.RUN_ONCE,
            resources={"db": MyDatabaseResource}
        )
        async def etl_task(config: MyTaskConfig, db):
            # config 已通过 Pydantic 验证
            # db 是自动注入的数据库连接
            data = await db.fetch(config.input_path)
            await db.save(data, config.output_path)
        ```

    Note:
        - 本类应作为全局单例使用（见模块底部的 `registry = JobRegistry()`）
        - 多次注册同一个 job_id 会覆盖旧的注册，并记录警告日志
        - 任务处理函数必须是协程函数（async def），否则注册时会抛出 ValueError
    """

    def __init__(self):
        """初始化注册中心

        创建三个内部字典用于存储任务的处理函数、配置 Schema 和元数据
        """
        # 存储任务的处理函数
        self._jobs: Dict[str, JobHandler] = {}

        # 存储任务的 Pydantic 配置 Schema
        self._schemas: Dict[str, Type[BaseModel]] = {}

        # 存储任务的元数据
        self._options: Dict[str, JobOptions] = {}

    def register(
            self,
            job_id: str,
            schema: Type[BaseModel] | None = None,
            stateful: bool = False,
            backfill_policy: BackfillPolicy = BackfillPolicy.SKIP,
            resources: Dict[str, Type[Resource]] | None = None
    ):
        """装饰器工厂：将函数注册为 Piko 任务

        本方法返回一个装饰器，用于将异步函数注册到任务注册中心支持任务配置验证、状态管理、补跑策略和资源依赖注入

        Args:
            job_id (str): 任务的唯一标识符避免使用数字或无意义字符串

            schema (Type[BaseModel] | None): 任务配置的 Pydantic Schema
                - 如果提供，Runner 会在任务执行前验证配置数据
                - 验证通过后，配置会被转换为 Pydantic 模型实例，提供类型安全的属性访问
                - 如果不提供，配置会作为普通字典传递给任务处理函数

            stateful (bool): 是否为有状态任务（默认 False）
                - True: 任务会维护状态（如上次处理的记录 ID），失败重试时从检查点继续
                - False: 任务无状态，每次执行都是独立的

            backfill_policy (BackfillPolicy): 补跑策略（默认 SKIP）
                - SKIP: 跳过漏跑的调度（适用于实时性要求不高的任务）
                - RUN_ONCE: 仅补跑一次最近的漏跑调度（适用于幂等任务）
                - RUN_ALL: 补跑所有漏跑的调度（适用于数据完整性要求高的任务）

            resources (Dict[str, Type[Resource]] | None): 资源依赖声明
                键为注入到任务处理函数的参数名，值为 Resource 类
                Runner 会在任务执行前实例化资源并调用 acquire，将资源作为关键字参数注入到任务处理函数中
                示例：{"db": PostgresResource, "cache": RedisResource}

        Returns:
            Callable: 装饰器函数，接受任务处理函数并返回原函数（支持链式装饰）

        Raises:
            ValueError: 如果任务处理函数不是协程函数，或 schema 不是 BaseModel 子类

        Example:
            ```python
            from piko.core.registry import job
            from pydantic import BaseModel

            class TaskConfig(BaseModel):
                batch_size: int = 100

            @job(
                "process_data",
                schema=TaskConfig,
                stateful=True,
                backfill_policy=BackfillPolicy.RUN_ONCE,
                resources={"db": MyDatabaseResource}
            )
            async def process_data(config: TaskConfig, db):
                for i in range(0, 1000, config.batch_size):
                    batch = await db.fetch(i, i + config.batch_size)
                    await process_batch(batch)
            ```

        Note:
            - 本方法是装饰器工厂（返回装饰器），而非直接的装饰器
            - 装饰器返回原函数，因此可以在注册后继续使用该函数（如在测试中直接调用）
            - 如果多次注册同一个 job_id，会覆盖旧的注册并记录警告日志
        """

        def wrapper(func: JobHandler):
            """真正地装饰器函数

            Args:
                func (JobHandler): 被装饰的任务处理函数

            Returns:
                JobHandler: 原函数（未修改，支持链式装饰）

            Raises:
                ValueError: 如果函数不是协程函数

            Note:
                - 不修改原函数，仅将其注册到内部字典中
                - 返回原函数使得函数可以在注册后继续被调用（如在测试中）
            """
            # 检查函数是否为协程函数（async def）
            # 原因：Piko 的 Runner 使用 await 调用任务处理函数，如果函数不是协程函数，会导致 "object is not awaitable" 运行时错误
            if not inspect.iscoroutinefunction(func):
                raise ValueError(f"Job handler '{func.__name__}' must be an async function.")

            # 检查是否已存在同名任务
            if job_id in self._jobs:
                logger.warning("registry_overwrite", job_id=job_id, old=self._jobs[job_id], new=func)

            # 注册任务处理函数
            self._jobs[job_id] = func

            # 存储任务的元数据
            self._options[job_id] = {
                "stateful": stateful,
                "backfill_policy": backfill_policy,
                "resources": resources or {}
            }

            # 记录任务注册日志
            logger.info(
                "job_registered",
                job_id=job_id,
                handler=func.__name__,
                stateful=stateful,
                resources=list(resources.keys()) if resources else []
            )

            # 验证并存储配置 Schema
            if schema:
                # 检查 schema 是否为 Pydantic BaseModel 的子类
                if not issubclass(schema, BaseModel):
                    raise ValueError(f"Schema for '{job_id}' must be a Pydantic BaseModel.")
                self._schemas[job_id] = schema

            # 返回原函数（未修改）
            return func

        # 返回装饰器函数
        return wrapper

    def get_job(self, job_id: str) -> JobHandler | None:
        """根据 job_id 获取任务处理函数

        Args:
            job_id (str): 任务的唯一标识符

        Returns:
            JobHandler | None: 任务处理函数（协程函数），如果 job_id 不存在，返回 None

        Note:
            - 返回 None 而非抛出异常，便于调用方判断任务是否存在
            - Runner 在调度任务前会调用此方法检查任务是否已注册（白名单检查）
        """
        return self._jobs.get(job_id)

    def get_options(self, job_id: str) -> JobOptions:
        """根据 job_id 获取任务元数据

        Args:
            job_id (str): 任务的唯一标识符

        Returns:
            JobOptions: 任务的元数据字典
                如果 job_id 不存在，返回默认值（stateful=False, backfill_policy=SKIP, resources={}）

        Note:
            - 返回默认值而非 None，避免调用方需要额外的空值检查
            - 默认值的设计哲学：假设任务是无状态的、不需要补跑、无资源依赖（最简单的情况）
        """
        return self._options.get(
            job_id,
            # 默认的任务元数据
            {
                "stateful": False,
                "backfill_policy": BackfillPolicy.SKIP,
                "resources": {}
            }
        )

    def validate_config(self, job_id: str, config_data: dict) -> BaseModel | dict:
        """验证并转换任务的配置数据

        如果任务注册时提供了 Pydantic Schema，则使用 Schema 验证配置数据，验证通过后返回 Pydantic 模型实例，提供类型安全的属性访问
        如果未提供 Schema，则原样返回配置字典

        Args:
            job_id (str): 任务的唯一标识符
            config_data (dict): 原始配置数据（通常来自数据库或配置文件）

        Returns:
            BaseModel | dict: 
                - 如果有 Schema，返回 Pydantic 模型实例（已验证）
                - 如果无 Schema，返回原始字典

        Raises:
            ValidationError: 如果配置数据不符合 Schema 定义，异常信息包含详细的字段错误和错误原因，便于调试

        Example:
            ```python
            from pydantic import BaseModel

            class TaskConfig(BaseModel):
                batch_size: int
                timeout: float = 30.0

            # 注册任务时提供 Schema
            @job("my_task", schema=TaskConfig)
            async def my_task(config: TaskConfig):
                print(config.batch_size)  # 类型安全的属性访问

            # 验证配置
            registry = JobRegistry()
            config = registry.validate_config("my_task", {"batch_size": 100})
            # config 是 TaskConfig 实例，config.batch_size == 100，config.timeout == 30.0
            ```

        Note:
            - 验证失败时会记录错误日志并重新抛出异常，便于排查配置问题
            - Pydantic 的验证不仅检查类型，还支持自定义验证器、数据转换等高级功能
            - 建议为所有任务定义 Schema，即使配置很简单，这能提前发现配置错误
        """
        # 查询任务是否有 Schema
        schema = self._schemas.get(job_id)
        if not schema:
            # 无 Schema，直接返回原始字典
            return config_data

        try:
            return schema.model_validate(config_data)
        except ValidationError as e:
            logger.error("config_validation_failed", job_id=job_id, error=str(e))
            raise


# 全局的注册中心实例
registry = JobRegistry()

# 提供简洁的装饰器别名
# 使用 @job 而非 @registry.register，更符合 Python 惯例（如 Flask 的 @app.route）
job = registry.register
