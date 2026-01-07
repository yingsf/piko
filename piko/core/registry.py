import inspect
from typing import Any, Callable, Type, Awaitable, Dict, TypedDict, List

from pydantic import BaseModel, ValidationError

from piko.core.resource import Resource
from piko.core.types import BackfillPolicy
from piko.infra.logging import get_logger

logger = get_logger(__name__)

# JobHandler: 任务处理函数的类型签名
JobHandler = Callable[..., Awaitable[Any]]


class JobOptions(TypedDict):
    """任务元数据的类型定义（TypedDict）

    Attributes:
        stateful (bool): 是否为有状态任务
        backfill_policy (BackfillPolicy): 补跑策略
        resources (Dict[str, Type[Resource]]): 资源依赖声明
    """
    stateful: bool
    backfill_policy: BackfillPolicy
    resources: Dict[str, Type[Resource]]


class JobRegistry:
    """任务注册中心（白名单模式）

    本类负责管理所有已注册的任务（job），提供装饰器语法进行任务注册，以及运行时的任务查询、配置验证等功能
    本类由 PikoApp 实例化并持有，不再作为全局单例存在

    Attributes:
        _jobs (Dict[str, JobHandler]): job_id -> 任务处理函数的映射
        _schemas (Dict[str, Type[BaseModel]]): job_id -> Pydantic 配置 Schema 的映射
        _options (Dict[str, JobOptions]): job_id -> 任务元数据的映射
    """

    def __init__(self):
        """初始化注册中心"""
        self._jobs: Dict[str, JobHandler] = {}
        self._schemas: Dict[str, Type[BaseModel]] = {}
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

        Args:
            job_id (str): 任务的唯一标识符
            schema (Type[BaseModel] | None): 任务配置的 Pydantic Schema
            stateful (bool): 是否为有状态任务
            backfill_policy (BackfillPolicy): 补跑策略
            resources (Dict[str, Type[Resource]] | None): 资源依赖声明

        Returns:
            Callable: 装饰器函数
        """

        def wrapper(func: JobHandler):
            if not inspect.iscoroutinefunction(func):
                raise ValueError(f"Job handler '{func.__name__}' must be an async function.")

            if job_id in self._jobs:
                logger.warning("registry_overwrite", job_id=job_id, old=self._jobs[job_id], new=func)

            self._jobs[job_id] = func
            self._options[job_id] = {
                "stateful": stateful,
                "backfill_policy": backfill_policy,
                "resources": resources or {}
            }

            logger.info(
                "job_registered",
                job_id=job_id,
                handler=func.__name__,
                stateful=stateful
            )

            if schema:
                if not issubclass(schema, BaseModel):
                    raise ValueError(f"Schema for '{job_id}' must be a Pydantic BaseModel.")
                self._schemas[job_id] = schema

            return func

        return wrapper

    def get_job(self, job_id: str) -> JobHandler | None:
        """根据 job_id 获取任务处理函数"""
        return self._jobs.get(job_id)

    def get_options(self, job_id: str) -> JobOptions:
        """根据 job_id 获取任务元数据"""
        return self._options.get(
            job_id,
            {
                "stateful": False,
                "backfill_policy": BackfillPolicy.SKIP,
                "resources": {}
            }
        )

    def validate_config(self, job_id: str, config_data: dict) -> BaseModel | dict:
        """验证并转换任务的配置数据"""
        schema = self._schemas.get(job_id)
        if not schema:
            return config_data

        try:
            return schema.model_validate(config_data)
        except ValidationError as e:
            logger.error("config_validation_failed", job_id=job_id, error=str(e))
            raise

    def get_all_job_ids(self) -> List[str]:
        """获取所有已注册的任务 ID 列表

        用于完整性检查等场景，避免外部直接访问私有成员 _jobs

        Returns:
            List[str]: 所有已注册的 job_id 列表
        """
        return list(self._jobs.keys())
