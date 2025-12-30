from dataclasses import dataclass
from typing import Dict, Optional, Set


@dataclass(frozen=True)
class CachedConfig:
    """缓存的任务配置数据容器（不可变数据类）

    本类使用 `@dataclass(frozen=True)` 确保配置数据的不可变性，防止在多线程或异步环境中被意外修改，提供线程安全保证

    Attributes:
        config_json (dict): 任务的配置数据（JSON 格式）
            应包含任务执行所需的所有参数（如数据源路径、API 密钥等）

        version (int): 配置的版本号（来自数据库）
            每次更新配置时递增，用于检测配置是否被修改
            结合数据库的 version 字段实现乐观锁，防止并发更新冲突

        schema_version (int): 配置 Schema 的版本号
            当配置格式发生破坏性变更时递增（如字段重命名、类型变更）
            用于在配置验证前判断是否需要数据迁移

    Example:
        ```python
        config = CachedConfig(
            config_json={"batch_size": 100, "timeout": 30.0},
            version=5,
            schema_version=2
        )

        # 尝试修改会抛出 FrozenInstanceError
        # config.version = 6

        # 正确的更新方式：创建新实例
        new_config = CachedConfig(
            config_json=config.config_json,
            version=6,
            schema_version=2
        )
        ```

    Note:
        - `frozen=True` 使所有字段不可变，尝试修改会抛出 `FrozenInstanceError`
        - config_json 虽然是字典（可变类型），但应视为不可变
          如果需要修改，应创建新的 CachedConfig 实例（Copy-on-Write 模式）
        - 版本号的设计遵循"乐观锁"模式：假设冲突很少，通过版本号检测冲突，
          而非使用悲观锁（加锁）阻止并发访问
    """
    config_json: dict
    version: int
    schema_version: int


class ConfigCache:
    """任务配置的内存缓存（LRU 语义）

    本类实现了一个简单的内存缓存，用于存储从数据库加载的任务配置
    通过减少数据库查询次数，显著提升任务调度的性能（尤其是高频任务）

    缓存策略：
        - 写入时机：当 Scheduler 从数据库加载配置后，调用 set 写入缓存
        - 读取时机：Scheduler 在每次调度前先查询缓存，命中则跳过数据库查询
        - 失效时机：
            1. 配置更新时，Scheduler 主动从缓存中删除旧配置（或覆盖）
            2. 任务被删除时，通过 prune 方法清理不活跃的配置
            3. 服务重启时，缓存自动清空（内存数据不持久化）

    性能考量：
        - 字典查询的时间复杂度为 O(1)，性能优于数据库查询（通常 10-100ms）
        - 假设平均配置大小为 1KB，10000 个任务的内存占用约 10MB，可接受
        - 如果任务数超过百万级，应考虑使用 Redis 等外部缓存

    线程安全：
        - 当前实现不是线程安全的，仅适用于单线程或单进程的异步环境
        - 如果在多线程环境中使用，应添加锁（如 threading.Lock）保护字典操作
        - 在 asyncio 单线程事件循环中无需加锁（协程在同一线程中串行执行）

    Attributes:
        _cache (Dict[str, CachedConfig]): job_id -> 配置对象的映射

    Example:
        ```python
        cache = ConfigCache()

        # 写入缓存
        config = CachedConfig({"batch_size": 100}, version=1, schema_version=1)
        cache.set("task_1", config)

        # 读取缓存
        cached = cache.get("task_1")
        if cached:
            print(cached.config_json)  # {"batch_size": 100}

        # 清理不活跃的任务
        active_jobs = {"task_1", "task_2"}
        cache.prune(active_jobs)  # task_3, task_4 等会被删除
        ```

    Note:
        - 本类应作为全局单例使用（见模块底部的 `config_cache = ConfigCache()`）
        - 未来可扩展为支持 TTL（过期时间）、LRU 淘汰策略等高级功能
    """

    def __init__(self):
        """初始化配置缓存

        创建一个空的字典用于存储配置
        """
        self._cache: Dict[str, CachedConfig] = {}

    def get(self, job_id: str) -> Optional[CachedConfig]:
        """根据 job_id 获取缓存的配置

        Args:
            job_id (str): 任务的唯一标识符

        Returns:
            Optional[CachedConfig]: 缓存的配置对象
                如果缓存中不存在该 job_id，返回 None

        Note:
            - 返回 None 而非抛出异常，便于调用方判断是否需要从数据库加载
            - 典型用法：
                ```python
                cached = cache.get("task_1")
                if cached is None:
                    # 缓存未命中，从数据库加载
                    config = await db.fetch_config("task_1")
                    cache.set("task_1", config)
                else:
                    # 缓存命中，直接使用
                    config = cached
                ```
        """
        return self._cache.get(job_id)

    def set(self, job_id: str, config: CachedConfig):
        """将配置写入缓存

        Args:
            job_id (str): 任务的唯一标识符
            config (CachedConfig): 配置对象（不可变）

        Note:
            - 如果 job_id 已存在，会覆盖旧的配置（更新缓存）
            - 调用方应在从数据库加载配置后立即写入缓存，保持缓存与数据库一致
            - 不检查配置的有效性（假设调用方已验证），保持方法简单高效
        """
        self._cache[job_id] = config

    def prune(self, active_job_ids: Set[str]):
        """清理不活跃的任务配置（内存管理）

        移除所有不在 active_job_ids 集合中的缓存项，释放内存

        Args:
            active_job_ids (Set[str]): 当前活跃的任务 ID 集合
                通常从数据库查询所有 enabled=True 的任务 ID

        Example:
            ```python
            # 从数据库查询所有活跃任务
            active_jobs = await db.fetch_active_job_ids()

            # 清理缓存中的非活跃任务
            cache.prune(active_jobs)
            ```

        Note:
            - [修复 L4] 内存清理接口
            - 使用集合运算（差集）计算需要删除的键，时间复杂度为 O(n)，
              其中 n 为缓存中的键数量（通常远小于数据库中的任务总数）
            - 在迭代中修改字典是不安全的（可能导致 RuntimeError），
              因此先计算出需要删除的键集合，再逐个删除
            - 建议定期调用此方法（如每小时一次），防止长时间运行后内存膨胀
        """
        # 获取当前缓存中的所有 job_id
        current_keys = set(self._cache.keys())

        # 计算需要删除的 job_id（差集运算）
        to_remove = current_keys - active_job_ids

        # 逐个删除不活跃的配置
        for key in to_remove:
            del self._cache[key]

    def clear(self):
        """清空所有缓存

        移除缓存中的所有配置，通常在服务关闭或测试清理时使用

        Note:
            - 使用 dict.clear() 而非重新赋值（self._cache = {}），因为前者更高效且不会改变字典对象的引用
            - 在生产环境中应谨慎使用，因为清空缓存会导致下次调度时需要重新从数据库加载所有配置
        """
        self._cache.clear()


# 创建全局的配置缓存实例
config_cache = ConfigCache()
