import logging
import logging.config
import sys
from typing import Any

import structlog

from piko.config import settings


class LazyLoggerProxy:
    """惰性 Logger 代理（解决全局变量过时问题）

    本类实现了 Lazy Loading 模式，延迟加载真正的 structlog Logger
    在每次调用日志方法（如 `info()`、`error()`）时，才从 structlog 获取最新的 Logger，确保使用最新的日志配置（如 JSON 渲染器）

    Attributes:
        name (str): Logger 的名称（通常是模块名，如 "piko.core.runner"）
        _binds (dict): 绑定的上下文字典（如 {"job_id": "task_1", "run_id": 12345}）

    Note:
        - 本类不直接使用 `structlog.get_logger(__name__)`，
          而是在每次调用日志方法时动态获取，确保使用最新配置
        - `_binds` 字典会在每次 `bind()` 调用时累积，支持链式调用
        - 本类实现了常见的日志方法（debug、info、warning、error、critical、exception），未实现的方法不会被代理（可按需扩展）
    """

    def __init__(self, name: str):
        """初始化惰性 Logger 代理

        Args:
            name (str): Logger 的名称（通常是模块名，如 "piko.core.runner"）
        """
        # 保存 Logger 的名称
        self.name = name

        # 默认绑定 module 字段
        self._binds: dict[str, Any] = {"module": name}

    def bind(self, **kwargs):
        """绑定上下文信息（支持链式调用）

        将键值对绑定到 Logger，后续所有日志调用都会自动附加这些字段

        Args:
            **kwargs: 要绑定的键值对（如 job_id="task_1", run_id=12345）

        Returns:
            LazyLoggerProxy: 新的代理实例（包含累积的上下文），支持链式调用

        Note:
            - 返回新的 LazyLoggerProxy 实例，不修改原实例（不可变设计）
            - 支持多次链式调用：`logger.bind(a=1).bind(b=2).info("event")`
            - 后绑定的字段会覆盖先绑定的同名字段（字典合并规则）
        """
        # 创建新的代理实例（保持不可变性）
        new_proxy = LazyLoggerProxy(self.name)

        # 合并上下文：先复制旧的上下文，再更新新的上下文
        new_proxy._binds = {**self._binds, **kwargs}

        return new_proxy

    def _proxy_to_real(self, method_name: str, event: str, **kwargs):
        """代理到真正的 structlog Logger（核心转发逻辑）

        在每次调用日志方法时，才从 structlog 获取最新的 Logger，应用绑定的上下文，然后调用真正的日志方法

        Args:
            method_name (str): 日志方法名（如 "info"、"error"）
            event (str): 日志事件名称（第一个参数，如 "task_started"）
            **kwargs: 日志的额外字段（如 job_id="task_1"）

        Note:
            - 每次调用都会重新获取 Logger，确保使用最新配置
            - `structlog.get_logger()` 不会创建新实例，而是返回配置好的单例
            - `bind(**self._binds)` 应用之前累积的上下文
            - `getattr(bound_logger, method_name)` 动态调用日志方法（如 info、error）
        """
        # 关键：每次打日志时，才去获取真正的 structlog logger
        real_logger = structlog.get_logger()

        # 应用之前 bind 的上下文
        bound_logger = real_logger.bind(**self._binds)

        # 调用真正的方法 (info, error, etc.)
        getattr(bound_logger, method_name)(event, **kwargs)

    def debug(self, event: str, **kwargs):
        """记录 DEBUG 级别日志

        Args:
            event (str): 日志事件名称
            **kwargs: 额外的日志字段
        """
        self._proxy_to_real("debug", event, **kwargs)

    def info(self, event: str, **kwargs):
        """记录 INFO 级别日志

        Args:
            event (str): 日志事件名称
            **kwargs: 额外的日志字段
        """
        self._proxy_to_real("info", event, **kwargs)

    def warning(self, event: str, **kwargs):
        """记录 WARNING 级别日志

        Args:
            event (str): 日志事件名称
            **kwargs: 额外的日志字段
        """
        self._proxy_to_real("warning", event, **kwargs)

    def error(self, event: str, **kwargs):
        """记录 ERROR 级别日志

        Args:
            event (str): 日志事件名称
            **kwargs: 额外的日志字段
        """
        self._proxy_to_real("error", event, **kwargs)

    def critical(self, event: str, **kwargs):
        """记录 CRITICAL 级别日志

        Args:
            event (str): 日志事件名称
            **kwargs: 额外的日志字段
        """
        self._proxy_to_real("critical", event, **kwargs)

    def exception(self, event: str, **kwargs):
        """记录异常日志（自动附加堆栈信息）

        Args:
            event (str): 日志事件名称
            **kwargs: 额外的日志字段

        Note:
            - 应在 except 块中调用，structlog 会自动捕获当前异常的堆栈
            - 堆栈信息会序列化为 JSON 字段（在 JSON 模式下）或格式化输出（在 Console 模式下）
        """
        self._proxy_to_real("exception", event, **kwargs)


def _get_shared_processors():
    """获取共享的日志处理器列表

    本函数返回一组通用的 structlog 处理器，用于所有日志输出（JSON 和 Console）

    处理器说明：
        1. **merge_contextvars**: 合并 contextvars 中的上下文（如请求 ID）
        2. **add_log_level**: 添加日志级别字段（如 "level": "info"）
        3. **StackInfoRenderer**: 渲染堆栈信息（在 `logger.exception()` 时使用）
        4. **set_exc_info**: 捕获当前异常信息（在 `logger.exception()` 时使用）
        5. **TimeStamper**: 添加时间戳字段（ISO 8601 格式，如 "2025-12-30T10:30:00"）

    Returns:
        list: structlog 处理器列表

    Note:
        - 处理器的顺序很重要，某些处理器依赖于前面处理器的输出
        - `TimeStamper(utc=False)` 使用本地时区，如果需要 UTC 时间应设为 True
    """
    return [
        # 合并 contextvars 中的上下文
        # 使用场景：在 Web 应用中，可以在中间件中将请求 ID 存储到 contextvars，所有后续日志都会自动附加请求 ID，无需手动传递
        structlog.contextvars.merge_contextvars,

        # 添加日志级别字段（如 "level": "info"）
        # 必须在渲染器之前，因为渲染器需要读取 level 字段
        structlog.processors.add_log_level,

        # 渲染堆栈信息（将堆栈转换为字符串）
        # 仅在调用 logger.exception() 或传入 exc_info=True 时生效
        structlog.processors.StackInfoRenderer(),

        # 捕获当前异常信息（在 except 块中调用时自动附加）
        # 必须在 StackInfoRenderer 之前，因为后者依赖于 exc_info 字段
        structlog.dev.set_exc_info,

        # 添加时间戳字段（ISO 8601 格式）
        # fmt="iso": 格式为 "2025-12-30T10:30:00.123456"
        # utc=False: 使用本地时区（如果需要 UTC 时间应设为 True）
        structlog.processors.TimeStamper(fmt="iso", utc=False),
    ]


def setup_logging():
    """全局初始化日志系统

    本函数配置 structlog 和标准库 logging，确保：
        1. 所有日志使用统一的格式（JSON 或 Console）
        2. 标准库的日志（如 uvicorn、apscheduler）也通过 structlog 输出
        3. LazyLoggerProxy 能够使用最新的配置

    配置流程：
        1. 根据 `settings.log_json` 选择渲染器（JSON 或 Console）
        2. 配置 structlog（处理器链、日志级别、Logger 工厂）
        3. 配置标准库 logging（拦截标准库日志，转发给 structlog）

    Note:
        - 应在应用启动时尽早调用（在导入其他模块之前）
        - 如果在多进程环境中，应在每个进程中独立调用
        - `cache_logger_on_first_use=False` 是关键，确保 LazyLoggerProxy 能使用最新配置
    """
    # 获取共享的处理器列表
    shared_processors = _get_shared_processors()

    # 根据配置选择渲染器
    if settings.log_json:
        # JSON 渲染器配置
        processors = shared_processors + [
            # 将 Python 异常对象转换为字典（便于 JSON 序列化）
            # 例如：{"exc_type": "ValueError", "exc_value": "invalid input", "exc_traceback": [...]}
            structlog.processors.dict_tracebacks,

            # JSON 渲染器：将日志事件序列化为 JSON 字符串
            # 输出示例：{"event": "task_started", "level": "info", "timestamp": "2025-12-30T10:30:00"}
            structlog.processors.JSONRenderer(),
        ]
    else:
        # Console 渲染器配置（彩色格式化输出）
        processors = shared_processors + [
            structlog.dev.ConsoleRenderer(),
        ]

    # 配置 structlog
    structlog.configure(
        processors=processors,

        # 包装类：过滤日志级别
        # make_filtering_bound_logger(logging.INFO): 只输出 INFO 及以上级别的日志
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),

        # 上下文存储类：使用普通字典
        context_class=dict,

        # Logger 工厂：使用 PrintLoggerFactory（直接打印到 stdout）
        logger_factory=structlog.PrintLoggerFactory(),

        # 必须关闭缓存，否则 LazyProxy 在配置生效前创建的 logger 无法切换到 JSON 模式
        cache_logger_on_first_use=False,
    )

    # 接管标准库 logging
    _configure_stdlib_logging(processors)


def _configure_stdlib_logging(processors):
    """拦截标准库 logging，转发给 structlog

    本函数配置 Python 标准库的 logging 模块，将所有日志转发给 structlog，确保第三方库（如 uvicorn、apscheduler）的日志也使用统一格式

    Args:
        processors (list): structlog 处理器列表（最后一个应是渲染器）

    Note:
        - `foreign_pre_chain` 是转换标准库日志为 structlog 事件的预处理器链
        - `disable_existing_loggers=False` 确保不会禁用已存在的 Logger（如 uvicorn）
    """
    # 预处理器链：将标准库日志转换为 structlog 事件
    pre_chain = [
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.contextvars.merge_contextvars,
        structlog.processors.TimeStamper(fmt="iso", utc=False),
    ]

    # 配置标准库 logging
    logging.config.dictConfig({
        "version": 1,

        # 不禁用已存在的 Logger（如 uvicorn 在启动时已创建 Logger），如果设为 True，uvicorn 的日志会丢失
        "disable_existing_loggers": False,

        # 格式化器：使用 structlog 的 ProcessorFormatter
        "formatters": {
            "structlog_formatter": {
                "()": structlog.stdlib.ProcessorFormatter,
                "processor": processors[-1],
                "foreign_pre_chain": pre_chain,
            },
        },

        # 处理器：输出到 stderr
        "handlers": {
            "default": {
                "level": "INFO",
                "class": "logging.StreamHandler",
                "formatter": "structlog_formatter"
            },
        },

        # 根 Logger 配置
        "root": {
            "handlers": ["default"],
            "level": settings.log_level.upper()
        },

        # 第三方库的 Logger 配置
        "loggers": {
            "uvicorn": {"handlers": ["default"], "level": "INFO", "propagate": False},
            "uvicorn.error": {"handlers": ["default"], "level": "INFO", "propagate": False},
            "uvicorn.access": {"handlers": ["default"], "level": "INFO", "propagate": False},
            "apscheduler": {"handlers": ["default"], "level": "INFO", "propagate": False},
        }
    })


def get_logger(name: str | None = None):
    """获取 Logger（推荐使用 `get_logger(__name__)`）

    本函数返回一个 LazyLoggerProxy 实例，支持延迟加载和上下文绑定

    Args:
        name (str | None): Logger 的名称（通常是模块名，如 "piko.core.runner"）
            - 如果提供，直接使用该名称
            - 如果为 None，自动推断调用者的模块名（通过栈帧分析）

    Returns:
        LazyLoggerProxy: 惰性 Logger 代理实例
    """
    if name is None:
        try:
            name = sys._getframe(1).f_globals.get("__name__", "piko")
        except (AttributeError, ValueError):
            name = "piko"

    # 返回 LazyLoggerProxy 实例
    return LazyLoggerProxy(name)
