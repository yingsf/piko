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
    """

    def __init__(self, name: str):
        """初始化惰性 Logger 代理

        Args:
            name (str): Logger 的名称（通常是模块名，如 "piko.core.runner"）
        """
        self.name = name
        self._binds: dict[str, Any] = {"module": name}

    def bind(self, **kwargs):
        """绑定上下文信息（支持链式调用）

        将键值对绑定到 Logger，后续所有日志调用都会自动附加这些字段

        Args:
            **kwargs: 要绑定的键值对（如 job_id="task_1", run_id=12345）

        Returns:
            LazyLoggerProxy: 新的代理实例（包含累积的上下文），支持链式调用
        """
        new_proxy = LazyLoggerProxy(self.name)
        new_proxy._binds = {**self._binds, **kwargs}
        return new_proxy

    def _proxy_to_real(self, method_name: str, event: str, **kwargs):
        """代理到真正的 structlog Logger（核心转发逻辑）

        在每次调用日志方法时，才从 structlog 获取最新的 Logger，应用绑定的上下文，然后调用真正的日志方法

        Args:
            method_name (str): 日志方法名（如 "info"、"error"）
            event (str): 日志事件名称（第一个参数）
            **kwargs: 日志的额外字段
        """
        real_logger = structlog.get_logger()
        bound_logger = real_logger.bind(**self._binds)
        getattr(bound_logger, method_name)(event, **kwargs)

    def trace(self, event: str, **kwargs):
        """记录 TRACE 级别日志（映射为 DEBUG）"""
        self._proxy_to_real("debug", event, **kwargs)

    def debug(self, event: str, **kwargs):
        """记录 DEBUG 级别日志"""
        self._proxy_to_real("debug", event, **kwargs)

    def info(self, event: str, **kwargs):
        """记录 INFO 级别日志"""
        self._proxy_to_real("info", event, **kwargs)

    def warning(self, event: str, **kwargs):
        """记录 WARNING 级别日志"""
        self._proxy_to_real("warning", event, **kwargs)

    def error(self, event: str, **kwargs):
        """记录 ERROR 级别日志"""
        self._proxy_to_real("error", event, **kwargs)

    def critical(self, event: str, **kwargs):
        """记录 CRITICAL 级别日志"""
        self._proxy_to_real("critical", event, **kwargs)

    def exception(self, event: str, **kwargs):
        """记录异常日志（自动附加堆栈信息）

        Note:
            应在 except 块中调用，structlog 会自动捕获当前异常的堆栈
        """
        self._proxy_to_real("exception", event, **kwargs)


def _get_shared_processors():
    """获取共享的日志处理器列表"""
    return [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.dev.set_exc_info,
        structlog.processors.TimeStamper(fmt="iso", utc=False),
    ]


def setup_logging():
    """全局初始化日志系统

    本函数配置 structlog 和标准库 logging，确保：
        1. 所有日志使用统一的格式（JSON 或 Console）
        2. 标准库的日志（如 uvicorn、apscheduler）也通过 structlog 输出
    """
    shared_processors = _get_shared_processors()

    if settings.log_json:
        processors = shared_processors + [
            structlog.processors.dict_tracebacks,
            structlog.processors.JSONRenderer(),
        ]
    else:
        processors = shared_processors + [
            structlog.dev.ConsoleRenderer(),
        ]

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=False,
    )

    _configure_stdlib_logging(processors)


def _configure_stdlib_logging(processors):
    """拦截标准库 logging，转发给 structlog"""
    pre_chain = [
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.contextvars.merge_contextvars,
        structlog.processors.TimeStamper(fmt="iso", utc=False),
    ]

    logging.config.dictConfig({
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "structlog_formatter": {
                "()": structlog.stdlib.ProcessorFormatter,
                "processor": processors[-1],
                "foreign_pre_chain": pre_chain,
            },
        },
        "handlers": {
            "default": {
                "level": "INFO",
                "class": "logging.StreamHandler",
                "formatter": "structlog_formatter"
            },
        },
        "root": {
            "handlers": ["default"],
            "level": settings.log_level.upper()
        },
        "loggers": {
            "uvicorn": {"handlers": ["default"], "level": "INFO", "propagate": False},
            "uvicorn.error": {"handlers": ["default"], "level": "INFO", "propagate": False},
            "uvicorn.access": {"handlers": ["default"], "level": "INFO", "propagate": False},
            "apscheduler": {"handlers": ["default"], "level": "INFO", "propagate": False},
        }
    })


def get_logger(name: str | None = None):
    """获取 Logger（推荐使用 `get_logger(__name__)`）

    Returns:
        LazyLoggerProxy: 惰性 Logger 代理实例
    """
    if name is None:
        try:
            name = sys._getframe(1).f_globals.get("__name__", "piko")
        except (AttributeError, ValueError):
            name = "piko"

    return LazyLoggerProxy(name)
