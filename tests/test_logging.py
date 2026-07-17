"""惰性日志代理的缓存行为测试。"""

from typing import Any

import structlog

from piko.infra.logging import LazyLoggerProxy


class _FakeBoundLogger:
    """记录 bind 和日志调用次数的测试 logger。"""

    def __init__(self) -> None:
        self.bind_calls = 0
        self.events: list[tuple[str, str, dict[str, object]]] = []

    def bind(self, **kwargs: object) -> "_FakeBoundLogger":
        """记录一次上下文绑定并返回自身。"""
        self.bind_calls += 1
        return self

    def info(self, event: str, **kwargs: object) -> None:
        """记录 INFO 事件。"""
        self.events.append(("info", event, kwargs))


def test_lazy_logger_caches_bound_logger(monkeypatch: Any) -> None:
    """验证同一代理重复记录时只创建一次 bound logger。"""
    fake_logger = _FakeBoundLogger()
    get_logger_calls = 0

    def get_logger() -> _FakeBoundLogger:
        nonlocal get_logger_calls
        get_logger_calls += 1
        return fake_logger

    monkeypatch.setattr(structlog, "get_logger", get_logger)
    proxy = LazyLoggerProxy("test.module")

    proxy.info("first", value=1)
    proxy.info("second", value=2)

    assert get_logger_calls == 1
    assert fake_logger.bind_calls == 1
    assert [event[1] for event in fake_logger.events] == ["first", "second"]
