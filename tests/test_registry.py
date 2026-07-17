from typing import cast

import pytest
from pydantic import BaseModel, ValidationError

from piko import PikoApp
from piko.core.registry import JobHandler


class DemoConfig(BaseModel):
    """注册表测试使用的配置模型"""

    threshold: int


class DefaultConfig(BaseModel):
    """验证缺省配置时的 Schema 默认值"""

    threshold: int = 10


def test_registry_flow() -> None:
    """验证应用实例中的任务注册和配置校验"""
    app = PikoApp(name="registry-test")

    @app.job(job_id="test_job", schema=DemoConfig)
    async def handler(ctx: dict[str, object], scheduled_time: object) -> str:
        return "ok"

    assert app.registry.get_job("test_job") is handler
    assert app.registry.get_job("unknown_job") is None

    model = app.registry.validate_config("test_job", {"threshold": 10})
    assert isinstance(model, DemoConfig)
    assert model.threshold == 10

    with pytest.raises(ValidationError):
        app.registry.validate_config("test_job", {"threshold": "not-an-int"})

    @app.job(job_id="default_config_job", schema=DefaultConfig)
    async def default_config_handler(ctx: dict[str, object], scheduled_time: object) -> str:
        return "ok"

    assert app.registry.get_job("default_config_job") is default_config_handler
    default_config = app.registry.validate_config("default_config_job", {})
    assert isinstance(default_config, DefaultConfig)
    assert default_config.threshold == 10

    def sync_handler() -> None:
        return None

    with pytest.raises(ValueError, match="must be an async function"):
        app.job("bad_job")(cast(JobHandler, sync_handler))


def test_registry_reregister_without_schema_clears_old_schema() -> None:
    """验证任务重注册时省略 schema 会移除旧配置校验器。"""
    app = PikoApp(name="registry-reregister-test")

    @app.job(job_id="replaceable", schema=DemoConfig)
    async def first_handler(ctx: dict[str, object], scheduled_time: object) -> str:
        return "first"

    assert app.registry.get_job("replaceable") is first_handler
    assert app.registry.validate_config("replaceable", {"threshold": 1}) == DemoConfig(threshold=1)

    @app.job(job_id="replaceable")
    async def second_handler(ctx: dict[str, object], scheduled_time: object) -> str:
        return "second"

    assert app.registry.get_job("replaceable") is second_handler
    assert app.registry.validate_config("replaceable", {"other": "value"}) == {"other": "value"}
