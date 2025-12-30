import pytest
from pydantic import BaseModel
from piko.core.registry import registry, job

# 定义配置模型
class DemoConfig(BaseModel):
    threshold: int

# 异步测试需要 pytest-asyncio
@pytest.mark.asyncio
async def test_registry_flow():
    # 1. 定义并注册任务
    @job(job_id="test_job", schema=DemoConfig)
    async def handler(ctx, scheduled_time):
        return "ok"

    # 2. 验证注册成功
    assert registry.get_job("test_job") is handler
    assert registry.get_job("unknown_job") is None

    # 3. 验证配置校验 (成功情况)
    valid_cfg = {"threshold": 10}
    model = registry.validate_config("test_job", valid_cfg)
    assert isinstance(model, DemoConfig)
    assert model.threshold == 10

    # 4. 验证配置校验 (失败情况)
    invalid_cfg = {"threshold": "not_an_int"}
    with pytest.raises(Exception): # Pydantic ValidationError
        registry.validate_config("test_job", invalid_cfg)

    # 5. 验证非异步函数报错
    with pytest.raises(ValueError, match="must be an async function"):
        @job(job_id="bad_job")
        def sync_handler():
            pass
