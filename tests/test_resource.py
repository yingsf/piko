"""验证 Resource 文档示例的异步生命周期

示例不连接外部服务，只验证资源类的生成、注入值和释放顺序。
"""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import pytest

from piko.core.resource import resource


@pytest.mark.asyncio
async def test_documented_resource_lifecycle() -> None:
    """验证文档中的资源工厂可以注入并释放资源"""
    state = {"closed": False}

    @resource(name="memory")
    @asynccontextmanager
    async def memory_resource(
        ctx: dict[str, object],
    ) -> AsyncGenerator[dict[str, object], None]:
        try:
            yield {"job_id": ctx["job_id"]}
        finally:
            state["closed"] = True

    resource_type = memory_resource
    async with resource_type().acquire({"job_id": "resource-test"}) as value:
        assert value == {"job_id": "resource-test"}
        assert state["closed"] is False

    assert state["closed"] is True
