"""测试环境的最小配置和外部依赖边界

集成测试只允许通过 PIKO_TEST_MYSQL_DSN 指定测试数据库，普通单元测试不建立数据库连接。
"""

import os
from collections.abc import AsyncGenerator

import pytest_asyncio


test_dsn = os.environ.get("PIKO_TEST_MYSQL_DSN")
if test_dsn:
    os.environ["PIKO_MYSQL_DSN"] = test_dsn
else:
    os.environ.setdefault(
        "PIKO_MYSQL_DSN",
        "mysql+asyncmy://test:test@127.0.0.1:3306/piko_test?charset=utf8mb4",
    )


@pytest_asyncio.fixture(autouse=True)
async def reset_database_engine_after_test() -> AsyncGenerator[None, None]:
    """在每个测试后释放异步引擎，避免跨事件循环复用连接。"""
    import piko.infra.db as db_infra

    yield
    await db_infra.reset_db()
