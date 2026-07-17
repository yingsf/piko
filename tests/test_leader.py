"""Leader fencing token 的回归测试。"""

from collections.abc import Awaitable, Callable
from datetime import datetime
from types import SimpleNamespace
from typing import Any, cast

from piko.infra.leader import LeaderMutex


class _Result:
    """模拟 SQLAlchemy UPDATE 结果。"""

    rowcount = 1


class _Session:
    """模拟会同步 ORM 行版本的异步数据库会话。"""

    def __init__(self, row: Any) -> None:
        self.row = row

    async def execute(self, _statement: object) -> _Result:
        """模拟 CAS 成功并同步 ORM 行对象。"""
        self.row.version += 1
        return _Result()

    async def commit(self) -> None:
        """模拟事务提交。"""


async def test_cas_fencing_token_uses_database_version() -> None:
    """验证 ORM 同步版本后，内存 fencing token 仍等于数据库版本。"""
    row = SimpleNamespace(version=10)
    session = _Session(row)
    mutex = LeaderMutex()

    perform_cas_update = cast(Callable[..., Awaitable[bool]], getattr(mutex, "_perform_cas_update"))
    acquired = await perform_cas_update(
        cast(Any, session),
        cast(Any, row),
        now=datetime.now(),
        new_lease_until=datetime.now(),
    )

    assert acquired is True
    assert row.version == 11
    assert vars(mutex)["_current_version"] == 11
