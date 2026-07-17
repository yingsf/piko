"""数据库连接配置的单元测试

验证异步 MySQL DSN 的驱动选择和兼容别名行为，不建立数据库连接。
"""

import pytest

from piko.infra.db import normalize_mysql_dsn


def test_asyncmy_dsn_uses_aiomysql_driver() -> None:
    """验证 asyncmy 配置别名解析为 aiomysql 驱动"""
    url = normalize_mysql_dsn("mysql+asyncmy://user:secret@example.test:3306/piko?charset=utf8mb4")

    assert url.drivername == "mysql+aiomysql"
    assert url.username == "user"
    assert url.password == "secret"
    assert url.host == "example.test"
    assert url.query["charset"] == "utf8mb4"


def test_aiomysql_dsn_is_preserved() -> None:
    """验证原生 aiomysql DSN 保持不变"""
    url = normalize_mysql_dsn("mysql+aiomysql://user:secret@example.test/piko")

    assert url.drivername == "mysql+aiomysql"


def test_sync_mysql_driver_is_rejected() -> None:
    """验证同步 MySQL 驱动配置被拒绝"""
    with pytest.raises(ValueError, match=r"mysql\+aiomysql"):
        normalize_mysql_dsn("mysql+pymysql://user:secret@example.test/piko")
