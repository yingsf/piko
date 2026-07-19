"""Piko schema CLI 的单元测试。"""

from piko.cli import schema
from piko.cli.main import _build_parser


def test_parser_db_init_and_upgrade_have_no_revision_argument() -> None:
    """验证 schema 命令不暴露版本号选择。"""
    parser = _build_parser()
    init_args = parser.parse_args(["db", "init", "--lock-timeout-s", "5"])
    assert init_args.db_command == "init"
    assert init_args.lock_timeout_s == 5

    upgrade_args = parser.parse_args(["db", "upgrade"])
    assert upgrade_args.db_command == "upgrade"
    assert upgrade_args.lock_timeout_s is None


def test_parser_db_check_repair_and_compatibility_aliases() -> None:
    """验证检查、修复和旧命令别名仍可解析。"""
    parser = _build_parser()
    assert parser.parse_args(["db", "check"]).db_command == "check"
    assert parser.parse_args(["db", "repair"]).db_command == "repair"


def test_resolve_timeout_reads_schema_env(monkeypatch) -> None:
    """验证 schema lock 超时读取新的环境变量。"""
    monkeypatch.setenv("PIKO_SCHEMA_LOCK_TIMEOUT_S", "42")
    assert schema._resolve_timeout(None) == 42
    assert schema._resolve_timeout(7) == 7
