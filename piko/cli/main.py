"""Piko 命令行入口

支持子命令：

    piko db init                     初始化或补齐数据库 schema
    piko db upgrade                  init 的兼容别名
    piko db check                    检查 schema 是否同步
    piko db repair                   重试幂等 schema 收敛

数据库连接通过 ``PIKO_MYSQL_DSN`` 配置（与应用启动使用同一配置源）。
schema 收敛期间持有 MySQL advisory lock，保证多副本部署时单副本执行 DDL。
"""

from __future__ import annotations

import argparse
from collections.abc import Callable

from piko.cli import schema


def _build_parser() -> argparse.ArgumentParser:
    """构造 ``piko`` 顶层命令解析器

    Returns:
        配置好子命令的 ArgumentParser。
    """
    parser = argparse.ArgumentParser(
        prog="piko",
        description="Piko 命令行工具：数据库 schema 与运维",
    )
    subparsers = parser.add_subparsers(dest="command", required=True, metavar="<command>")

    db_parser = subparsers.add_parser("db", help="数据库 schema 管理")
    db_sub = db_parser.add_subparsers(dest="db_command", required=True, metavar="<db-command>")

    schema_commands: tuple[tuple[str, str, Callable[[argparse.Namespace], int]], ...] = (
        ("init", "初始化或补齐数据库 schema", _cmd_db_init),
        ("upgrade", "兼容别名：初始化或补齐数据库 schema", _cmd_db_upgrade),
        ("repair", "重试幂等 schema 收敛", _cmd_db_repair),
    )
    for command, help_text, handler in schema_commands:
        command_parser = db_sub.add_parser(command, help=help_text)
        command_parser.add_argument(
            "--lock-timeout-s",
            type=int,
            default=None,
            help="schema lock 等待秒数（默认读 PIKO_SCHEMA_LOCK_TIMEOUT_S=30）",
        )
        command_parser.set_defaults(func=handler)

    inspection_commands: tuple[tuple[str, str, Callable[[argparse.Namespace], int]], ...] = (
        ("check", "检查 schema 是否同步", _cmd_db_check),
    )
    for command, help_text, handler in inspection_commands:
        command_parser = db_sub.add_parser(command, help=help_text)
        command_parser.set_defaults(func=handler)

    return parser


def _cmd_db_init(args: argparse.Namespace) -> int:
    """执行 ``piko db init``"""
    return schema.run_init(lock_timeout_s=args.lock_timeout_s)


def _cmd_db_upgrade(args: argparse.Namespace) -> int:
    """执行 ``piko db upgrade``"""
    return schema.run_upgrade(lock_timeout_s=args.lock_timeout_s)


def _cmd_db_repair(args: argparse.Namespace) -> int:
    """执行 ``piko db repair``"""
    return schema.run_repair(lock_timeout_s=args.lock_timeout_s)


def _cmd_db_check(_args: argparse.Namespace) -> int:
    """执行 ``piko db check``"""
    return schema.run_check()


def main() -> int:
    """解析命令行参数并分发到对应子命令

    Returns:
        进程退出码。
    """
    parser = _build_parser()
    args = parser.parse_args()
    if getattr(args, "lock_timeout_s", None) is not None and args.lock_timeout_s < 0:
        parser.error("--lock-timeout-s 必须非负")
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
