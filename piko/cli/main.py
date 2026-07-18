"""Piko 命令行入口

支持子命令：

    piko db upgrade [revision]      升级到指定版本（默认 head）
    piko db downgrade [revision]    降级（默认 -1，回退一步）
    piko db current                 查看当前 schema 版本
    piko db history                 查看迁移版本链

数据库连接通过 ``PIKO_MYSQL_DSN`` 配置（与应用启动使用同一配置源）。
迁移执行期间持有 MySQL advisory lock，保证多副本部署时单副本执行 DDL。
"""

from __future__ import annotations

import argparse

from piko.cli import migrate


def _build_parser() -> argparse.ArgumentParser:
    """构造 ``piko`` 顶层命令解析器

    Returns:
        配置好子命令的 ArgumentParser。
    """
    parser = argparse.ArgumentParser(
        prog="piko",
        description="Piko 命令行工具：数据库迁移与运维",
    )
    subparsers = parser.add_subparsers(dest="command", required=True, metavar="<command>")

    db_parser = subparsers.add_parser("db", help="数据库迁移管理")
    db_sub = db_parser.add_subparsers(dest="db_command", required=True, metavar="<db-command>")

    upgrade_parser = db_sub.add_parser("upgrade", help="升级数据库 schema（默认到 head）")
    upgrade_parser.add_argument("revision", nargs="?", default="head", help="目标版本，默认 head")
    upgrade_parser.add_argument(
        "--lock-timeout-s",
        type=int,
        default=None,
        help="advisory lock 等待秒数（默认读 PIKO_MIGRATION_LOCK_TIMEOUT_S=30）",
    )
    upgrade_parser.set_defaults(func=_cmd_db_upgrade)

    downgrade_parser = db_sub.add_parser("downgrade", help="降级数据库 schema（默认 -1）")
    downgrade_parser.add_argument("revision", nargs="?", default="-1", help="目标版本，默认 -1")
    downgrade_parser.add_argument(
        "--lock-timeout-s",
        type=int,
        default=None,
        help="advisory lock 等待秒数（默认读 PIKO_MIGRATION_LOCK_TIMEOUT_S=30）",
    )
    downgrade_parser.set_defaults(func=_cmd_db_downgrade)

    current_parser = db_sub.add_parser("current", help="查看当前 schema 版本")
    current_parser.set_defaults(func=_cmd_db_current)

    history_parser = db_sub.add_parser("history", help="查看迁移版本链")
    history_parser.set_defaults(func=_cmd_db_history)

    return parser


def _cmd_db_upgrade(args: argparse.Namespace) -> int:
    """执行 ``piko db upgrade``"""
    return migrate.run_upgrade(args.revision, lock_timeout_s=args.lock_timeout_s)


def _cmd_db_downgrade(args: argparse.Namespace) -> int:
    """执行 ``piko db downgrade``"""
    return migrate.run_downgrade(args.revision, lock_timeout_s=args.lock_timeout_s)


def _cmd_db_current(_args: argparse.Namespace) -> int:
    """执行 ``piko db current``"""
    return migrate.show_current()


def _cmd_db_history(_args: argparse.Namespace) -> int:
    """执行 ``piko db history``"""
    return migrate.show_history()


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
