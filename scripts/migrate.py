#!/usr/bin/env python3
"""迁移脚本薄包装（开发期使用）

迁移逻辑已下沉到 ``piko.cli.migrate`` 并随 ``piko`` 包发布。生产环境请使用
安装后的命令行入口：

    uv run piko db upgrade
    uv run piko db current
    uv run piko db downgrade -1

本脚本仅用于仓库内开发流程的向后兼容，等价于 ``piko db``。

用法：``python scripts/migrate.py upgrade``，默认目标为 ``head``。
"""

from __future__ import annotations

import argparse
import os

from piko.cli import migrate


def main() -> int:
    """解析迁移参数并转发到 piko.cli.migrate

    兼容旧脚本的命令格式：upgrade/downgrade/current + 可选 revision。
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("upgrade", "downgrade", "current"))
    parser.add_argument("revision", nargs="?", default="head")
    parser.add_argument(
        "--lock-timeout-s",
        type=int,
        default=int(os.environ.get("PIKO_MIGRATION_LOCK_TIMEOUT_S", "30")),
    )
    args = parser.parse_args()
    if args.lock_timeout_s < 0:
        parser.error("--lock-timeout-s must be non-negative")

    if args.command == "upgrade":
        return migrate.run_upgrade(args.revision, lock_timeout_s=args.lock_timeout_s)
    if args.command == "downgrade":
        return migrate.run_downgrade(args.revision, lock_timeout_s=args.lock_timeout_s)
    return migrate.show_current()


if __name__ == "__main__":
    raise SystemExit(main())
