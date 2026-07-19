#!/usr/bin/env python3
"""schema 收敛脚本薄包装（开发期使用）。

生产环境请使用安装后的 ``piko db`` 命令。本脚本保留仓库内开发习惯，
用法为 ``python scripts/migrate.py init`` 或 ``check``。
"""

from __future__ import annotations

import argparse
import os

from piko.cli import schema


def main() -> int:
    """解析 schema 参数并转发到 piko.cli.schema。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("init", "upgrade", "repair", "check"))
    parser.add_argument(
        "--lock-timeout-s",
        type=int,
        default=int(os.environ.get("PIKO_SCHEMA_LOCK_TIMEOUT_S", "30")),
    )
    args = parser.parse_args()
    if args.lock_timeout_s < 0:
        parser.error("--lock-timeout-s must be non-negative")

    if args.command == "init":
        return schema.run_init(lock_timeout_s=args.lock_timeout_s)
    if args.command == "upgrade":
        return schema.run_upgrade(lock_timeout_s=args.lock_timeout_s)
    if args.command == "repair":
        return schema.run_repair(lock_timeout_s=args.lock_timeout_s)
    return schema.run_check()


if __name__ == "__main__":
    raise SystemExit(main())
