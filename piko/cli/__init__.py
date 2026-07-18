"""Piko 命令行工具

提供 ``piko db`` 子命令组，用于执行带 MySQL advisory lock 的单副本数据库迁移。
迁移脚本与 alembic 配置随 ``piko`` 包一起发布，安装后即可使用：

    uv run piko db upgrade
    uv run piko db current
    uv run piko db downgrade -1
"""

from piko.cli.main import main

__all__ = ["main"]
