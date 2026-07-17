"""验证安装后 TOML 配置来源、优先级和日志脱敏契约"""

import logging
from pathlib import Path

import pytest

from piko.config import log_config_sources, resolve_config_sources, settings


def test_config_source_precedence(tmp_path: Path) -> None:
    """验证 CWD 兼容层低于配置目录和显式文件"""
    cwd = tmp_path / "cwd"
    config_dir = tmp_path / "config"
    cwd.mkdir()
    config_dir.mkdir()
    for filename in ("settings.toml", ".secrets.toml", "piko.toml"):
        (cwd / filename).write_text("value = 1", encoding="utf-8")
    (config_dir / "piko.toml").write_text("value = 2", encoding="utf-8")
    explicit = tmp_path / "explicit.toml"
    explicit.write_text("value = 3", encoding="utf-8")

    sources, legacy_used = resolve_config_sources(
        {
            "PIKO_ENABLE_CWD_CONFIG": "true",
            "PIKO_CONFIG_DIR": str(config_dir),
            "PIKO_SETTINGS_PATH": str(explicit),
        },
        cwd,
    )

    assert sources == [
        cwd / "settings.toml",
        cwd / ".secrets.toml",
        cwd / "piko.toml",
        config_dir / "piko.toml",
        explicit,
    ]
    assert legacy_used is True


def test_settings_path_missing_fails_closed(tmp_path: Path) -> None:
    """验证显式配置文件不存在时启动前失败"""
    with pytest.raises(FileNotFoundError, match="PIKO_SETTINGS_PATH"):
        resolve_config_sources({"PIKO_SETTINGS_PATH": str(tmp_path / "missing.toml")}, tmp_path)


def test_config_dir_is_cwd_independent(tmp_path: Path) -> None:
    """验证未启用兼容层时不会读取 CWD 配置"""
    cwd = tmp_path / "cwd"
    config_dir = tmp_path / "config"
    cwd.mkdir()
    config_dir.mkdir()
    (cwd / "piko.toml").write_text("value = 1", encoding="utf-8")
    config_file = config_dir / "piko.toml"
    config_file.write_text("value = 2", encoding="utf-8")

    sources, legacy_used = resolve_config_sources({"PIKO_CONFIG_DIR": str(config_dir)}, cwd)

    assert sources == [config_file]
    assert legacy_used is False


def test_environment_configuration_is_loaded() -> None:
    """验证环境变量覆盖被 Dynaconf 读取"""
    assert str(settings.mysql_dsn).startswith("mysql+")


def test_config_log_redacts_values(caplog: pytest.LogCaptureFixture) -> None:
    """验证配置来源日志只包含键名，不包含敏感值"""
    caplog.set_level(logging.INFO, logger="piko.config")
    secret = "mysql+aiomysql://user:secret@example.test/piko"

    log_config_sources([], False, {"PIKO_MYSQL_DSN": secret, "PIKO_TOKEN": "token-value"})

    assert secret not in caplog.text
    assert "token-value" not in caplog.text
    assert "mysql_dsn" in caplog.text
