import os
import logging
from importlib import metadata
from collections.abc import Mapping
from pathlib import Path

from dynaconf import Dynaconf, Validator

_LIB_CONFIG_DIR = Path(__file__).parent
_logger = logging.getLogger("piko.config")

# _DEFAULT_SETTINGS 指向库内置的 defaults.toml
_DEFAULT_SETTINGS = _LIB_CONFIG_DIR / "defaults.toml"


def _is_enabled(value: str | None) -> bool:
    """解析显式布尔环境变量。"""
    return value is not None and value.strip().lower() in {"1", "true", "yes", "on"}


def _require_absolute_file(raw_path: str, name: str) -> Path:
    """校验显式配置文件路径，拒绝相对路径和不可读文件。"""
    path = Path(raw_path).expanduser()
    if not path.is_absolute():
        raise ValueError(f"{name} must be an absolute path")
    if not path.is_file():
        raise FileNotFoundError(f"{name} does not point to a regular file: {path}")
    if not os.access(path, os.R_OK):
        raise PermissionError(f"{name} is not readable: {path}")
    return path


def resolve_config_sources(
    environ: Mapping[str, str], cwd: Path | None = None
) -> tuple[list[Path], bool]:
    """解析外部 TOML 文件来源，返回按优先级排列的路径和兼容层状态。"""
    working_dir = cwd or Path.cwd()
    sources: list[Path] = []
    legacy_used = False

    if _is_enabled(environ.get("PIKO_ENABLE_CWD_CONFIG")):
        for filename in ("settings.toml", ".secrets.toml", "piko.toml"):
            candidate = working_dir / filename
            if candidate.is_file() and os.access(candidate, os.R_OK):
                sources.append(candidate)
                legacy_used = True

    config_dir_raw = environ.get("PIKO_CONFIG_DIR")
    if config_dir_raw:
        config_dir = Path(config_dir_raw).expanduser()
        if not config_dir.is_absolute():
            raise ValueError("PIKO_CONFIG_DIR must be an absolute path")
        if not config_dir.is_dir() or not os.access(config_dir, os.R_OK):
            raise FileNotFoundError(f"PIKO_CONFIG_DIR is not a readable directory: {config_dir}")
        config_file = config_dir / "piko.toml"
        if config_file.is_file() and os.access(config_file, os.R_OK):
            sources.append(config_file)

    settings_path_raw = environ.get("PIKO_SETTINGS_PATH")
    if settings_path_raw:
        sources.append(_require_absolute_file(settings_path_raw, "PIKO_SETTINGS_PATH"))

    deduplicated: list[Path] = []
    for source in sources:
        if source not in deduplicated:
            deduplicated.append(source)
    return deduplicated, legacy_used


def log_config_sources(sources: list[Path], legacy_used: bool, environ: Mapping[str, str]) -> None:
    """记录配置来源和环境覆盖键名，不记录任何配置值。"""
    control_keys = {
        "PIKO_CONFIG_DIR",
        "PIKO_SETTINGS_PATH",
        "PIKO_ENABLE_CWD_CONFIG",
    }
    override_keys = sorted(
        key.removeprefix("PIKO_").lower()
        for key in environ
        if key.startswith("PIKO_") and key not in control_keys
    )
    source_names = [str(source) for source in sources]
    _logger.info(
        "config_sources sources=%s legacy_cwd_used=%s env_override_keys=%s",
        source_names,
        legacy_used,
        override_keys,
        extra={
            "config_sources": source_names,
            "legacy_cwd_used": legacy_used,
            "env_override_keys": override_keys,
        },
    )


_config_sources, _legacy_cwd_used = resolve_config_sources(os.environ)
log_config_sources(_config_sources, _legacy_cwd_used, os.environ)
if _legacy_cwd_used:
    _logger.warning("legacy_cwd_config_loaded")
elif _is_enabled(os.environ.get("PIKO_ENABLE_CWD_CONFIG")):
    _logger.warning("legacy_cwd_config_not_found")

# ============================================================================
# Dynaconf 配置加载策略
# ============================================================================
settings = Dynaconf(
    envvar_prefix="PIKO",
    preload=[str(_DEFAULT_SETTINGS)],
    settings_files=[str(source) for source in _config_sources],
    environments=True,
    load_dotenv=False,
    validators=[
        Validator("mysql_dsn", must_exist=True),
        Validator("leader_name", must_exist=True),
    ],
)

# ============================================================================
# 动态版本注入
# ============================================================================
# 尝试从安装包元数据中读取版本号 (pyproject.toml)，覆盖配置中的默认值
try:
    # 如果执行过 pip install . 或 pip install -e .，这里能读到版本
    _dist_version = metadata.version("piko-cucc")
    settings.version = _dist_version
except metadata.PackageNotFoundError:
    # 未安装模式（纯源码运行），保持 defaults.toml 中的默认值
    pass

# 配置验证执行
settings.validators.validate()

# 模块导出
__all__ = ["log_config_sources", "resolve_config_sources", "settings"]
