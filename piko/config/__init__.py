import os
from importlib import metadata
from pathlib import Path

from dynaconf import Dynaconf, Validator

_LIB_CONFIG_DIR = Path(__file__).parent

# _DEFAULT_SETTINGS 指向库内置的 defaults.toml
_DEFAULT_SETTINGS = _LIB_CONFIG_DIR / "defaults.toml"

# 环境变量驱动的配置覆盖
_includes = [
    path for path in [os.environ.get("PIKO_SETTINGS_PATH")]
    if path is not None
]

# ============================================================================
# Dynaconf 配置加载策略
# ============================================================================
settings = Dynaconf(
    envvar_prefix="PIKO",
    preload=[str(_DEFAULT_SETTINGS)],
    settings_files=[
        "defaults.toml",
        "settings.toml",
        ".secrets.toml",
        "piko.toml"
    ],
    includes=_includes,
    environments=True,
    load_dotenv=True,
    validators=[
        Validator("mysql_dsn", must_exist=True),
        Validator("leader_name", must_exist=True),
        Validator("startup_mode", is_in=["fail_closed", "fail_open_snapshot"]),
    ],
)

# ============================================================================
# 动态版本注入 (Version Injection)
# ============================================================================
# 尝试从安装包元数据中读取版本号 (pyproject.toml)，覆盖配置中的默认值
try:
    # 如果执行过 pip install . 或 pip install -e .，这里能读到版本
    _dist_version = metadata.version("piko")
    settings.version = _dist_version
except metadata.PackageNotFoundError:
    # 未安装模式（纯源码运行），保持 defaults.toml 中的默认值
    pass

# 配置验证执行
settings.validators.validate()

# 模块导出
__all__ = ["settings"]
