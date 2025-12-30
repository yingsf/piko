import os
from pathlib import Path
from dynaconf import Dynaconf, Validator

_LIB_CONFIG_DIR = Path(__file__).parent

# _DEFAULT_SETTINGS 指向库内置的 defaults.toml，作为所有配置的基线
# 这确保即使用户没有提供任何自定义配置，服务也能以默认值启动
_DEFAULT_SETTINGS = _LIB_CONFIG_DIR / "defaults.toml"

# 环境变量驱动的配置覆盖
_includes = [
    path for path in [os.environ.get("PIKO_SETTINGS_PATH")]
    if path is not None
]

# ============================================================================
# Dynaconf 配置加载策略
# ============================================================================
# Dynaconf 实例负责从多个配置源加载配置，并按优先级合并：
#   1. preload: 库内置的默认配置（优先级最低，作为兜底）
#   2. settings_files: 按顺序查找的本地配置文件（优先级逐步升高）
#      - defaults.toml: 通用配置
#      - .secrets.toml: 敏感信息（如密钥，应在 .gitignore 中排除）
#      - piko.toml: 项目特定配置
#   3. includes: 环境变量 PIKO_SETTINGS_PATH 指定的外部配置（优先级更高）
#   4. 环境变量: 以 PIKO_ 前缀的环境变量会自动覆盖同名配置项（优先级最高）
#
settings = Dynaconf(
    # envvar_prefix: 环境变量前缀，例如 PIKO_MYSQL_DSN 会映射到 mysql_dsn
    envvar_prefix="PIKO",

    # preload: 最先加载的配置文件，作为所有后续配置的基线
    preload=[str(_DEFAULT_SETTINGS)],

    # settings_files: 按顺序查找的配置文件列表
    # Dynaconf 会从当前工作目录及其父目录递归查找这些文件，后加载的文件会覆盖先加载文件中的同名配置项
    settings_files=[
        "defaults.toml",
        "settings.toml",
        ".secrets.toml",
        "piko.toml"
    ],

    # includes: 额外的配置文件路径，通常通过环境变量动态指定
    # 场景：在容器化部署时，ConfigMap/Secret 挂载的配置文件路径可通过此参数传入
    includes=_includes,

    # environments: 启用环境隔离（如 [development]、[production] 等节）
    # 允许在同一配置文件中为不同环境定义差异化配置
    environments=True,

    # load_dotenv: 自动加载 .env 文件中的环境变量
    load_dotenv=True,

    # validators: 配置验证器列表，在加载后立即执行校验
    validators=[
        # mysql_dsn 必须存在，否则无法初始化数据库连接
        Validator("mysql_dsn", must_exist=True),

        # leader_name 必须存在，用于分布式环境下的leader选举
        Validator("leader_name", must_exist=True),

        # startup_mode 只能是预定义的两个值之一，防止配置拼写错误
        # "fail_closed": 生产环境推荐，任何异常都中止启动
        # "fail_open_snapshot": 仅用于开发/测试，允许降级启动
        Validator("startup_mode", is_in=["fail_closed", "fail_open_snapshot"]),
    ],
)

# 配置验证执行
settings.validators.validate()

# 模块导出
__all__ = ["settings"]
