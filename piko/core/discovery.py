import importlib
import logging
import pkgutil
from types import ModuleType
from typing import Union, Optional

logger = logging.getLogger(__name__)


def _resolve_package(package: Union[str, ModuleType]) -> Optional[ModuleType]:
    """
    [Internal] 解析并校验包对象。

    负责将字符串形式的包名转换为模块对象，并校验该对象是否为可遍历的“包”（Package）。
    如果对象仅为普通模块（Module）而非包，则无法进行递归扫描。

    Args:
        package (str | ModuleType): 包名或模块对象。

    Returns:
        Optional[ModuleType]: 解析成功且合法的包对象；若解析失败或校验不通过则返回 None。
    """
    resolved_pkg = package

    # 1. 如果传入的是字符串，尝试导入
    if isinstance(package, str):
        try:
            resolved_pkg = importlib.import_module(package)
        except ImportError as e:
            logger.error(f"Autodiscover failed: Could not import root package {package!r}. details: {e}")
            return None

    # 2. 核心校验：只有包含 __path__ 属性的模块才是“包”，才能被 pkgutil 遍历
    # 如果用户传入了一个普通模块文件 (e.g. utils.py)，这里应拦截并报错
    if not hasattr(resolved_pkg, "__path__"):
        pkg_name = getattr(resolved_pkg, "__name__", str(package))
        logger.error(
            f"Autodiscover failed: '{pkg_name}' is a module, not a package. "
            "Autodiscover requires a package with a __path__ attribute to scan sub-modules."
        )
        return None

    return resolved_pkg


def autodiscover(package: Union[str, ModuleType], module_name: str = "jobs") -> None:
    """
    递归扫描指定包下的所有子模块，并自动加载名为 `module_name` 的模块。

    该函数利用 `pkgutil` 遍历指定包及其所有子包。当发现模块名以指定的后缀
    （默认为 ".jobs"）结尾时，会动态导入该模块。这通常用于触发 Piko 框架中
    @job 装饰器的副作用（Side Effects），从而在无需显式引用的情况下完成任务注册。

    Args:
        package (str | ModuleType): 根包名称 (如 "my_project") 或包对象。
        module_name (str): 要寻找的目标模块后缀名，默认为 "jobs" (即扫描 jobs.py)。

    Note:
        - 会忽略导入失败的模块，但会记录 Error 日志，不会阻塞整个应用启动。
        - 遇到 KeyboardInterrupt 或 SystemExit 时会正常抛出，确保进程可被终止。
    """
    # 1. 解析包对象
    root_package = _resolve_package(package)
    if not root_package:
        return

    # 2. 准备扫描参数
    # pkgutil.walk_packages 需要传入 path 列表 (List[str])
    path = getattr(root_package, "__path__", [])
    # 确保前缀以点号结尾，用于后续拼接子模块名
    prefix = root_package.__name__ + "."

    target_suffix = f".{module_name}"
    discovered_count = 0

    # 3. 递归遍历所有子模块和子包
    # walk_packages 会自动处理嵌套结构
    for _, name, _ in pkgutil.walk_packages(path, prefix):
        # 过滤：只加载符合命名规则的模块
        # 例如: iop_session_archiver.ftp_download.jobs
        if name.endswith(target_suffix):
            try:
                importlib.import_module(name)
                logger.debug(f"🔍 Piko Auto-loaded: {name}")
                discovered_count += 1

            except (KeyboardInterrupt, SystemExit):
                # 严禁捕获系统级中断信号，确保 Ctrl+C 或 kill 能正常工作
                raise
            except Exception as e:
                # 捕获 ImportError, SyntaxError 以及模块执行时的其他运行时错误
                # 即使一个模块坏了，也不应该导致整个应用崩溃（但必须记录严重错误）
                logger.error(f"Failed to load job module '{name}': {e}", exc_info=True)

    # 4. 结果汇总
    if discovered_count == 0:
        logger.warning(
            f"Autodiscover finished but found 0 '{module_name}' modules in '{root_package.__name__}'. "
            f"Please check if your job files are named '{module_name}.py' and have __init__.py in directories."
        )
    else:
        logger.info(f"Piko Autodiscover: Loaded {discovered_count} job modules from '{root_package.__name__}'")
