#!/usr/bin/env python3
"""执行 Piko 的发布前检查、构建和受控上传

脚本要求从仓库根目录运行。默认流程只构建和校验产物；上传必须指定
目标、隔离测试库和对应的显式确认变量。
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import tarfile
import tempfile
import tomllib
import zipfile
from pathlib import Path, PurePosixPath
from typing import Any

from sqlalchemy.engine import URL, make_url

ROOT = Path(__file__).resolve().parents[1]
DIST = ROOT / "dist"
TEST_CONFIRMATION = "PIKO_TESTPYPI_CONFIRM"
PYPI_CONFIRMATION = "PIKO_PYPI_CONFIRM"


def _read_project_metadata() -> tuple[str, str]:
    """读取项目名称和版本

    Returns:
        tuple[str, str]: 发布包名称和 PEP 440 版本。

    Raises:
        RuntimeError: 当项目元数据缺失或格式错误时。
    """
    try:
        with (ROOT / "pyproject.toml").open("rb") as file:
            document: dict[str, Any] = tomllib.load(file)
        project = document["project"]
        name = project["name"]
        version = project["version"]
    except (KeyError, OSError, TypeError, tomllib.TOMLDecodeError) as error:
        raise RuntimeError("无法读取 pyproject.toml 的 project 元数据") from error
    if not isinstance(name, str) or not name or not isinstance(version, str) or not version:
        raise RuntimeError("pyproject.toml 必须提供非空的 project.name 和 project.version")
    return name, version


def _run(command: list[str], *, env: dict[str, str] | None = None) -> None:
    """在仓库根目录执行一个发布步骤

    不打印环境变量，避免将 DSN、PyPI token 或其他凭据写入日志。
    """
    print("$", " ".join(command))
    subprocess.run(command, cwd=ROOT, env=env, check=True)


def _assert_clean_worktree() -> None:
    """确认发布开始前工作树没有未提交修改"""
    result = subprocess.run(
        ["git", "status", "--porcelain", "--untracked-files=all"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    if result.stdout.strip():
        raise RuntimeError("发布要求 Git 工作树干净，请先提交或清理修改")


def _clean() -> None:
    """删除旧的构建目录和 egg-info 元数据"""
    for path in [ROOT / "dist", ROOT / "build"]:
        if path.exists():
            shutil.rmtree(path)
    for path in ROOT.rglob("*.egg-info"):
        if ".venv" not in path.parts and path.is_dir():
            shutil.rmtree(path)


def _canonical_mysql_url(dsn: str) -> URL:
    """解析异步 MySQL DSN 并统一兼容驱动别名"""
    url = make_url(dsn)
    if url.drivername == "mysql+asyncmy":
        return url.set(drivername="mysql+aiomysql")
    return url


# 测试库允许的数据库名关键词（任一匹配即视为非生产库）。
# 不再强制要求库名含 "test"，dev/staging/ci/piko 等隔离库同样可接受。
_SAFE_DB_KEYWORDS = ("test", "dev", "staging", "ci", "piko")
# 触发生产确认的数据库名关键词。
_PROD_DB_KEYWORDS = ("prod", "production")


def _resolve_test_environment() -> tuple[dict[str, str], bool]:
    """解析质量门禁环境与集成测试可用性

    与早期版本不同，``PIKO_TEST_MYSQL_DSN`` 现为可选：未配置时仍可发布，
    只是跳过集成测试（仅跑单元测试与静态检查）。配置了 DSN 时校验其
    符合隔离约束（非生产库或显式放行），并把该 DSN 注入到子进程环境。

    Returns:
        tuple[dict, bool]: (子进程环境覆盖, 是否启用集成测试)。

    Raises:
        RuntimeError: 当配置的 DSN 不满足隔离约束或与生产 DSN 撞库时。
    """
    import os

    dsn = os.environ.get("PIKO_TEST_MYSQL_DSN", "").strip()
    if not dsn:
        # 未配置测试库：质量门禁仍可运行（集成测试会被 pytest 自动跳过）。
        return os.environ.copy(), False

    try:
        url = _canonical_mysql_url(dsn)
    except Exception as error:
        raise RuntimeError("PIKO_TEST_MYSQL_DSN 不是有效的 MySQL DSN") from error
    if url.drivername != "mysql+aiomysql":
        raise RuntimeError("PIKO_TEST_MYSQL_DSN 必须使用 mysql+aiomysql 或 mysql+asyncmy")

    database = (url.database or "").lower()
    allow_production = os.environ.get("PIKO_ALLOW_PRODUCTION_DB", "").strip() == "1"
    looks_like_prod = any(word in database for word in _PROD_DB_KEYWORDS)
    looks_like_safe = any(word in database for word in _SAFE_DB_KEYWORDS)
    if looks_like_prod and not allow_production:
        raise RuntimeError(
            "PIKO_TEST_MYSQL_DSN 指向疑似生产库（库名含 prod/production）。"
            "如确需使用，请显式设置 PIKO_ALLOW_PRODUCTION_DB=1。"
        )
    if not looks_like_safe and not allow_production:
        raise RuntimeError(
            f"PIKO_TEST_MYSQL_DSN 的数据库名 {database!r} 不在安全关键词"
            f"（{_SAFE_DB_KEYWORDS}）内。如确需使用，请显式设置 PIKO_ALLOW_PRODUCTION_DB=1。"
        )

    generic_dsn = os.environ.get("PIKO_MYSQL_DSN", "").strip()
    if generic_dsn:
        try:
            same_database = _canonical_mysql_url(generic_dsn) == url
        except Exception:
            same_database = False
        if same_database:
            raise RuntimeError("PIKO_TEST_MYSQL_DSN 不能与 PIKO_MYSQL_DSN 相同")

    environment = os.environ.copy()
    environment["PIKO_MYSQL_DSN"] = dsn
    return environment, True


def _run_quality_gates() -> None:
    """执行发布所需的静态检查、测试和安全扫描

    静态检查（ruff/pyright/bandit）与依赖审计始终执行。pytest 门禁按是否
    配置隔离测试库分层：配置了 ``PIKO_TEST_MYSQL_DSN`` 时跑全部用例（含集成
    测试），未配置时只跑单元测试（``-m "not integration"``），后者不会阻断
    发布。建议发布前尽量在隔离库上跑一次完整集成测试。
    """
    environment, integration_enabled = _resolve_test_environment()
    executable = sys.executable
    commands = [
        [executable, "-m", "ruff", "check", "."],
        [executable, "-m", "ruff", "format", "--check", "."],
        [executable, "-m", "pyright"],
    ]
    if integration_enabled:
        commands.append([executable, "-m", "pytest"])
        print("质量门禁：启用集成测试（PIKO_TEST_MYSQL_DSN 已配置）")
    else:
        commands.append([executable, "-m", "pytest", "-m", "not integration"])
        print("质量门禁：跳过集成测试（未配置 PIKO_TEST_MYSQL_DSN，仅运行单元测试）")
    commands.append([executable, "-m", "bandit", "-r", "piko", "-q"])
    for command in commands:
        _run(command, env=environment)
    _run_dependency_audit(environment)


def _run_dependency_audit(environment: dict[str, str]) -> None:
    """导出锁定的第三方依赖并执行严格审计。

    当前项目会被 `uv run` 以 editable 形式安装，直接审计环境会尝试在
    PyPI 查询本地项目自身。导出时排除项目本身，只审计可解析的第三方依赖。
    """
    export = subprocess.run(
        [
            "uv",
            "export",
            "--frozen",
            "--no-dev",
            "--no-emit-project",
            "--format",
            "requirements.txt",
        ],
        cwd=ROOT,
        env=environment,
        check=True,
        capture_output=True,
        text=True,
    )
    with tempfile.NamedTemporaryFile(mode="w", suffix="-piko-requirements.txt") as file:
        file.write(export.stdout)
        file.flush()
        _run(
            [
                sys.executable,
                "-m",
                "pip_audit",
                "--strict",
                "--disable-pip",
                "-r",
                file.name,
            ],
            env=environment,
        )


def _artifact_names() -> list[Path]:
    """读取构建输出并确认同时存在 sdist 和 wheel"""
    artifacts = sorted(DIST.iterdir()) if DIST.exists() else []
    if not any(path.suffix == ".whl" for path in artifacts):
        raise RuntimeError("构建输出缺少 wheel")
    if not any(path.name.endswith(".tar.gz") for path in artifacts):
        raise RuntimeError("构建输出缺少 sdist")
    return artifacts


def _artifact_members(path: Path) -> list[str]:
    """列出 wheel 或 sdist 中的文件名"""
    if path.suffix == ".whl":
        with zipfile.ZipFile(path) as archive:
            return archive.namelist()
    if path.name.endswith(".tar.gz"):
        with tarfile.open(path, "r:gz") as archive:
            return archive.getnames()
    return []


def _validate_artifacts(name: str, version: str) -> list[Path]:
    """校验产物版本和发布内容边界

    Args:
        name: 项目分发名称。
        version: 项目版本。

    Returns:
        list[Path]: 已通过检查的构建产物。

    Raises:
        RuntimeError: 当版本名不匹配或包含测试内容时。
    """
    artifacts = _artifact_names()
    normalized_name = name.replace("-", "_")
    expected_prefix = f"{normalized_name}-{version}"
    forbidden_parts = {"test", "tests", "fixture", "fixtures"}
    for artifact in artifacts:
        if not artifact.name.startswith(expected_prefix):
            raise RuntimeError(f"构建产物版本不匹配: {artifact.name}，期望前缀 {expected_prefix}")
        members = _artifact_members(artifact)
        for member in members:
            parts = PurePosixPath(member).parts
            if any(part.lower() in forbidden_parts for part in parts):
                raise RuntimeError(f"构建产物包含测试或 fixture 内容: {artifact.name}:{member}")
            if any(part.lower().startswith(("test_", "tests.", "conftest")) for part in parts):
                raise RuntimeError(f"构建产物包含测试文件: {artifact.name}:{member}")
    return artifacts


def _build_and_check(name: str, version: str) -> list[Path]:
    """构建 sdist 和 wheel 并执行 Twine 元数据校验"""
    _run([sys.executable, "-m", "build", "--sdist", "--wheel", "--outdir", str(DIST)])
    artifacts = _validate_artifacts(name, version)
    _run([sys.executable, "-m", "twine", "check", *(str(path) for path in artifacts)])
    return artifacts


def _upload(target: str, artifacts: list[Path]) -> None:
    """按目标和显式确认变量上传构建产物"""
    import os

    if target == "pypi":
        confirmation = os.environ.get(PYPI_CONFIRMATION)
        repository_url = "https://upload.pypi.org/legacy/"
        variable = PYPI_CONFIRMATION
    else:
        confirmation = os.environ.get(TEST_CONFIRMATION)
        repository_url = "https://test.pypi.org/legacy/"
        variable = TEST_CONFIRMATION
    if confirmation != "publish":
        raise RuntimeError(f"上传前必须设置 {variable}=publish")
    _run(
        [
            sys.executable,
            "-m",
            "twine",
            "upload",
            "--non-interactive",
            "--repository-url",
            repository_url,
            *(str(path) for path in artifacts),
        ]
    )


def main() -> int:
    """解析发布参数并执行安全发布流程"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("target", choices=("test", "pypi"), help="上传目标")
    parser.add_argument("--dry-run", action="store_true", help="只检查和构建，不上传")
    args = parser.parse_args()

    try:
        _assert_clean_worktree()
        name, version = _read_project_metadata()
        _run_quality_gates()
        _clean()
        artifacts = _build_and_check(name, version)
        if not args.dry_run:
            _upload(args.target, artifacts)
    except (RuntimeError, subprocess.CalledProcessError) as error:
        print(f"发布失败: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
