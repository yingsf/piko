"""基于目标结构的 MySQL schema 收敛器。

Piko 不维护独立的版本表。应用启动时以 ``Base.metadata`` 作为目标结构，
在数据库锁保护下执行幂等的增量收敛：缺失表、字段、索引和约束会被补齐，
已知的历史兼容数据迁移会被显式、幂等地执行并记录在收敛报告中。重命名、
删列、类型收紧等无法安全推断的变更会直接报告为 schema 不兼容，要求发布者
提供显式的数据迁移代码。
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from copy import copy
from dataclasses import dataclass
import re
from typing import Any, cast

from sqlalchemy import delete, func, inspect, literal_column, select, text, update
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.schema import AddConstraint, CreateColumn, CreateIndex, DefaultClause
from sqlalchemy.sql.schema import Column, ForeignKeyConstraint, Table, UniqueConstraint

from piko.infra.db import Base
from piko.infra.logging import get_logger

logger = get_logger(__name__)

SCHEMA_LOCK_NAME = "piko-schema-bootstrap"
DEFAULT_SCHEMA_LOCK_TIMEOUT_S = 30


class SchemaReconciliationError(RuntimeError):
    """Piko 目标 schema 无法安全收敛。"""


class SchemaMismatchError(SchemaReconciliationError):
    """现有表结构与目标结构不兼容。"""


@dataclass(frozen=True)
class SchemaReport:
    """一次 schema 检查或收敛的结构化结果。"""

    created_tables: tuple[str, ...] = ()
    added_columns: tuple[str, ...] = ()
    added_indexes: tuple[str, ...] = ()
    added_constraints: tuple[str, ...] = ()
    missing_tables: tuple[str, ...] = ()
    missing_columns: tuple[str, ...] = ()
    missing_indexes: tuple[str, ...] = ()
    missing_constraints: tuple[str, ...] = ()
    compatibility_actions: tuple[str, ...] = ()
    updated_defaults: tuple[str, ...] = ()
    missing_defaults: tuple[str, ...] = ()

    @property
    def is_synchronized(self) -> bool:
        """返回数据库是否已经满足目标结构。"""
        return not any(
            (
                self.missing_tables,
                self.missing_columns,
                self.missing_indexes,
                self.missing_constraints,
                self.missing_defaults,
            )
        )

    @property
    def changed(self) -> bool:
        """返回本次是否实际补齐了结构。"""
        return any(
            (
                self.created_tables,
                self.added_columns,
                self.added_indexes,
                self.added_constraints,
                self.updated_defaults,
                self.compatibility_actions,
            )
        )

    def summary(self) -> str:
        """返回适合日志和 CLI 输出的简短摘要。"""
        if not self.changed and self.is_synchronized:
            return "schema is synchronized"
        parts: list[str] = []
        for label, values in (
            ("created tables", self.created_tables),
            ("added columns", self.added_columns),
            ("added indexes", self.added_indexes),
            ("added constraints", self.added_constraints),
            ("updated defaults", self.updated_defaults),
            ("compatibility actions", self.compatibility_actions),
            ("missing tables", self.missing_tables),
            ("missing columns", self.missing_columns),
            ("missing indexes", self.missing_indexes),
            ("missing constraints", self.missing_constraints),
            ("missing defaults", self.missing_defaults),
        ):
            if values:
                parts.append(f"{label}: {', '.join(values)}")
        return "; ".join(parts)


@dataclass(frozen=True)
class _ColumnPlan:
    table: Table
    column: Column[Any]
    backfill_sql: str | None
    default_sql: str | None
    server_default_sql: str | None
    table_has_rows: bool


@dataclass(frozen=True)
class _DefaultPlan:
    table: Table
    column: Column[Any]
    default_sql: str


@dataclass(frozen=True)
class _SchemaDrift:
    report: SchemaReport
    column_plans: tuple[_ColumnPlan, ...]
    default_plans: tuple[_DefaultPlan, ...]


# These are the only historical data transformations inferred by the generic
# reconciler. New non-nullable columns with existing rows must add an explicit
# entry here, otherwise startup fails instead of guessing a value.
_COLUMN_BACKFILLS: dict[tuple[str, str], str] = {
    ("job_lock", "owner_token"): "UUID()",
    ("job_lock", "expires_at"): "DATE_ADD(acquired_at, INTERVAL 300 SECOND)",
}


def _quoted(connection: Connection, identifier: str) -> str:
    """引用由代码定义的 MySQL 标识符。"""
    return connection.dialect.identifier_preparer.quote(identifier)


def _type_sql(connection: Connection, type_: Any) -> str:
    """规范化 SQLAlchemy 类型文本，供结构兼容性检查使用。"""
    normalized = re.sub(r"\s+", "", str(type_.compile(dialect=connection.dialect))).upper()
    normalized = re.sub(r"COLLATE[A-Z0-9_]+", "", normalized)
    normalized = re.sub(r"CHARACTERSET[A-Z0-9_]+", "", normalized)
    return normalized


def _types_compatible(connection: Connection, expected: Any, actual: Any) -> bool:
    """判断 MySQL 反射类型是否可以承载当前模型类型。"""
    expected_name = type(expected).__name__.upper()
    actual_name = type(actual).__name__.upper()
    if expected_name == "BOOLEAN":
        return actual_name in {"BOOLEAN", "TINYINT"} and _type_sql(connection, actual) in {
            "BOOLEAN",
            "BOOL",
            "TINYINT(1)",
        }
    return _type_sql(connection, expected) == _type_sql(connection, actual)


def _column_signature(columns: Iterable[str]) -> tuple[str, ...]:
    return tuple(columns)


def _index_signature(index: Mapping[str, Any]) -> tuple[tuple[str, ...], bool]:
    return (
        _column_signature(cast(Iterable[str], index.get("column_names") or ())),
        bool(index.get("unique")),
    )


def _expected_index_signature(index: Any) -> tuple[tuple[str, ...], bool]:
    return (tuple(column.name for column in index.columns), bool(index.unique))


def _find_incompatible_index(
    actual_signatures: set[tuple[tuple[str, ...], bool]],
    expected: tuple[tuple[str, ...], bool],
) -> tuple[tuple[str, ...], bool] | None:
    return next(
        (
            actual
            for actual in actual_signatures
            if actual[0] == expected[0] and actual[1] != expected[1]
        ),
        None,
    )


def _raise_index_mismatch(
    table_name: str,
    index_name: str,
    expected: tuple[tuple[str, ...], bool],
    actual: tuple[tuple[str, ...], bool],
) -> None:
    if actual[0] == expected[0] and actual[1] != expected[1]:
        raise SchemaMismatchError(
            f"index {table_name}.{index_name} has incompatible uniqueness: "
            f"expected unique={expected[1]!r}, got unique={actual[1]!r}"
        )
    raise SchemaMismatchError(
        f"index {table_name}.{index_name} differs: "
        f"expected columns={expected[0]!r}, unique={expected[1]!r}; "
        f"got columns={actual[0]!r}, unique={actual[1]!r}"
    )


def _unique_signatures(inspector: Any, table_name: str) -> set[tuple[str, ...]]:
    signatures = {
        _column_signature(cast(Iterable[str], item.get("column_names") or ()))
        for item in inspector.get_unique_constraints(table_name)
    }
    signatures.update(
        _column_signature(cast(Iterable[str], item.get("column_names") or ()))
        for item in inspector.get_indexes(table_name)
        if item.get("unique")
    )
    return signatures


def _foreign_key_signature(item: Mapping[str, Any]) -> tuple[Any, ...]:
    options = cast(Mapping[str, Any], item.get("options") or {})
    ondelete = str(options.get("ondelete") or "").upper()
    return (
        _column_signature(cast(Iterable[str], item.get("constrained_columns") or ())),
        str(item.get("referred_table") or ""),
        _column_signature(cast(Iterable[str], item.get("referred_columns") or ())),
        ondelete,
    )


def _expected_foreign_key_signature(constraint: ForeignKeyConstraint) -> tuple[Any, ...]:
    elements = tuple(constraint.elements)
    return (
        _column_signature(element.parent.name for element in elements),
        str(elements[0].target_fullname.rsplit(".", 1)[0]),
        _column_signature(element.column.name for element in elements),
        str(constraint.ondelete or "").upper(),
    )


def _default_sql(column: Column[Any]) -> str | None:
    """获取可安全用于历史数据回填的标量默认值。"""
    default = cast(Any, column.default)
    if default is None:
        return None
    value: Any = default.arg
    if callable(value):
        if getattr(value, "__name__", "") == "utcnow":
            return "CURRENT_TIMESTAMP(6)"
        return None
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    return None


def _server_default_sql(column: Column[Any]) -> str | None:
    """获取模型声明的数据库级默认值 SQL。"""
    default = cast(Any, column.server_default)
    if default is None:
        return None
    return str(default.arg)


def _default_signature(value: Any) -> str | None:
    """规范化模型和 MySQL 反射返回的默认值文本。"""
    if value is None:
        return None
    normalized = re.sub(r"\s+", "", str(value)).upper()
    while len(normalized) >= 2 and normalized.startswith("(") and normalized.endswith(")"):
        normalized = normalized[1:-1]
    if re.fullmatch(r"'[+-]?\d+(?:\.\d+)?'", normalized):
        normalized = normalized[1:-1]
    return normalized


def _column_ddl(
    connection: Connection,
    column: Column[Any],
    *,
    nullable: bool | None = None,
    default_sql: str | None = None,
) -> str:
    """编译 ADD/MODIFY COLUMN 所需的列定义。"""
    cloned = copy(column)
    if nullable is not None:
        cloned.nullable = nullable
    if default_sql is not None:
        cloned.server_default = DefaultClause(text(default_sql))
    return str(CreateColumn(cloned).compile(dialect=connection.dialect))


def _table_has_rows(connection: Connection, table_name: str) -> bool:
    table = Base.metadata.tables[table_name]
    return connection.execute(select(1).select_from(table).limit(1)).first() is not None


def _validate_existing_table(connection: Connection, table: Table, inspector: Any) -> None:
    actual_columns = {item["name"]: item for item in inspector.get_columns(table.name)}
    expected_primary_key = tuple(column.name for column in table.primary_key.columns)
    actual_primary_key = tuple(
        inspector.get_pk_constraint(table.name).get("constrained_columns") or ()
    )
    if expected_primary_key != actual_primary_key:
        raise SchemaMismatchError(
            f"table {table.name!r} primary key differs: "
            f"expected {expected_primary_key!r}, got {actual_primary_key!r}"
        )

    for column in table.columns:
        actual = actual_columns.get(column.name)
        if actual is None:
            continue
        if not _types_compatible(connection, column.type, actual["type"]):
            raise SchemaMismatchError(
                f"column {table.name}.{column.name} type differs: "
                f"expected {_type_sql(connection, column.type)}, "
                f"got {_type_sql(connection, actual['type'])}; "
                "provide an explicit data migration before upgrading"
            )
        if column.nullable is False and actual.get("nullable") is True:
            raise SchemaMismatchError(
                f"column {table.name}.{column.name} is nullable but the target is NOT NULL; "
                "backfill and tighten it explicitly before upgrading"
            )


def _plan_missing_columns(
    connection: Connection, table: Table, inspector: Any
) -> tuple[_ColumnPlan, ...]:
    actual_names = {item["name"] for item in inspector.get_columns(table.name)}
    missing_columns = [column for column in table.columns if column.name not in actual_names]
    if not missing_columns:
        return ()
    table_has_rows = _table_has_rows(connection, table.name)
    plans: list[_ColumnPlan] = []
    for column in missing_columns:
        backfill_sql = _COLUMN_BACKFILLS.get((table.name, column.name))
        default_sql = _default_sql(column)
        server_default_sql = _server_default_sql(column)
        if column.nullable is False and table_has_rows and not (backfill_sql or default_sql):
            raise SchemaMismatchError(
                f"cannot add required column {table.name}.{column.name} to a non-empty table; "
                "add an explicit backfill rule before upgrading"
            )
        plans.append(
            _ColumnPlan(
                table=table,
                column=column,
                backfill_sql=backfill_sql,
                default_sql=default_sql,
                server_default_sql=server_default_sql,
                table_has_rows=table_has_rows,
            )
        )
    return tuple(plans)


def _plan_missing_defaults(table: Table, inspector: Any) -> tuple[_DefaultPlan, ...]:
    actual_columns = {item["name"]: item for item in inspector.get_columns(table.name)}
    plans: list[_DefaultPlan] = []
    for column in table.columns:
        expected = _server_default_sql(column)
        actual = actual_columns.get(column.name)
        if expected is None or actual is None:
            continue
        if _default_signature(expected) != _default_signature(actual.get("default")):
            plans.append(_DefaultPlan(table=table, column=column, default_sql=expected))
    return tuple(plans)


def _missing_indexes(table: Table, inspector: Any) -> tuple[str, ...]:
    actual_indexes = {str(item.get("name")): item for item in inspector.get_indexes(table.name)}
    actual_signatures = {_index_signature(item) for item in actual_indexes.values()}
    missing: list[str] = []
    for index in table.indexes:
        signature = _expected_index_signature(index)
        actual = actual_indexes.get(str(index.name))
        if actual is not None:
            actual_signature = _index_signature(actual)
            if actual_signature != signature:
                _raise_index_mismatch(table.name, str(index.name), signature, actual_signature)
            continue
        if signature in actual_signatures:
            continue
        incompatible = _find_incompatible_index(actual_signatures, signature)
        if incompatible is not None:
            _raise_index_mismatch(table.name, str(index.name), signature, incompatible)
        missing.append(f"{table.name}.{index.name}")
    return tuple(missing)


def _missing_foreign_keys(table: Table, inspector: Any) -> tuple[str, ...]:
    actual = {_foreign_key_signature(item) for item in inspector.get_foreign_keys(table.name)}
    missing: list[str] = []
    for constraint in table.constraints:
        if not isinstance(constraint, ForeignKeyConstraint):
            continue
        expected = _expected_foreign_key_signature(constraint)
        if expected not in actual:
            missing.append(f"{table.name}.foreign-key:{expected}")
    return tuple(missing)


def _missing_unique_constraints(table: Table, inspector: Any) -> tuple[str, ...]:
    actual = _unique_signatures(inspector, table.name)
    missing: list[str] = []
    for constraint in table.constraints:
        if not isinstance(constraint, UniqueConstraint):
            continue
        signature = tuple(column.name for column in constraint.columns)
        if signature not in actual:
            missing.append(f"{table.name}.{constraint.name or signature}")
    return tuple(missing)


def _inspect_schema(connection: Connection) -> _SchemaDrift:
    inspector = inspect(connection)
    actual_tables = set(inspector.get_table_names())
    missing_tables = tuple(sorted(set(Base.metadata.tables) - actual_tables))
    missing_columns: list[str] = []
    missing_indexes: list[str] = []
    missing_constraints: list[str] = []
    plans: list[_ColumnPlan] = []
    default_plans: list[_DefaultPlan] = []

    for table in Base.metadata.sorted_tables:
        if table.name not in actual_tables:
            continue
        _validate_existing_table(connection, table, inspector)
        plans.extend(_plan_missing_columns(connection, table, inspector))
        default_plans.extend(_plan_missing_defaults(table, inspector))
        actual_columns = {item["name"] for item in inspector.get_columns(table.name)}
        missing_columns.extend(
            f"{table.name}.{column.name}"
            for column in table.columns
            if column.name not in actual_columns
        )
        missing_indexes.extend(_missing_indexes(table, inspector))
        missing_constraints.extend(_missing_foreign_keys(table, inspector))
        missing_constraints.extend(_missing_unique_constraints(table, inspector))

    return _SchemaDrift(
        report=SchemaReport(
            missing_tables=missing_tables,
            missing_columns=tuple(missing_columns),
            missing_indexes=tuple(missing_indexes),
            missing_constraints=tuple(missing_constraints),
            missing_defaults=tuple(
                f"{plan.table.name}.{plan.column.name}" for plan in default_plans
            ),
        ),
        column_plans=tuple(plans),
        default_plans=tuple(default_plans),
    )


def _apply_column_plans(connection: Connection, plans: Iterable[_ColumnPlan]) -> tuple[str, ...]:
    added: list[str] = []
    for plan in plans:
        table_name = _quoted(connection, plan.table.name)
        if plan.column.nullable is False and plan.table_has_rows:
            ddl = _column_ddl(connection, plan.column, nullable=True)
        else:
            ddl = _column_ddl(
                connection,
                plan.column,
                default_sql=plan.server_default_sql or plan.default_sql,
            )
        connection.execute(text(f"ALTER TABLE {table_name} ADD COLUMN {ddl}"))

        backfill_sql = plan.backfill_sql or plan.default_sql or plan.server_default_sql
        if backfill_sql is not None:
            connection.execute(
                update(plan.table)
                .where(plan.column.is_(None))
                .values({plan.column.name: literal_column(backfill_sql)})
            )
        if plan.column.nullable is False and plan.table_has_rows:
            ddl = _column_ddl(
                connection,
                plan.column,
                default_sql=plan.server_default_sql or plan.default_sql,
            )
            connection.execute(text(f"ALTER TABLE {table_name} MODIFY COLUMN {ddl}"))
        added.append(f"{plan.table.name}.{plan.column.name}")
    return tuple(added)


def _apply_default_plans(connection: Connection, plans: Iterable[_DefaultPlan]) -> tuple[str, ...]:
    updated: list[str] = []
    for plan in plans:
        table_name = _quoted(connection, plan.table.name)
        ddl = _column_ddl(connection, plan.column, default_sql=plan.default_sql)
        connection.execute(text(f"ALTER TABLE {table_name} MODIFY COLUMN {ddl}"))
        updated.append(f"{plan.table.name}.{plan.column.name}")
    return tuple(updated)


def _apply_indexes_for_table(
    connection: Connection, table: Table, inspector: Any
) -> tuple[str, ...]:
    added_indexes: list[str] = []
    actual_indexes = {str(item.get("name")): item for item in inspector.get_indexes(table.name)}
    actual_signatures = {_index_signature(item) for item in actual_indexes.values()}
    for index in table.indexes:
        signature = _expected_index_signature(index)
        actual = actual_indexes.get(str(index.name))
        if actual is not None:
            actual_signature = _index_signature(actual)
            if actual_signature != signature:
                _raise_index_mismatch(table.name, str(index.name), signature, actual_signature)
            continue
        if signature in actual_signatures:
            continue
        incompatible = _find_incompatible_index(actual_signatures, signature)
        if incompatible is not None:
            _raise_index_mismatch(table.name, str(index.name), signature, incompatible)
        connection.execute(CreateIndex(index))
        added_indexes.append(f"{table.name}.{index.name}")
    return tuple(added_indexes)


def _remove_legacy_job_run_duplicates(connection: Connection) -> int:
    """保留每组旧重复执行记录中最新的一条。"""
    job_run = Base.metadata.tables["job_run"]
    duplicate_groups = connection.execute(
        select(
            job_run.c.job_id,
            job_run.c.scheduled_time,
            job_run.c.attempt,
            func.count(job_run.c.run_id).label("duplicate_count"),
        )
        .group_by(job_run.c.job_id, job_run.c.scheduled_time, job_run.c.attempt)
        .having(func.count(job_run.c.run_id) > 1)
    ).all()

    removed = 0
    for group in duplicate_groups:
        run_ids = (
            connection.execute(
                select(job_run.c.run_id)
                .where(
                    job_run.c.job_id == group.job_id,
                    job_run.c.scheduled_time == group.scheduled_time,
                    job_run.c.attempt == group.attempt,
                )
                .order_by(job_run.c.run_id.desc())
            )
            .scalars()
            .all()
        )
        obsolete_ids = run_ids[1:]
        if obsolete_ids:
            result = connection.execute(delete(job_run).where(job_run.c.run_id.in_(obsolete_ids)))
            removed += max(result.rowcount or 0, 0)
    return removed


def _backfill_completed_date_jobs(connection: Connection) -> int:
    """按旧 0004 语义收敛已成功的一次性任务。"""
    scheduled_job = Base.metadata.tables["scheduled_job"]
    job_run = Base.metadata.tables["job_run"]
    pending_date_jobs = select(scheduled_job.c.job_id).where(
        scheduled_job.c.schedule_type == "date",
        scheduled_job.c.enabled.is_(True),
        scheduled_job.c.completed_at.is_(None),
    )
    successful_jobs = connection.execute(
        select(job_run.c.job_id, func.max(job_run.c.end_time).label("completed_at"))
        .where(
            job_run.c.status == "SUCCESS",
            job_run.c.end_time.is_not(None),
            job_run.c.job_id.in_(pending_date_jobs),
        )
        .group_by(job_run.c.job_id)
    ).all()

    updated = 0
    for row in successful_jobs:
        result = connection.execute(
            update(scheduled_job)
            .where(
                scheduled_job.c.job_id == row.job_id,
                scheduled_job.c.schedule_type == "date",
                scheduled_job.c.enabled.is_(True),
                scheduled_job.c.completed_at.is_(None),
            )
            .values(enabled=False, completed_at=row.completed_at)
        )
        updated += max(result.rowcount or 0, 0)
    return updated


def _apply_compatibility_migrations(
    connection: Connection, *, cleanup_job_run_duplicates: bool
) -> tuple[str, ...]:
    """执行无版本表的历史数据兼容迁移，并返回可审计动作。"""
    actions: list[str] = []
    if cleanup_job_run_duplicates:
        removed_duplicates = _remove_legacy_job_run_duplicates(connection)
        if removed_duplicates:
            actions.append(f"job_run_duplicate_cleanup:{removed_duplicates}")
            logger.info(
                "schema_compatibility_migration",
                migration="job_run_duplicate_cleanup",
                rows_removed=removed_duplicates,
            )

    completed_jobs = _backfill_completed_date_jobs(connection)
    if completed_jobs:
        actions.append(f"date_job_completion_backfill:{completed_jobs}")
        logger.info(
            "schema_compatibility_migration",
            migration="date_job_completion_backfill",
            jobs_updated=completed_jobs,
        )
    return tuple(actions)


def _apply_constraints_for_table(
    connection: Connection, table: Table, inspector: Any
) -> tuple[str, ...]:
    added: list[str] = []
    actual_unique_signatures = _unique_signatures(inspector, table.name)
    actual_foreign_keys = {
        _foreign_key_signature(item) for item in inspector.get_foreign_keys(table.name)
    }
    for constraint in table.constraints:
        if isinstance(constraint, ForeignKeyConstraint):
            expected = _expected_foreign_key_signature(constraint)
            if expected not in actual_foreign_keys:
                connection.execute(AddConstraint(constraint))
                added.append(f"{table.name}.foreign-key:{expected}")
                actual_foreign_keys.add(expected)
            continue
        if not isinstance(constraint, UniqueConstraint):
            continue
        signature = tuple(column.name for column in constraint.columns)
        if signature not in actual_unique_signatures:
            connection.execute(AddConstraint(constraint))
            added.append(f"{table.name}.{constraint.name or signature}")
            actual_unique_signatures.add(signature)
    return tuple(added)


def _apply_indexes_and_constraints(
    connection: Connection,
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    added_indexes: list[str] = []
    added_constraints: list[str] = []
    tables = set(inspect(connection).get_table_names())
    for table in Base.metadata.sorted_tables:
        if table.name not in tables:
            continue
        inspector = inspect(connection)
        added_indexes.extend(_apply_indexes_for_table(connection, table, inspector))
        inspector = inspect(connection)
        added_constraints.extend(_apply_constraints_for_table(connection, table, inspector))
    return tuple(added_indexes), tuple(added_constraints)


def _reconcile_sync(connection: Connection) -> SchemaReport:
    before_tables = set(inspect(connection).get_table_names())
    initial = _inspect_schema(connection)
    Base.metadata.create_all(connection)
    after_tables = set(inspect(connection).get_table_names())
    created_tables = tuple(sorted((after_tables - before_tables) & set(Base.metadata.tables)))

    added_columns = _apply_column_plans(connection, initial.column_plans)
    updated_defaults = _apply_default_plans(connection, initial.default_plans)
    compatibility_actions = _apply_compatibility_migrations(
        connection,
        cleanup_job_run_duplicates=any(
            item.startswith("job_run.uq_run_job_time_attempt")
            for item in initial.report.missing_constraints
        ),
    )
    added_indexes, added_constraints = _apply_indexes_and_constraints(connection)
    final = _inspect_schema(connection).report
    if not final.is_synchronized:
        raise SchemaReconciliationError(
            "schema reconciliation did not converge: " + final.summary()
        )
    return SchemaReport(
        created_tables=created_tables,
        added_columns=added_columns,
        added_indexes=added_indexes,
        added_constraints=added_constraints,
        updated_defaults=updated_defaults,
        compatibility_actions=compatibility_actions,
    )


async def _acquire_lock(connection: Any, timeout_s: int) -> bool:
    value = await connection.scalar(
        text("SELECT GET_LOCK(:lock_name, :timeout_s)"),
        {"lock_name": SCHEMA_LOCK_NAME, "timeout_s": timeout_s},
    )
    return value == 1


async def _release_lock(connection: Any) -> None:
    await connection.scalar(
        text("SELECT RELEASE_LOCK(:lock_name)"), {"lock_name": SCHEMA_LOCK_NAME}
    )


async def ensure_schema(
    engine: AsyncEngine, *, lock_timeout_s: int = DEFAULT_SCHEMA_LOCK_TIMEOUT_S
) -> SchemaReport:
    """在 MySQL advisory lock 下将数据库收敛到当前目标结构。"""
    if lock_timeout_s < 0:
        raise ValueError("lock_timeout_s must be non-negative")
    async with engine.connect() as connection:
        if not await _acquire_lock(connection, lock_timeout_s):
            raise SchemaReconciliationError(f"could not acquire schema lock {SCHEMA_LOCK_NAME!r}")
        try:
            await connection.commit()
            report = await connection.run_sync(_reconcile_sync)
            await connection.commit()
            logger.info("schema_reconciled", summary=report.summary())
            return report
        except Exception:
            await connection.rollback()
            raise
        finally:
            await _release_lock(connection)


async def check_schema(engine: AsyncEngine) -> SchemaReport:
    """只检查目标结构，不执行 DDL。"""
    async with engine.connect() as connection:
        drift = await connection.run_sync(_inspect_schema)
    if not drift.report.is_synchronized:
        return drift.report
    return SchemaReport()
