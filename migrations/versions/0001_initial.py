"""创建 Piko 当前数据库基线

该版本只包含现有模型对应的表和索引。后续 schema 变化必须追加新版本，
不应直接修改已经部署的迁移文件。
"""

from typing import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import mysql

revision: str = "0001_initial"
down_revision: str | None = None
branch_labels: Sequence[str] | None = None
depends_on: str | None = None


def upgrade() -> None:
    """创建 Piko 基础表和索引"""
    op.create_table(
        "scheduled_job",
        sa.Column("job_id", sa.String(length=128), nullable=False),
        sa.Column("schedule_type", sa.String(length=16), nullable=False),
        sa.Column("schedule_expr", sa.String(length=512), nullable=False),
        sa.Column("timezone", sa.String(length=64), nullable=False, server_default="Asia/Shanghai"),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.text("1")),
        sa.Column("misfire_grace_s", sa.Integer(), nullable=False, server_default=sa.text("300")),
        sa.Column("coalesce", sa.Boolean(), nullable=False, server_default=sa.text("1")),
        sa.Column("max_instances", sa.Integer(), nullable=False, server_default=sa.text("1")),
        sa.Column("jitter_s", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("executor", sa.String(length=16), nullable=False, server_default="cpu"),
        sa.Column(
            "concurrency_group",
            sa.String(length=64),
            nullable=False,
            server_default="default",
        ),
        sa.Column("is_stateful", sa.Boolean(), nullable=False, server_default=sa.text("0")),
        sa.Column("last_data_time", mysql.DATETIME(fsp=6), nullable=True),
        sa.Column("max_lookback_window", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("version", mysql.BIGINT(), nullable=False, server_default=sa.text("1")),
        sa.Column(
            "updated_at",
            mysql.DATETIME(fsp=6),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP(6)"),
        ),
        sa.PrimaryKeyConstraint("job_id"),
    )
    op.create_index("idx_sjob_enabled", "scheduled_job", ["enabled"])
    op.create_index("idx_sjob_version", "scheduled_job", ["version"])
    op.create_index("idx_sjob_updated", "scheduled_job", ["updated_at"])

    op.create_table(
        "job_config",
        sa.Column("job_id", sa.String(length=128), nullable=False),
        sa.Column("schema_version", sa.Integer(), nullable=False, server_default=sa.text("1")),
        sa.Column("config_json", mysql.JSON(), nullable=False),
        sa.Column("effective_from", mysql.DATETIME(fsp=6), nullable=True),
        sa.Column("version", mysql.BIGINT(), nullable=False, server_default=sa.text("1")),
        sa.Column(
            "updated_at",
            mysql.DATETIME(fsp=6),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP(6)"),
        ),
        sa.PrimaryKeyConstraint("job_id"),
    )
    op.create_index("idx_jcfg_version", "job_config", ["version"])
    op.create_index("idx_jcfg_updated", "job_config", ["updated_at"])

    op.create_table(
        "job_run",
        sa.Column("run_id", mysql.BIGINT(), autoincrement=True, nullable=False),
        sa.Column("job_id", sa.String(length=128), nullable=False),
        sa.Column("scheduled_time", mysql.DATETIME(fsp=6), nullable=False),
        sa.Column("start_time", mysql.DATETIME(fsp=6), nullable=False),
        sa.Column("end_time", mysql.DATETIME(fsp=6), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("config_version", mysql.BIGINT(), nullable=True),
        sa.Column("schedule_version", mysql.BIGINT(), nullable=True),
        sa.Column("data_time_start", mysql.DATETIME(fsp=6), nullable=True),
        sa.Column("data_time_end", mysql.DATETIME(fsp=6), nullable=True),
        sa.Column("compute_ms", sa.Integer(), nullable=True),
        sa.Column("persist_ms", sa.Integer(), nullable=True),
        sa.Column("duration_ms", sa.Integer(), nullable=True),
        sa.Column("attempt", sa.Integer(), nullable=False, server_default=sa.text("1")),
        sa.Column("error_type", sa.String(length=64), nullable=True),
        sa.Column("error_hash", sa.String(length=64), nullable=True),
        sa.Column("error_msg", sa.String(length=512), nullable=True),
        sa.Column("host", sa.String(length=128), nullable=True),
        sa.Column("pid", sa.Integer(), nullable=True),
        sa.Column(
            "created_at",
            mysql.DATETIME(fsp=6),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP(6)"),
        ),
        sa.PrimaryKeyConstraint("run_id"),
    )
    op.create_index("idx_run_job_time", "job_run", ["job_id", "scheduled_time"])
    op.create_index("idx_run_status", "job_run", ["status"])
    op.create_index("idx_run_created", "job_run", ["created_at"])

    op.create_table(
        "job_lock",
        sa.Column("job_id", sa.String(length=128), nullable=False),
        sa.Column("scheduled_time", mysql.DATETIME(fsp=6), nullable=False),
        sa.Column("owner", sa.String(length=128), nullable=False),
        sa.Column(
            "acquired_at",
            mysql.DATETIME(fsp=6),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP(6)"),
        ),
        sa.PrimaryKeyConstraint("job_id", "scheduled_time"),
    )
    op.create_index("idx_lock_owner", "job_lock", ["owner"])

    op.create_table(
        "scheduler_leader",
        sa.Column("name", sa.String(length=64), nullable=False),
        sa.Column("owner", sa.String(length=128), nullable=False),
        sa.Column("lease_until", mysql.DATETIME(fsp=6), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column(
            "updated_at",
            mysql.DATETIME(fsp=6),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP(6)"),
        ),
        sa.PrimaryKeyConstraint("name"),
    )

    op.create_table(
        "sink_dedupe",
        sa.Column("sink_name", sa.String(length=64), nullable=False),
        sa.Column("idempotency_key", sa.String(length=128), nullable=False),
        sa.Column("run_id", mysql.BIGINT(), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column(
            "updated_at",
            mysql.DATETIME(fsp=6),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP(6)"),
        ),
        sa.Column(
            "created_at",
            mysql.DATETIME(fsp=6),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP(6)"),
        ),
        sa.PrimaryKeyConstraint("sink_name", "idempotency_key"),
    )
    op.create_index("idx_dedupe_run", "sink_dedupe", ["run_id"])
    op.create_index("idx_dedupe_status_updated", "sink_dedupe", ["status", "updated_at"])
    op.create_index("idx_dedupe_created", "sink_dedupe", ["created_at"])


def downgrade() -> None:
    """删除 Piko 基础表"""
    op.drop_table("sink_dedupe")
    op.drop_table("scheduler_leader")
    op.drop_index("idx_lock_owner", table_name="job_lock")
    op.drop_table("job_lock")
    op.drop_index("idx_run_created", table_name="job_run")
    op.drop_index("idx_run_status", table_name="job_run")
    op.drop_index("idx_run_job_time", table_name="job_run")
    op.drop_table("job_run")
    op.drop_index("idx_jcfg_updated", table_name="job_config")
    op.drop_index("idx_jcfg_version", table_name="job_config")
    op.drop_table("job_config")
    op.drop_index("idx_sjob_updated", table_name="scheduled_job")
    op.drop_index("idx_sjob_version", table_name="scheduled_job")
    op.drop_index("idx_sjob_enabled", table_name="scheduled_job")
    op.drop_table("scheduled_job")
