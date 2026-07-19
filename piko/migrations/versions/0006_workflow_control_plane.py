"""Add the generic durable workflow control plane."""

from typing import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import mysql

revision: str = "0006_workflow_control_plane"
down_revision: str | None = "schema_v1"
branch_labels: Sequence[str] | None = None
depends_on: str | None = None
WORKFLOW_RUN_FOREIGN_KEY = "workflow_run.run_id"
WORKFLOW_TASK_FOREIGN_KEY = "workflow_task.task_id"


def _timestamp() -> mysql.DATETIME:
    return mysql.DATETIME(fsp=6)


def upgrade() -> None:
    op.create_table(
        "workflow_run",
        sa.Column("run_id", sa.String(length=64), nullable=False),
        sa.Column("workflow_id", sa.String(length=128), nullable=False),
        sa.Column("idempotency_key", sa.String(length=255), nullable=False),
        sa.Column("config_snapshot_json", mysql.JSON(), nullable=False),
        sa.Column("config_digest", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("business_result_status", sa.String(length=32), nullable=False),
        sa.Column("started_at", _timestamp(), nullable=True),
        sa.Column("finished_at", _timestamp(), nullable=True),
        sa.Column("created_at", _timestamp(), nullable=False),
        sa.Column("updated_at", _timestamp(), nullable=False),
        sa.PrimaryKeyConstraint("run_id"),
        sa.UniqueConstraint("workflow_id", "idempotency_key", name="uq_workflow_run_idempotency"),
    )
    op.create_index("idx_workflow_run_status", "workflow_run", ["status"])
    op.create_index("idx_workflow_run_updated", "workflow_run", ["updated_at"])

    op.create_table(
        "workflow_task",
        sa.Column("task_id", sa.String(length=64), nullable=False),
        sa.Column("run_id", sa.String(length=64), nullable=False),
        sa.Column("workflow_id", sa.String(length=128), nullable=False),
        sa.Column("stage", sa.String(length=128), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("attempt", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("max_attempts", sa.Integer(), nullable=False, server_default=sa.text("3")),
        sa.Column("available_at", _timestamp(), nullable=True),
        sa.Column("owner", sa.String(length=128), nullable=True),
        sa.Column("lock_token", sa.String(length=128), nullable=True),
        sa.Column("lease_until", _timestamp(), nullable=True),
        sa.Column("heartbeat_at", _timestamp(), nullable=True),
        sa.Column("idempotency_key", sa.String(length=255), nullable=False),
        sa.Column("error_code", sa.String(length=128), nullable=True),
        sa.Column("error_message", sa.String(length=1024), nullable=True),
        sa.Column("started_at", _timestamp(), nullable=True),
        sa.Column("finished_at", _timestamp(), nullable=True),
        sa.Column("created_at", _timestamp(), nullable=False),
        sa.Column("updated_at", _timestamp(), nullable=False),
        sa.ForeignKeyConstraint(["run_id"], [WORKFLOW_RUN_FOREIGN_KEY], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("task_id"),
        sa.UniqueConstraint("run_id", "stage", name="uq_workflow_task_run_stage"),
        sa.UniqueConstraint("idempotency_key", name="uq_workflow_task_idempotency"),
    )
    op.create_index("idx_workflow_task_claim", "workflow_task", ["status", "available_at", "stage"])
    op.create_index("idx_workflow_task_lease", "workflow_task", ["status", "lease_until"])
    op.create_index("idx_workflow_task_run", "workflow_task", ["run_id", "status"])

    op.create_table(
        "workflow_task_dependency",
        sa.Column("dependency_id", mysql.BIGINT(), autoincrement=True, nullable=False),
        sa.Column("run_id", sa.String(length=64), nullable=False),
        sa.Column("task_id", sa.String(length=64), nullable=False),
        sa.Column("depends_on_task_id", sa.String(length=64), nullable=False),
        sa.Column("condition_json", mysql.JSON(), nullable=False),
        sa.ForeignKeyConstraint(["run_id"], [WORKFLOW_RUN_FOREIGN_KEY], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["task_id"], [WORKFLOW_TASK_FOREIGN_KEY], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["depends_on_task_id"], [WORKFLOW_TASK_FOREIGN_KEY], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("dependency_id"),
        sa.UniqueConstraint(
            "run_id", "task_id", "depends_on_task_id", name="uq_workflow_task_dependency"
        ),
    )
    op.create_index(
        "idx_workflow_dependency_task", "workflow_task_dependency", ["run_id", "task_id"]
    )
    op.create_index(
        "idx_workflow_dependency_upstream",
        "workflow_task_dependency",
        ["run_id", "depends_on_task_id"],
    )

    op.create_table(
        "workflow_task_event",
        sa.Column("event_id", mysql.BIGINT(), autoincrement=True, nullable=False),
        sa.Column("task_id", sa.String(length=64), nullable=False),
        sa.Column("run_id", sa.String(length=64), nullable=False),
        sa.Column("stage", sa.String(length=128), nullable=False),
        sa.Column("event_type", sa.String(length=64), nullable=False),
        sa.Column("payload_json", mysql.JSON(), nullable=False),
        sa.Column("created_at", _timestamp(), nullable=False),
        sa.ForeignKeyConstraint(["task_id"], [WORKFLOW_TASK_FOREIGN_KEY], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["run_id"], [WORKFLOW_RUN_FOREIGN_KEY], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("event_id"),
    )
    op.create_index(
        "idx_workflow_event_task_time", "workflow_task_event", ["task_id", "created_at"]
    )
    op.create_index(
        "idx_workflow_event_run_stage",
        "workflow_task_event",
        ["run_id", "stage", "created_at"],
    )

    op.create_table(
        "workflow_task_manifest",
        sa.Column("manifest_id", mysql.BIGINT(), autoincrement=True, nullable=False),
        sa.Column("task_id", sa.String(length=64), nullable=False),
        sa.Column("run_id", sa.String(length=64), nullable=False),
        sa.Column("idempotency_key", sa.String(length=255), nullable=False),
        sa.Column("result_status", sa.String(length=32), nullable=False),
        sa.Column("result_json", mysql.JSON(), nullable=False),
        sa.Column("output_digest", sa.String(length=128), nullable=True),
        sa.Column("created_at", _timestamp(), nullable=False),
        sa.Column("updated_at", _timestamp(), nullable=False),
        sa.ForeignKeyConstraint(["task_id"], [WORKFLOW_TASK_FOREIGN_KEY], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["run_id"], [WORKFLOW_RUN_FOREIGN_KEY], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("manifest_id"),
        sa.UniqueConstraint("task_id", name="uq_workflow_manifest_task"),
        sa.UniqueConstraint("idempotency_key", name="uq_workflow_manifest_idempotency"),
    )
    op.create_index("idx_workflow_manifest_run", "workflow_task_manifest", ["run_id"])


def downgrade() -> None:
    op.drop_table("workflow_task_manifest")
    op.drop_table("workflow_task_event")
    op.drop_table("workflow_task_dependency")
    op.drop_table("workflow_task")
    op.drop_table("workflow_run")
