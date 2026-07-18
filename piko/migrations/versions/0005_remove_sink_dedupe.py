"""删除没有实现写入流程的中心去重表"""

from typing import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import mysql

revision: str = "0005_remove_sink_dedupe"
down_revision: str | None = "0004_date_job_completion"
branch_labels: Sequence[str] | None = None
depends_on: str | None = None


def upgrade() -> None:
    """删除未被 Sink 使用的中心去重表"""
    op.drop_table("sink_dedupe")


def downgrade() -> None:
    """恢复历史中心去重表结构"""
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
