"""为任务锁增加租约和 owner token"""

from typing import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import mysql

revision: str = "0002_job_lock_lease"
down_revision: str | None = "0001_initial"
branch_labels: Sequence[str] | None = None
depends_on: str | None = None


def upgrade() -> None:
    """增加任务锁租约字段并迁移已有锁"""
    op.add_column(
        "job_lock",
        sa.Column("owner_token", sa.String(length=64), nullable=True),
    )
    op.add_column(
        "job_lock",
        sa.Column("expires_at", mysql.DATETIME(fsp=6), nullable=True),
    )
    op.execute(
        "UPDATE job_lock "
        "SET owner_token = UUID(), "
        "expires_at = DATE_ADD(acquired_at, INTERVAL 300 SECOND)"
    )
    op.alter_column(
        "job_lock",
        "owner_token",
        existing_type=sa.String(length=64),
        nullable=False,
    )
    op.alter_column(
        "job_lock",
        "expires_at",
        existing_type=mysql.DATETIME(fsp=6),
        nullable=False,
    )


def downgrade() -> None:
    """删除任务锁租约字段"""
    op.drop_column("job_lock", "expires_at")
    op.drop_column("job_lock", "owner_token")
