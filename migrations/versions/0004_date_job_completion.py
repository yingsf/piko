"""为一次性任务增加持久化完成状态"""

from typing import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import mysql

revision: str = "0004_date_job_completion"
down_revision: str | None = "0003_job_run_attempt"
branch_labels: Sequence[str] | None = None
depends_on: str | None = None


def upgrade() -> None:
    """增加一次性任务完成时间并收敛已有成功记录"""
    op.add_column(
        "scheduled_job",
        sa.Column("completed_at", mysql.DATETIME(fsp=6), nullable=True),
    )
    op.execute(
        "UPDATE scheduled_job AS scheduled "
        "JOIN (SELECT job_id, MAX(end_time) AS completed_at "
        "FROM job_run WHERE status = 'SUCCESS' GROUP BY job_id) AS runs "
        "ON scheduled.job_id = runs.job_id "
        "SET scheduled.enabled = 0, scheduled.completed_at = runs.completed_at "
        "WHERE scheduled.schedule_type = 'date' AND scheduled.enabled = 1"
    )


def downgrade() -> None:
    """删除一次性任务完成时间"""
    op.drop_column("scheduled_job", "completed_at")
