"""为 JobRun 增加 attempt 唯一性和历史保留约束"""

from typing import Sequence

from alembic import op

revision: str = "0003_job_run_attempt"
down_revision: str | None = "0002_job_lock_lease"
branch_labels: Sequence[str] | None = None
depends_on: str | None = None


def upgrade() -> None:
    """清理历史重复记录并建立 JobRun 唯一约束"""
    op.execute(
        "DELETE older FROM job_run AS older "
        "JOIN job_run AS newer "
        "ON older.job_id = newer.job_id "
        "AND older.scheduled_time = newer.scheduled_time "
        "AND older.attempt = newer.attempt "
        "AND older.run_id < newer.run_id"
    )
    op.create_unique_constraint(
        "uq_run_job_time_attempt",
        "job_run",
        ["job_id", "scheduled_time", "attempt"],
    )


def downgrade() -> None:
    """删除 JobRun 唯一约束"""
    op.drop_constraint("uq_run_job_time_attempt", "job_run", type_="unique")
