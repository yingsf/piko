"""将 schema 版本收敛为稳定的语义化命名

此前迁移链以阶段性的实现细节命名 head（如 ``0005_remove_sink_dedupe``），
这类名字会随每次重构变动，作为长期契约并不合适。本节点不修改任何表结构，
仅作为版本锚点：把 head 从 ``0005_remove_sink_dedupe`` 推进到 ``schema_v1``，
表示当前 schema 定型的第一版基线。后续破坏性变更再演进到 ``schema_v2``。

注意：``alembic_version.version_num`` 由 Alembic 框架在执行完本 revision 后
自动更新为本 revision id（即 ``schema_v1``），因此 upgrade/downgrade 无需
手动 UPDATE 该表。
"""

from typing import Sequence

revision: str = "schema_v1"
down_revision: str | None = "0005_remove_sink_dedupe"
branch_labels: Sequence[str] | None = None
depends_on: str | None = None


def upgrade() -> None:
    """版本锚点：无表结构变更，仅推进 head 到 schema_v1"""
    # Alembic 执行完本 revision 后会自动把 alembic_version.version_num
    # 更新为 "schema_v1"，这里无需任何 SQL。


def downgrade() -> None:
    """回退到上一阶段的版本锚点"""
    # Alembic 会自动把 alembic_version.version_num 回退为 down_revision。
