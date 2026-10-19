"""index minion_subtask_acquirement.executor_id for the per-executor open-acquirement cap

Revision ID: 0012
Revises: 0011
Create Date: 2026-10-18 20:00:00
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0012"
down_revision: str | None = "0011"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # The table only holds running work (small); a plain CREATE INDEX is a brief lock.
    op.execute("SET LOCAL lock_timeout = '5s'")  # fail fast rather than queue behind acquires
    op.create_index(
        "ix_minion_subtask_acquirement_executor_id",
        "minion_subtask_acquirement",
        ["executor_id"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_minion_subtask_acquirement_executor_id", table_name="minion_subtask_acquirement"
    )
