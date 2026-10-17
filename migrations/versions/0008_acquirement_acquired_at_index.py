"""index minion_subtask_acquirement.acquired_at for the watchdog max-lease scan

Revision ID: 0008
Revises: 0007
Create Date: 2026-10-17 21:00:00
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0008"
down_revision: str | None = "0007"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # The table only holds running work (small); a plain CREATE INDEX is a brief lock.
    op.execute("SET LOCAL lock_timeout = '5s'")  # fail fast rather than queue behind acquires
    op.create_index(
        "ix_minion_subtask_acquirement_acquired_at",
        "minion_subtask_acquirement",
        ["acquired_at"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_minion_subtask_acquirement_acquired_at", table_name="minion_subtask_acquirement"
    )
