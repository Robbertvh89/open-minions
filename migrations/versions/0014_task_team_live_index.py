"""partial index minion_task(team) over live tasks for the per-team live-task cap

Revision ID: 0014
Revises: 0013
Create Date: 2026-10-20 12:00:00
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0014"
down_revision: str | None = "0013"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # minion_task is small (one row per task); a plain CREATE INDEX is a brief lock.
    op.execute("SET LOCAL lock_timeout = '5s'")  # fail fast rather than queue behind writers
    op.create_index(
        "ix_minion_task_team_live",
        "minion_task",
        ["team"],
        postgresql_where=sa.text("status IN ('SPLITTING', 'PENDING', 'RUNNING')"),
    )


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.drop_index("ix_minion_task_team_live", table_name="minion_task")
