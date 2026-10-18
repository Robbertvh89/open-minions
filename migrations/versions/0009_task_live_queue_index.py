"""partial index of live tasks in FR-22 order for the acquire candidate query

Revision ID: 0009
Revises: 0008
Create Date: 2026-10-18 10:00:00
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0009"
down_revision: str | None = "0008"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # minion_task is small (one row per task, not per subtask): a plain CREATE INDEX is a brief
    # lock. Fail fast rather than queue behind writers.
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.create_index(
        "ix_minion_task_live_queue",
        "minion_task",
        [sa.text("priority DESC"), "created_at", "team", "project", "task_name"],
        postgresql_where=sa.text("status IN ('PENDING', 'RUNNING')"),
    )


def downgrade() -> None:
    op.drop_index("ix_minion_task_live_queue", table_name="minion_task")
