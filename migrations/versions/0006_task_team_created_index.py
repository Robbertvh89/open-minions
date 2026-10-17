"""index minion_task (team, created_at DESC, project DESC, task_name DESC) for GET /tasks keyset

Revision ID: 0006
Revises: 0005
Create Date: 2026-10-17 08:00:00
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0006"
down_revision: str | None = "0005"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # minion_task has one row per task (small); a plain CREATE INDEX is a brief lock.
    op.create_index(
        "ix_minion_task_team_created",
        "minion_task",
        ["team", sa.text("created_at DESC"), sa.text("project DESC"), sa.text("task_name DESC")],
    )


def downgrade() -> None:
    op.drop_index("ix_minion_task_team_created", table_name="minion_task")
