"""index minion_task(team, status) for the per-team live-task cap

Revision ID: 0014
Revises: 0013
Create Date: 2026-10-20 12:00:00
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0014"
down_revision: str | None = "0013"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # minion_task is small (one row per task); a plain CREATE INDEX is a brief lock.
    op.execute("SET LOCAL lock_timeout = '5s'")  # fail fast rather than queue behind writers
    op.create_index(
        "ix_minion_task_team_status",
        "minion_task",
        ["team", "status"],
    )
    # Per-(team, status) frequencies: without them the planner assumes team and status are
    # independent, overestimates the matches of the bounded live count for a team with a long
    # finished history, and picks a Seq Scan that stops "early" only on paper.
    op.execute(
        "CREATE STATISTICS st_minion_task_team_status (mcv) ON team, status FROM minion_task"
    )


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("DROP STATISTICS IF EXISTS st_minion_task_team_status")
    op.drop_index("ix_minion_task_team_status", table_name="minion_task")
