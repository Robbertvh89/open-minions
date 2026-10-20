"""live-queue index on (-priority): uniform direction for the row-compare cursor walk

Revision ID: 0015
Revises: 0014
Create Date: 2026-10-20 15:00:00
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0015"
down_revision: str | None = "0014"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_WHERE = "status IN ('PENDING', 'RUNNING')"


def upgrade() -> None:
    # minion_task is small (one row per task): a plain CREATE INDEX is a brief lock, as in 0009.
    # Drop and create run in one transaction, so there is no window without the index.
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("DROP INDEX ix_minion_task_live_queue")
    op.execute(
        "CREATE INDEX ix_minion_task_live_queue ON minion_task"
        f" ((-priority), created_at, team, project, task_name) WHERE {_WHERE}"
    )


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("DROP INDEX ix_minion_task_live_queue")
    op.execute(
        "CREATE INDEX ix_minion_task_live_queue ON minion_task"
        f" (priority DESC, created_at, team, project, task_name) WHERE {_WHERE}"
    )
