"""live-queue index on (-priority): uniform direction for the row-compare cursor walk

Revision ID: 0016
Revises: 0015
Create Date: 2026-10-20 15:00:00
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0016"
down_revision: str | None = "0015"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_WHERE = "status IN ('PENDING', 'RUNNING')"


def upgrade() -> None:
    # minion_task is small (one row per task): a plain CREATE INDEX is a short lock, as in 0009.
    # DROP INDEX takes ACCESS EXCLUSIVE on minion_task until this transaction commits, so every
    # reader and writer of minion_task (acquire, create, watchdog) waits for the CREATE INDEX
    # (tens of ms at 10k live tasks, seconds at millions of rows); lock_timeout makes it fail fast
    # instead of queueing behind long transactions. Stop the server and watchdog first (README
    # Upgrading, as for 0013). Do not reorder to create-then-drop: both indexes would share one
    # name. In one transaction there is no window without the index.
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
