"""minion_task_counter.cancelled: sharded cancelled-subtask counter (TASK-16)

Revision ID: 0013
Revises: 0012
Create Date: 2026-10-20 12:00:00

List/detail counts read counters instead of aggregating `minion_subtask` (ADR-0015 addendum).
Backfills `cancelled` from the existing CANCELLED rows.

Deploy like 0011: stop the servers and the watchdog first. The backfill scans `minion_subtask`
once while writers are blocked (SHARE lock); the lock_timeout makes it fail fast if something is
still running. A rolling deploy is unsafe: an old server would not bump `cancelled`.
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0013"
down_revision: str | None = "0012"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("LOCK TABLE minion_task_counter IN ACCESS EXCLUSIVE MODE")
    op.execute("LOCK TABLE minion_subtask IN SHARE MODE")  # no status change during the backfill
    op.execute("ALTER TABLE minion_task_counter ADD COLUMN cancelled integer NOT NULL DEFAULT 0")
    op.execute(
        "ALTER TABLE minion_task_counter "
        "ADD CONSTRAINT ck_minion_task_counter_cancelled_nonneg CHECK (cancelled >= 0)"
    )
    # 16 must equal TASK_COUNTER_SHARDS (minion_server.repositories.completion).
    op.execute(
        """
        INSERT INTO minion_task_counter (team, project, task_name, shard, succeeded, failed, cancelled)
        SELECT team, project, task_name, (subtask_id % 16)::smallint, 0, 0, count(*)
        FROM minion_subtask WHERE status = 'CANCELLED'
        GROUP BY team, project, task_name, subtask_id % 16
        ON CONFLICT (team, project, task_name, shard) DO UPDATE SET cancelled = EXCLUDED.cancelled
        """
    )


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("ALTER TABLE minion_task_counter DROP COLUMN cancelled")  # drops its CHECK too
