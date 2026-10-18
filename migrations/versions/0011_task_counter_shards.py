"""sharded task counters: minion_task_counter replaces minion_task.succeeded/failed_subtasks

Revision ID: 0011
Revises: 0010
Create Date: 2026-10-18 14:00:00

Every completion used to UPDATE the one minion_task row of its task (a hot row). Counters now live
in `minion_task_counter`, sharded by `subtask_id % 16`, and are summed on read (ADR-0015).
`minion_ro.task` keeps its column names and types.

Deploy: migrate with the servers and the watchdog stopped. The migration takes an ACCESS
EXCLUSIVE lock on minion_task (small: one row per task) and fails fast after 5 s.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0011"
down_revision: str | None = "0010"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_VIEW_SHARDED = """
CREATE OR REPLACE VIEW minion_ro.task AS
SELECT t.team, t.project, t.task_name, t.type, t.model_id, t.engine, t.model_memory_gb,
       t.records_per_subtask, t.priority, t.max_attempts, t.allowed_pool, t.status,
       t.total_subtasks,
       coalesce(c.succeeded, 0)::integer AS succeeded_subtasks,
       coalesce(c.failed, 0)::integer AS failed_subtasks,
       t.created_by, t.created_at, t.updated_at, t.finished_at
FROM public.minion_task t
LEFT JOIN LATERAL (
    SELECT sum(k.succeeded) AS succeeded, sum(k.failed) AS failed
    FROM public.minion_task_counter k
    WHERE k.team = t.team AND k.project = t.project AND k.task_name = t.task_name
) c ON true
"""

_VIEW_COLUMNAR = """
CREATE OR REPLACE VIEW minion_ro.task AS
SELECT team, project, task_name, type, model_id, engine, model_memory_gb, records_per_subtask,
       priority, max_attempts, allowed_pool, status, total_subtasks, succeeded_subtasks,
       failed_subtasks, created_by, created_at, updated_at, finished_at
FROM public.minion_task
"""

_GRANT = """
DO $$
BEGIN
    IF EXISTS (SELECT FROM pg_roles WHERE rolname = 'minion_readonly') THEN
        GRANT SELECT ON ALL TABLES IN SCHEMA minion_ro TO minion_readonly;
    END IF;
END
$$
"""


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("LOCK TABLE minion_task IN ACCESS EXCLUSIVE MODE")
    op.create_table(
        "minion_task_counter",
        sa.Column("team", sa.String(length=64), nullable=False),
        sa.Column("project", sa.String(length=64), nullable=False),
        sa.Column("task_name", sa.String(length=128), nullable=False),
        sa.Column("shard", sa.SmallInteger(), nullable=False),
        sa.Column("succeeded", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("failed", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.CheckConstraint("succeeded >= 0", name=op.f("ck_minion_task_counter_succeeded_nonneg")),
        sa.CheckConstraint("failed >= 0", name=op.f("ck_minion_task_counter_failed_nonneg")),
        sa.ForeignKeyConstraint(
            ["team", "project", "task_name"],
            ["minion_task.team", "minion_task.project", "minion_task.task_name"],
            name=op.f("fk_minion_task_counter_team_minion_task"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint(
            "team", "project", "task_name", "shard", name=op.f("pk_minion_task_counter")
        ),
    )
    op.execute(
        "INSERT INTO minion_task_counter (team, project, task_name, shard, succeeded, failed) "
        "SELECT team, project, task_name, 0, succeeded_subtasks, failed_subtasks "
        "FROM minion_task WHERE succeeded_subtasks > 0 OR failed_subtasks > 0"
    )
    # The view must stop using the columns before they can be dropped.
    op.execute(_VIEW_SHARDED)
    op.drop_column("minion_task", "succeeded_subtasks")  # drops its CHECK too
    op.drop_column("minion_task", "failed_subtasks")
    op.execute(_GRANT)


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("LOCK TABLE minion_task IN ACCESS EXCLUSIVE MODE")
    for col in ("succeeded_subtasks", "failed_subtasks"):
        op.add_column(
            "minion_task",
            sa.Column(col, sa.Integer(), server_default=sa.text("0"), nullable=False),
        )
    op.execute(
        "UPDATE minion_task t SET "
        "succeeded_subtasks = coalesce(s.succeeded, 0), failed_subtasks = coalesce(s.failed, 0) "
        "FROM (SELECT team, project, task_name, sum(succeeded) AS succeeded, sum(failed) AS failed "
        "      FROM minion_task_counter GROUP BY team, project, task_name) s "
        "WHERE t.team = s.team AND t.project = s.project AND t.task_name = s.task_name"
    )
    op.create_check_constraint(
        op.f("ck_minion_task_succeeded_subtasks_nonneg"), "minion_task", "succeeded_subtasks >= 0"
    )
    op.create_check_constraint(
        op.f("ck_minion_task_failed_subtasks_nonneg"), "minion_task", "failed_subtasks >= 0"
    )
    op.execute(_VIEW_COLUMNAR)
    op.drop_table("minion_task_counter")
    op.execute(_GRANT)
