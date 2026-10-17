"""schema minion_ro: read-only views and role minion_readonly for Grafana (TASK-6.5, FR-40)

The views expose no secrets and no payload locators: no token hash, task params, input/output
object keys, acquirement ids or free-text errors. They run with the owner's rights (default
security-definer views), so `minion_readonly` needs no privilege on the base tables at all.

The role is cluster-wide. It is created here only when missing (deploy/postgres/initdb creates it
first in the compose stack) and only when the migrating user may create roles; otherwise the
migration emits a NOTICE and skips the role and its grants (the views are still created, an
operator creates the role and runs the GRANTs printed in the NOTICE).

Maintenance: a view blocks ALTER COLUMN TYPE / DROP COLUMN on the columns it uses. A migration
that changes one drops the view first and recreates it (see the db-migration skill).

Revision ID: 0007
Revises: 0006
Create Date: 2026-10-17 10:00:00
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0007"
down_revision: str | None = "0006"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

VIEWS = ("executor", "task", "subtask", "attempt", "heartbeat")

# Operator-only data (S-11): hostname/owner/profile/paused_reason reveal when an owner is active.
# Grafana is an operator tool; do not hand `minion_grafana` to ordinary users.
#
# `online` mirrors GET /executors (repositories/executors.py list_fleet): ACTIVE and heard from
# within 600 s (the newest of last_seen_at and the heartbeat) while it holds a running subtask,
# else within 120 s of last_seen_at. 600/120 are the defaults of the settings
# `executor_online_busy_s` / `executor_online_idle_s`; a deployment that changes those must
# recreate this view (the coupling is deliberate: a view cannot read server settings).
_EXECUTOR = """
CREATE VIEW minion_ro.executor AS
WITH hb AS (
    SELECT q.executor_id, max(h.last_seen_at) AS last_hb
    FROM public.minion_executor_heartbeat h
    JOIN public.minion_subtask_acquirement q USING (acquirement_id)
    GROUP BY q.executor_id
)
SELECT e.executor_id, e.owner, e.pool, e.hostname, e.chip, e.total_memory_gb, e.os_version,
       e.engines, e.status, e.profile, e.paused_reason,
       (e.budget ->> 'memory_gb')::numeric AS budget_memory_gb,
       e.registered_at, e.last_seen_at, e.token_expires_at,
       coalesce(
           e.status = 'ACTIVE'
           AND greatest(e.last_seen_at, hb.last_hb) >= now()
               - CASE WHEN hb.last_hb IS NOT NULL THEN interval '600 seconds'
                      ELSE interval '120 seconds' END,
           false
       ) AS online
FROM public.minion_executor e
LEFT JOIN hb USING (executor_id)
"""

# The error code is exposed only when it has the shape of a stable code (never free text).
_CODE = "CASE WHEN {col} ->> 'code' ~ '^[A-Z0-9_]{{1,64}}$' THEN {col} ->> 'code' END"

_TASK = """
CREATE VIEW minion_ro.task AS
SELECT team, project, task_name, type, model_id, engine, model_memory_gb, records_per_subtask,
       priority, max_attempts, allowed_pool, status, total_subtasks, succeeded_subtasks,
       failed_subtasks, created_by, created_at, updated_at, finished_at
FROM public.minion_task
"""

_SUBTASK = """
CREATE VIEW minion_ro.subtask AS
SELECT team, project, task_name, subtask_id, status, attempt, max_attempts, record_count,
       {code} AS error_code, created_at, updated_at
FROM public.minion_subtask
""".format(code=_CODE.format(col="error"))

# One row per finished attempt, with the task's model and the subtask's record count so panels
# need no joins. `error_code` is the failure code or, for RELEASED, the release reason.
_ATTEMPT = """
CREATE VIEW minion_ro.attempt AS
SELECT a.id, a.team, a.project, a.task_name, a.subtask_id, a.attempt, a.executor_id, a.outcome,
       a.started_at, a.finished_at, {code} AS error_code,
       t.model_id, s.record_count
FROM public.minion_subtask_attempt a
LEFT JOIN public.minion_task t USING (team, project, task_name)
LEFT JOIN public.minion_subtask s USING (team, project, task_name, subtask_id)
""".format(code=_CODE.format(col="a.error"))

# One row per running subtask (its heartbeat): who is busy and how far along.
_HEARTBEAT = """
CREATE VIEW minion_ro.heartbeat AS
SELECT q.executor_id, q.team, q.project, q.task_name, q.subtask_id, q.attempt, q.acquired_at,
       h.last_seen_at, (h.progress ->> 'records_done')::int AS records_done,
       (h.progress ->> 'records_total')::int AS records_total
FROM public.minion_executor_heartbeat h
JOIN public.minion_subtask_acquirement q USING (acquirement_id)
"""

_ROLE = """
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'minion_readonly') THEN
        BEGIN
            CREATE ROLE minion_readonly NOLOGIN;
        EXCEPTION WHEN insufficient_privilege THEN
            RAISE NOTICE 'cannot create role minion_readonly (needs CREATEROLE): skipped. '
                'Create it, then: GRANT USAGE ON SCHEMA minion_ro TO minion_readonly; '
                'GRANT SELECT ON ALL TABLES IN SCHEMA minion_ro TO minion_readonly;';
        END;
    END IF;
    IF EXISTS (SELECT FROM pg_roles WHERE rolname = 'minion_readonly') THEN
        GRANT USAGE ON SCHEMA minion_ro TO minion_readonly;
        GRANT SELECT ON ALL TABLES IN SCHEMA minion_ro TO minion_readonly;
    END IF;
END
$$
"""


def upgrade() -> None:
    op.execute("CREATE SCHEMA minion_ro")
    for ddl in (_EXECUTOR, _TASK, _SUBTASK, _ATTEMPT, _HEARTBEAT):
        op.execute(ddl)
    op.execute(_ROLE)


def downgrade() -> None:
    # The role is cluster-wide (shared by other databases, owned by deploy/postgres/initdb) and is
    # left in place; its grants on this database's schema go away with the schema.
    for view in reversed(VIEWS):
        op.execute(f"DROP VIEW minion_ro.{view}")
    op.execute("DROP SCHEMA minion_ro")
