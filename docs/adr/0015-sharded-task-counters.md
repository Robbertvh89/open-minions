# ADR-0015: Sharded task counters and post-commit task settle

- Status: **Accepted** (TASK-7.4.8, M6.4h)
- Deviates from paper: yes (an eighth table; terminal task status is set just after the commit)

## Context

Every completion ran `UPDATE minion_task SET succeeded_subtasks = ..., failed_subtasks = ...` for its
task, so all completions of one task row-locked the same row. After TASK-7.4.3 this was the top
`pg_stat_statements` entry (15.6 ms mean in B-20k, 32.6 ms in A stress).

## Decision

- New table `minion_task_counter(team, project, task_name, shard smallint, succeeded, failed)`, PK
  with `shard`, FK to `minion_task` `ON DELETE CASCADE`. `shard = subtask_id % 16`
  (`TASK_COUNTER_SHARDS`). The task's counters are the **sum over shards**.
- Migration 0011 backfills shard 0 from the old columns, recreates `minion_ro.task` with the same
  column names and types (`succeeded_subtasks` / `failed_subtasks` = sums), then drops the two
  `minion_task` columns. Deploy with servers and watchdog stopped (ACCESS EXCLUSIVE lock on
  `minion_task`, 5 s lock timeout). The downgrade folds the shards back.
- Owner ops (`complete`, and `fail` / `release` / reclaim when the subtask ends `FAILED`) upsert
  their shard inside the owner transaction. Lock order: acquirement, heartbeat, subtask, counter
  shard. Completions of different subtasks hit different shard rows (16-way instead of 1).
- The terminal task status is no longer decided in the owner transaction. After it commits,
  `settle_task` runs a short second transaction: read status, `total_subtasks` and the shard sums;
  if the task is `PENDING`/`RUNNING` and `derive_task_status` is terminal, `UPDATE ... WHERE status IN
  (PENDING, RUNNING)` sets status and `finished_at`; only the call with rowcount 1 made the
  transition. Settling after commit is what makes write skew harmless: two last completions that were
  both uncommitted (each missing the other's bump) both settle after their own commit, and the later
  one sees every count.
- A settle that fails (DB error) is logged as `task_settle_failed` (error class only) and swallowed:
  the owner op is already committed and must not turn into an error. The idempotent complete-retry
  branch and reclaim that fails the last subtask do not settle inline.
- Backstop: the watchdog tick runs `settle_stuck` after the reclaim loop (live tasks whose shard sums
  cover `total_subtasks`, one transaction per task). So a finished task shows `RUNNING` for at most
  one watchdog interval after a lost settle.

## Consequences

- 8 tables instead of the paper's 7; SPEC §7 and ARCHITECTURE §5.1 are updated.
- Terminal status appears just after, not in, the last transition's commit.
- Cancel window: a cancel committed between the last bump and the settle wins (the settle's
  `WHERE status IN (PENDING, RUNNING)` skips it); counters still equal the subtask statuses.
- `minion_task.updated_at` no longer moves per completion (only on status changes); progress
  consumers should read counts or `minion_ro.task`.
- Readers sum the shards (`minion_ro.task`, `minion-scale` verify and progress poll).
- Not done: a counter-less derivation from `minion_subtask` (a count over millions of rows per
  completion); a deferred batch job (stale counts for minutes).
