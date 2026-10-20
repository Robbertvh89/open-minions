---
id: TASK-16
title: >-
  Per-status subtask counters on minion_task (avoid aggregating subtasks on
  list/detail)
status: Done
assignee: []
created_date: '2026-10-17 07:22'
updated_date: '2026-10-20 02:58'
labels:
  - server
  - performance
dependencies: []
ordinal: 43000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
GET /tasks aggregates every subtask of up to 100 tasks per page (up to ~10M rows); keep counters on minion_task maintained in the same transactions as status changes, or cache. From TASK-6.4 security review.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 Counters stay consistent under concurrent acquire/complete/reclaim (integration test)
- [x] #2 List/detail counts read without scanning subtasks; per task O(16 shards + open acquirements <= executors x MINION_MAX_OPEN_ACQUIREMENTS)
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
# TASK-16 plan: per-status task counts read O(1) in subtasks

Worktree `/Users/robbertvanhintum/Documents/claude_code/open-minions-task16`, branch `perf/task-16-task-list-counters`.

## 1. Goal and acceptance criteria

`GET /tasks` and `GET /tasks/{team}/{project}/{task_name}` (also the web UI and the response of
`POST /tasks` and `cancel`) return `counts: {total, pending, running, succeeded, failed, cancelled}`
(SPEC §6 `TaskOut`, FR-7). Today they come from `GROUP BY status` over `minion_subtask`: up to 100
tasks x 100k subtasks per page (SPEC S-12). Goal: read them from bounded state kept in the same
transactions as the status changes.

- AC#1 "List/detail counts read O(1) per task" maps to FR-7, S-12, NF-5 (all state stays in Postgres).
  Proof: one SQL statement per list page and per detail, and EXPLAIN shows no `minion_subtask` node.
- AC#2 "Counters stay consistent under concurrent acquire/complete/reclaim" maps to NF-2 and NF-3
  (the `minion-scale verify` counter check) and invariants 2 and 3. Proof: an integration storm
  that checks counters against a `minion_subtask` aggregate in one REPEATABLE READ snapshot, during
  the storm and after it.

## 2. What the counters track today, and the gaps

| API count | Source today | After TASK-16 |
|---|---|---|
| total | `minion_task.total_subtasks` (set by `mark_split_done`, `repositories/tasks.py:238`; the splitter checks it equals the row count, `services/splitter.py:187`) | unchanged |
| succeeded | aggregate | `sum(minion_task_counter.succeeded)` (already kept, ADR-0015) |
| failed | aggregate | `sum(minion_task_counter.failed)` (already kept) |
| cancelled | aggregate | **new** shard column `minion_task_counter.cancelled` (migration 0013) |
| running | aggregate | **`count(*)` of `minion_subtask_acquirement` for the task**: no counter |
| pending | aggregate | `max(0, total - running - succeeded - failed - cancelled)` |

Why running comes from the acquirement table and is not a counter:

- RUNNING holds exactly when an acquirement row exists, in every committed state. The only way in is
  the claim: the acquirement INSERT, then `_after_win` sets RUNNING in the same transaction, and a
  failed re-check rolls back (`repositories/acquisition.py:224-257`, `services/acquisition.py:118`).
  Every way out (`finish_attempt`, `repositories/completion.py:205-236`; `finish_reclaimed`,
  `repositories/reclaim.py:230-270`) deletes the acquirement and moves the subtask in one
  transaction. `minion-scale verify` already checks this as the orphan/acquirement check
  (`minion_scale/verify.py`, the query just above `_COUNTERS`).
- A running counter would add a shard upsert to every acquire, and a decrement to every retryable
  fail, release and requeueing reclaim. Acquire is the measured bottleneck (ADR-0015 "Measured", NF-1).
  Counting acquirements adds no writes on the hot paths.
- Cost: an index range scan of `uq_minion_subtask_acquirement_subtask` (prefix team, project,
  task_name). This scan is bounded by fleet concurrency (executors x `MINION_MAX_OPEN_ACQUIREMENTS`,
  e.g. 500 x 4 = 2,000 rows in total across all tasks), not by task size. So the read is "O(1) in
  subtasks" but not strictly O(1). **Open question for the orchestrator:** accept this bound, or
  require a strict counter (alternative A below, with a measured acquire cost).

Alternative A (rejected): shard columns `running` (and maybe `pending`), bumped by acquire, fail,
release and reclaim. Reads would be strictly O(16) rows per task. But it adds +1 upsert per acquire,
and acquire and complete would then contend on the same 16 rows of the hot task. It would need a
`minion-scale` A/B run to justify, which is not planned for this task.

Why pending is derived: in any single snapshot, total = number of rows (after the split) =
PENDING + RUNNING + SUCCEEDED + FAILED + CANCELLED. So `pending = total - others` is exact, not
just eventually right, provided all inputs come from **one statement** (one READ COMMITTED snapshot).
The read must therefore be one statement (see step 3). Today it is two (page, then grouped counts).

Behaviour change (edge cases only):
- While a task is SPLITTING, and for a task whose split never finished (FAILED split, or cancelled
  while SPLITTING), total is 0, so pending reads 0 instead of the number of rows inserted so far.
  `cancelled` still shows the rows cancelled during the split, as today.
- The clamp `max(0, ...)` only exists for that case. Document it in SPEC §6.

## 3. Every subtask status transition and its counter effect

| Transition | Code | Counter write after TASK-16 |
|---|---|---|
| split insert -> PENDING | `repositories/tasks.py:217-235` | none (derived) |
| acquire PENDING -> RUNNING | `repositories/acquisition.py:238-256` | none (acquirement row) |
| complete RUNNING -> SUCCEEDED | `services/completion.py:120-127` | `succeeded+1` (unchanged) |
| fail / release cap / reclaim -> FAILED | `services/completion.py:165-166`, `repositories/reclaim.py:265-270` | `failed+1` (unchanged) |
| fail / release cap -> PENDING (retry) | `services/completion.py:164` | none |
| fail / release cap -> CANCELLED (task cancelled) | `services/completion.py:136-137,164` | **`cancelled+1`** |
| release on a cancelled task -> CANCELLED | `services/completion.py:222-229` | **`cancelled+1`** |
| plain release -> PENDING | `services/completion.py:243-250` | none |
| reclaim -> PENDING / CANCELLED (incl. free NOT_ACKNOWLEDGED) | `services/reclaim.py:52-86`, writer `repositories/reclaim.py:230-270` | **`cancelled+n` per shard** for CANCELLED |
| cancel txn 2 PENDING -> CANCELLED | `repositories/tasks.py:318-338` | **`cancelled+n` per shard** |
| cancel sweep PENDING -> CANCELLED | `repositories/tasks.py:341-385` | **`cancelled+n` per (task, shard)** |

No transition leaves SUCCEEDED, FAILED or CANCELLED, so every counter only grows (the CHECKs stay `>= 0`).

## 4. Steps

### Step 1: migration 0013 + model column (implementer, `db-migration` skill)

Files:
- **new** `migrations/versions/0013_task_counter_cancelled.py` (revises `0012`). Do not edit 0011.
  ```sql
  SET LOCAL lock_timeout = '5s';
  LOCK TABLE minion_task_counter IN ACCESS EXCLUSIVE MODE;
  LOCK TABLE minion_subtask IN SHARE MODE;   -- no status change during the backfill aggregate
  ALTER TABLE minion_task_counter ADD COLUMN cancelled integer NOT NULL DEFAULT 0;  -- metadata-only
  ALTER TABLE minion_task_counter ADD CONSTRAINT ck_minion_task_counter_cancelled_nonneg CHECK (cancelled >= 0);
  INSERT INTO minion_task_counter (team, project, task_name, shard, succeeded, failed, cancelled)
  SELECT team, project, task_name, (subtask_id % 16)::smallint, 0, 0, count(*)
  FROM minion_subtask WHERE status = 'CANCELLED'
  GROUP BY team, project, task_name, subtask_id % 16
  ON CONFLICT (team, project, task_name, shard) DO UPDATE SET cancelled = EXCLUDED.cancelled;
  ```
  - Hard-code `16` with a comment that it must equal `TASK_COUNTER_SHARDS`
    (`repositories/completion.py:25`). Migrations do not import app code.
  - The downgrade drops the column, which also drops its CHECK. `minion_ro.task` does not use the
    column, so no view change is needed.
  - Docstring: deploy with the servers and the watchdog stopped, like 0011. The backfill scans
    `minion_subtask` once, while writers are blocked by SHARE.
- `packages/minion-server/src/minion_server/db/models.py:141-165` `TaskCounter`: add
  `cancelled: Mapped[int] = mapped_column(Integer, server_default=text("0"))` and
  `CheckConstraint("cancelled >= 0", name="cancelled_nonneg")`. Update the docstring.

Tests:
- `test_migrations.py`, new `test_0013_cancelled_backfill_and_round_trip` (integration). Upgrade to
  0012. Seed 1 task with 40 subtasks: CANCELLED on ids spread across shards, plus one pre-existing
  counter row (succeeded=2) on a shard that also has cancelled rows. Upgrade to 0013. Then:
  - `sum(cancelled)` equals the aggregate, and the per-shard values equal `count(*) ... GROUP BY subtask_id % 16`;
  - succeeded is untouched;
  - downgrade to 0012 removes the column; upgrade to head again.
- `test_migrated_schema_matches_models` (`test_migrations.py:33`) must stay green; it covers the model/DDL match.

Invariants: none touched (schema only).

### Step 2: write path, the cancelled bumps (implementer)

Files and interfaces:
- `repositories/completion.py:239-262`: generalise the upsert.
  ```python
  ShardKey = tuple[str, str, str, int]  # team, project, task_name, shard

  async def bump_shard(session: AsyncSession, key: ShardKey, *, succeeded: int = 0,
                       failed: int = 0, cancelled: int = 0) -> None: ...   # one upsert, adds all three
  async def bump_counters(session: AsyncSession, o: Owned, *, succeeded: int = 0,
                          failed: int = 0, cancelled: int = 0) -> None:     # delegates to bump_shard
  def shard_totals(keys: Iterable[tuple[str, str, str, int]]) -> list[tuple[ShardKey, int]]:
      """(team, project, task_name, subtask_id) rows -> sorted [(shard key, n)]; pure."""
  async def bump_cancelled(session: AsyncSession, keys: Iterable[tuple[str, str, str, int]]) -> None:
      """Add cancelled per (task, shard), in sorted shard-key order (deadlock-free)."""
  ```
  The existing calls `bump_counters(..., succeeded=1, failed=0)` keep working.
- `services/completion.py:165-166` `finish_failed`: also `elif status == SubtaskStatus.CANCELLED:
  await repo.bump_counters(session, o, cancelled=1)`.
- `services/completion.py:222-229` `release`, cancelled branch: add `bump_counters(session, o, cancelled=1)`
  after `finish_attempt`.
- `repositories/reclaim.py:264-270` `finish_reclaimed`: build per-shard `(failed, cancelled)` from
  FAILED and CANCELLED items, then one `bump_shard` per shard in sorted order (one upsert per shard,
  as today).
- `repositories/tasks.py:318-338` `cancel_pending_subtasks`: `.returning(Subtask.subtask_id)`, then
  `bump_cancelled` with those ids, still in txn 2. Return `len(ids)`, so the return type stays `int`
  and the callers (`services/tasks.py:186-190`) are unchanged.
- `repositories/tasks.py:375-385` `sweep_cancelled`: `.returning(team, project, task_name, subtask_id)`,
  then `bump_cancelled`, then return the count.
- `tasks.py` imports `bump_cancelled` from `completion.py`. There is no cycle, because `completion.py`
  imports only models.

Lock order (stays acyclic; document it in ARCHITECTURE §4): in every transaction that writes a
counter, the shard upserts are the **last** row locks taken, in sorted shard-key order.
- complete / fail / release / reclaim: acquirement -> heartbeat -> subtask -> shard(s), as today.
- cancel txn 2 and the sweep: PENDING subtask rows (SKIP LOCKED, bounded to 10,000) -> shards
  (sorted, at most 16 per task).
- Acquire takes no shard lock.

A transaction that holds a shard lock never waits for a subtask, acquirement, executor or task row
lock. So no wait-for cycle can pass through a shard. The FK KEY SHARE on `minion_task` from the
upsert's insert path is unchanged (ADR-0015) and compatible with cancel txn 1's NO KEY UPDATE.

Tests (unit):
- `shard_totals` grouping and ordering.
- `finish_failed` / `release` mapping. Existing test style: `test_settle_unit.py`; if a fake session
  is too heavy, these move to the integration tests of step 4.

Invariants:
- 2 and 3: no change to the claim or to ownership checks. Counter writes are inside the owner
  transaction, after `lock_owned`.
- 5: no S3 change.

### Step 3: read path, one statement per detail and per page (implementer)

Files and interfaces:
- `repositories/tasks.py`, new helpers:
  ```python
  def _count_columns(t: type[Task] | AliasedClass[Task]) -> tuple[ColumnElement[int], ...]:
      """running, succeeded, failed, cancelled for task row `t`, correlated:
      LEFT JOIN LATERAL (SELECT coalesce(sum(succeeded),0), coalesce(sum(failed),0),
      coalesce(sum(cancelled),0) FROM minion_task_counter WHERE key = t.key)
      + scalar (SELECT count(*) FROM minion_subtask_acquirement WHERE key = t.key)."""


  def derive_counts(
      total: int, running: int, succeeded: int, failed: int, cancelled: int
  ) -> dict[SubtaskStatus, int]:
      """PENDING = max(0, total - running - succeeded - failed - cancelled); pure."""
  ```
- `get_task` (`repositories/tasks.py:87-104`): one `select(Task, *counts).where(key)`. Return type
  unchanged: `tuple[Task, dict[SubtaskStatus, int]] | None`.
- `list_tasks` (`repositories/tasks.py:447-480`):
  - Wrap `_list_tasks_stmt(...)` as a subquery `page` (it already has LIMIT limit+1).
  - `t = aliased(Task, page)`; `select(t, *count_columns(t)).order_by(<same 4 keys DESC>)`, so the
    counts are evaluated for at most limit+1 rows.
  - Delete the grouped `minion_subtask` query (`:467-479`).
  - Keep `_list_tasks_stmt` itself unchanged, so `test_multi_team_branches_use_team_created_index`
    (`test_read_api.py:394-418`) still applies.
  - Return type unchanged.
- No change needed in `services/tasks.py:75-101` (`to_out`), `api/tasks.py:103-135,148-157`,
  `ui/`, `minion-cli`: same dict, same `TaskCounts` schema (`minion_common/schemas.py:154-160`).
- `completion.task_counts` / settle (`repositories/completion.py:272-300`) stay as they are (they
  need only succeeded and failed).

Tests (integration, `test_read_api.py`):
- Update `test_list_tasks_counts_is_one_grouped_query` (`:190-205`). Rename it to
  `test_list_tasks_counts_one_statement`. Assert `len(stmts) == 1` for list and for detail, with 5
  tasks and with 50 tasks (no growth with page size).
- New `test_counts_plans_never_touch_subtasks`:
  - Seed one task with N = 20,000 subtasks (`INSERT ... SELECT generate_series`) and its counters,
    plus 3 acquirements.
  - Compile the list and the detail statement; run `EXPLAIN (FORMAT JSON)`. Walk the plan tree:
    no node has `Relation Name == 'minion_subtask'`. Expect index scans on `pk_minion_task_counter`
    and `uq_minion_subtask_acquirement_subtask`.
  - Run this once with `enable_seqscan = off` and once with defaults.
- `derive_counts` unit test, incl. the SPLITTING clamp (total 0, cancelled 3 -> pending 0).
- Existing count assertions must stay green: `test_read_api.py:118`, `test_tasks_api.py:189`,
  `test_cancel.py:150,313`, `test_protocol_races.py:645,696`.
  - Tests that write `minion_subtask.status` with raw SQL and then read API counts would now
    disagree. Found so far, all only checking shard sums or subtask rows, not API counts:
    `test_protocol_races.py:462`, `test_reclaim_races.py:255`, `test_read_api.py:216`.
  - If one breaks, fix the test by also bumping the counter, through a conftest helper. Never fix
    it in the code.

Invariants:
- 1 and 4: read-only server path.
- S-11 (no keys or URLs in counts): unchanged.

### Step 4: AC#2 concurrency and consistency tests (test-engineer)

New `packages/minion-server/tests/test_task_counters.py` (integration). Reuse the `app_env`,
`_acquire_all` and `_put_output` patterns of `test_protocol_races.py` (real HTTP app + testcontainers
Postgres and S3).

- conftest helper `assert_counts_match(sm, team, project, name)`. In **one REPEATABLE READ
  transaction** (`await s.connection(execution_options={"isolation_level": "REPEATABLE READ"})`):
  - `repo.get_task` counts;
  - `SELECT status, count(*) FROM minion_subtask WHERE key GROUP BY status`;
  - `SELECT count(*) FROM minion_subtask_acquirement WHERE key`.
  Assert, for all statuses, that `derive_counts` == the aggregate and that the acquirement count == RUNNING.
  Because both reads share one snapshot, the check is exact even mid-storm.
- `test_counts_consistent_under_storm`:
  - 1 task, 200 subtasks, `max_attempts=3`, 24 executors.
  - Workers loop for about 10 s: acquire, then a weighted choice of complete (with output put),
    fail (retryable / non-retryable), release, or "vanish" (the heartbeat is backdated by SQL; the
    acquirement is left).
  - In parallel: a reclaimer loop calling `reclaim_one` / watchdog `tick` on stale acquirements,
    and a checker coroutine calling `assert_counts_match` every 50 ms (expect more than 50 checks).
  - At the end: drain, then `tick`. Check `assert_counts_match`, check the API `counts` sum to
    total, and check the task status equals `derive_task_status` of the counts.
- `test_counts_consistent_with_cancel_mid_storm`: same storm, plus `POST .../cancel` after about 3 s.
  Afterwards check:
  - in-flight complete, fail and release on the cancelled task (the cancelled branches at
    `services/completion.py:136,222`);
  - reclaim of cancelled work (`decide` -> CANCELLED);
  - `sweep_cancelled` of a straggler (inserted PENDING after the cancel, the way `test_cancel.py`
    does it);
  - `assert_counts_match` throughout, and `cancelled + succeeded + failed == total` at the end.
- Two concurrent `cancel` calls plus the sweep on a 25,000-subtask task: both batch loops bump the
  same shards. Expect no deadlock (40P01), `cancelled == 25,000`, and the aggregate matching.
- Add `assert_counts_match` at the end of the existing `test_cancel.py` scenarios that end
  CANCELLED via fail, release or reclaim (`:283-327`, `:341-382`), and of
  `test_protocol_races.py::test_cancel_concurrent_with_last_completions` (`:470-500`).

### Step 5: scale verifier + docs (implementer for code, docs-keeper for docs)

- `packages/minion-scale/src/minion_scale/verify.py:94-113` `_COUNTERS`: add
  `sum(cancelled)` vs `count(*) FILTER (WHERE status='CANCELLED')`. Update the
  `test_seed_verify.py` fixture if it seeds CANCELLED rows by SQL.
- `seed.py:267-288` `sum_remaining`: unchanged. It runs only for live tasks, and cancelled tasks are
  not live.
- `docs/SPEC.md`:
  - §7 item 8: add `cancelled`.
  - S-12 (`:531`): `GET /tasks` reads counters and the acquirement count; drop "tracked in the backlog".
  - §6 `TaskOut`: one sentence on how pending is derived, and pending = 0 while SPLITTING.
- `docs/ARCHITECTURE.md`:
  - §4 lock order (`:586-597`): shards are the last locks, sorted; cancel txn 2 and the sweep bump shards.
  - §5.1 `minion_task_counter` (`:611`): add `cancelled` and migration 0013.
  - cancel flow (`:579`).
- `docs/adr/0015-sharded-task-counters.md`: an "Addendum (TASK-16)" covering:
  - the cancelled column;
  - running from acquirements, not a counter (with the fleet bound);
  - pending derived;
  - one-statement read;
  - the rejected alternative A.
  This is not a paper deviation beyond ADR-0015, so no new ADR is needed.
- CLAUDE.md: no command changes. Mention `0013` only if a migration list exists (none does).

## 5. Risks and open questions

1. **The running bound is not strictly O(1)** (above). Recommendation: accept it, because it is
   bounded by executors x the cap and needs no hot-path writes. The orchestrator decides; if strict
   O(1) is required, switch to alternative A and add a `minion-scale` A/B run.
2. **Migration 0013 needs downtime.** It blocks writes to `minion_subtask` while it scans (SHARE).
   At 1M+ subtasks that is seconds to tens of seconds. It fails fast after 5 s if servers still run.
   Same procedure as 0011; state it in the docstring and the PR.
3. **Rolling deploy is unsafe.** An old server that keeps cancelling after 0013 would not bump
   `cancelled`, and pending would then be inflated. Same constraint as 0011: stop everything,
   migrate, start the new image.
4. **Raw-SQL status edits in tests** bypass the counters. Flag them, fix them with a helper.
5. **Planner risk for the lateral:** if PG evaluates `count_columns` before the outer sort, it runs
   at most teams x (limit+1) times. That is still bounded and still O(1) in subtasks. The EXPLAIN
   test asserts only the absence of `minion_subtask`.
6. Not done (follow-ups, if wanted): a `cancelled_subtasks` column on `minion_ro.task` for Grafana;
   an admin `verify-counters` command (the scale verifier and the test helper cover it).

## 6. Evidence for the verifier

- AC#1: the statement-count test (1 statement per list and per detail), and the EXPLAIN-JSON test
  (no `minion_subtask` relation) at N = 20,000.
- Optional live check: `minion-scale seed` 100 tasks x 10k subtasks, then `GET /tasks?limit=100`
  latency before and after, plus `EXPLAIN ANALYZE` of the logged statement.
- AC#2: `test_task_counters.py` (number of mid-storm checks logged), the migration backfill test,
  and `minion-scale verify` `task_counters_match_subtasks` PASS after a chaos drain.

## 7. Size

5 steps, each at most about 250 changed lines, in one PR. No sub-tasks are needed.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
AC (list/detail without scanning subtasks) evidenced by test_counts_plans_never_touch_subtasks (EXPLAIN, 20k subtasks) and test_list_tasks_counts_one_statement. Bound accepted: running = acquirement rows <= executors x MINION_MAX_OPEN_ACQUIREMENTS (ADR-0015 addendum).
<!-- SECTION:NOTES:END -->
