---
id: TASK-54
title: Acquire candidate plan becomes O(live tasks) above ~1k live tasks
status: In Progress
assignee: []
created_date: '2026-10-20 03:30'
updated_date: '2026-10-20 04:53'
labels:
  - perf
  - scale
dependencies: []
ordinal: 95000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
From TASK-30 review: on main (no extra index) the planner switches from the ordered walk on ix_minion_task_live_queue to 'bitmap all live tasks + sort' above roughly 1k live tasks, literal and generic plans: ~1 ms at 1k, 2-3 ms at 2-3k, 5 ms at 5k, 10-19 ms at 10k; plan choice is data-dependent (20k vs 40k historical tasks flips the 10k same-priority case). With the default cap (500/team) 20 teams = 10k live tasks. Scripts: scratchpad rev30/exp*.py. Investigate statistics/plan stability (e.g. ORDER BY/LIMIT shape, extended statistics, plan hints via query shape), measure on the scale harness.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Candidate query stays on the ordered index walk (or bounded cost) at 10k live tasks, literal and generic plans
- [ ] #2 Scale-harness measurement attached
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
# TASK-54 (+ TASK-29) plan: planner-proof ordered walk for the acquire candidate query

Worktree `/Users/robbertvanhintum/Documents/claude_code/open-minions-task54`, branch `perf/task-54-acquire-plan`.
All numbers below are from scratch testcontainer-style experiments on postgres:16-alpine (scripts in
`/private/tmp/claude-501/-Users-robbertvanhintum-Documents-claude-code-open-minions/e90acccf-30eb-47dd-abc3-b29dffdff04d/scratchpad/t54/`, the machine had load 5-14 from other jobs, so timings are noisy; buffers and probe
counts are stable and are what the tests should assert).

## 1. Goal and acceptance criteria

| AC | Restated | SPEC / ADR |
|---|---|---|
| TASK-54 #1 | At 10k live tasks the candidate query must not fall back to "read every live task, then sort". It must walk the live tasks in FR-22 order and stop once the window is full. Both the literal (custom) plan and the asyncpg generic plan count. | FR-22 (order), FR-9 / ADR-0020 (cap makes 20 teams x 500 = 10k live realistic), NF-1 (acquire p99), ADR-0013 (window) |
| TASK-54 #2 | A scale-harness measurement attached (docs/bench). | NF-1 evidence convention |
| TASK-29 #1 | Show with EXPLAIN / pg_stat_user_indexes that no query needs `ix_minion_task_queue`, or keep it and record why. | -- |
| TASK-29 #2 | If it is dropped: migration up/down tested, candidate plan tests green at 2k. | -- |

Must stay true: FR-22 order and ADR-0013 window semantics (the top `window` rows in `priority DESC, created_at,
team, project, task_name, subtask_id` order, then a random pick among the highest priority in that window),
preference pass 1 / global pass 2 unchanged, invariant 2 (the acquirement INSERT ... ON CONFLICT is the only
arbiter; the candidate read stays unlocked).

## 2. Findings (what is wrong today)

Touchpoints: `packages/minion-server/src/minion_server/repositories/acquisition.py:76-180` (`candidates_stmt`),
`:331-448` (`claim`, which embeds `candidates_stmt(...).cte("cand")` at `:353-361`),
`packages/minion-server/src/minion_server/db/models.py:83-110` (minion_task indexes),
`migrations/versions/0009_task_live_queue_index.py`, `0014_task_team_live_index.py`.

1. **Literal LIMITs alone do not fix it.** The generic plan has `LIMIT $3` / `LIMIT $4`. PostgreSQL then assumes
   10 % of the rows will be fetched, which pushes it to bitmap + sort. But making the LIMITs literals gave
   *identical* plans in the literal kind too. Literal plans also switch to "Bitmap Heap Scan on
   ix_minion_task_queue + Sort of all live tasks" from about 1k live tasks (e1.out, 120 cases): 1k = 61 buf / 0.9 ms,
   2k = 87 / 1.9 ms, 5k = 168 / 4.6 ms, 10k = 309 / 10-13 ms. Which plan wins depends on the data (20k vs 40k
   history flips the 10k cases), as the TASK-30 reviewer saw. The cause is the cost model. The ordered walk needs an
   Incremental Sort over a nested loop with a per-task lateral, and the planner prices that above one bitmap pass
   plus a full sort. Statistics targets and extended statistics do not change this trade-off. **Rejected.**
2. **Per-statement GUCs** (`SET LOCAL enable_bitmapscan/enable_sort = off`): these leak into every statement of
   the claim transaction. A cached generic plan keeps whatever GUCs were set when it was planned (plancache does not
   key on them). The behaviour also changes between PG versions (PG 18 counts disabled nodes). **Rejected.**
3. **Row-compare cursor on the existing index is not possible.** The index is `(priority DESC, created_at, ...)`.
   `ROW(priority, created_at, ...) < ROW(...)` is accepted as an Index Cond, but it has SQL lexicographic semantics
   (created_at would go DESC as well), so the result is wrong. Splitting it into "same priority, later" UNION
   "lower priority" either relies on Append order or makes every step evaluate the second branch. Over a junk tail
   the second branch is O(lower-priority tasks) per step.
4. **Plan instability in the probes themselves (found while prototyping):**
   - A pending check written as `EXISTS` is flattened into a semi-join. The planner then drives the walk from
     `minion_subtask` (31k-86k buffers at 200-2k live).
   - An unordered `(SELECT ... LIMIT 1)` falls into the LIMIT-1 seq-scan trap (2000 empty tasks: 38k buffers,
     96 ms).
   - What works: `ORDER BY subtask_id LIMIT k` on every probe. A seq scan would then need a full sort, so it is
     never chosen.

## 3. Design (measured): recursive ordered walk, one index step per contributing task

The live-task walk is written as a `WITH RECURSIVE` walk. That way no plan can visit all live tasks before
choosing. Each recursion step is a `LIMIT 1` index range scan that starts at a cursor. Uniform-direction cursor:
the live-queue index becomes `((-priority), created_at, team, project, task_name) WHERE status IN
('PENDING','RUNNING')`. The order is the same as today (`-priority ASC` == `priority DESC`, priority is NOT
NULL, API range -1000..1000). Then `ROW(-priority, created_at, team, project, task_name) > ROW(cursor)` is a
single Index Cond (verified).

```sql
WITH RECURSIVE walk(nprio, created_at, team, project, task_name, model_id, output_uri, params, priority, got) AS (
  SELECT -2147483647, '-infinity'::timestamptz, ''::varchar, ''::varchar, ''::varchar,
         NULL::varchar, NULL::text, NULL::jsonb, NULL::integer, 0::bigint          -- sentinel: one plan shape for every step
  UNION ALL
  SELECT nx.* FROM walk w CROSS JOIN LATERAL (
    SELECT -t.priority, t.created_at, t.team, t.project, t.task_name, t.model_id, t.output_uri, t.params, t.priority,
           w.got + (SELECT count(*) FROM (SELECT 1 FROM minion_subtask s WHERE <s = t task key> AND s.status = 'PENDING'
                                          ORDER BY s.subtask_id LIMIT W) c)
    FROM minion_task t
    WHERE t.status IN ('PENDING','RUNNING') AND <model/engine/memory/pool/preference filters as today>
      AND (SELECT s.subtask_id FROM minion_subtask s WHERE <key> AND s.status = 'PENDING'
           ORDER BY s.subtask_id LIMIT 1) IS NOT NULL                                 -- skip drained tasks inside the scan
      AND ((-t.priority), t.created_at, t.team, t.project, t.task_name) > (w.nprio, w.created_at, w.team, w.project, w.task_name)
    ORDER BY (-t.priority), t.created_at, t.team, t.project, t.task_name LIMIT 1) nx
  WHERE w.got < W)                                                                     -- stop once the window can be filled
SELECT w.team, ..., s.subtask_id, ... FROM walk w
  CROSS JOIN LATERAL (SELECT ... FROM minion_subtask s WHERE <key> AND status = 'PENDING' ORDER BY subtask_id LIMIT W) s
WHERE w.model_id IS NOT NULL                                                           -- drop the sentinel
ORDER BY w.nprio, w.created_at, w.team, w.project, w.task_name, s.subtask_id LIMIT W
```

Why the window is the same: `got` sums min(W, pending) over the visited tasks in index order. Recursion stops once
`got >= W`, or when no eligible task with pending work is left. The rows of the visited tasks therefore always
contain the first W rows of the old single-join order. The task key is unique, so the order is total. One statement
means one snapshot for the walk, the count and the lateral. `claim` (`:353-445`) keeps `.cte("cand")`, the
max-priority pick, the INSERT ... ON CONFLICT, the post-win re-check and the key cross-check unchanged.
SQLAlchemy hoists the recursive CTE into the top-level `WITH RECURSIVE` (verified).

### Measurements (prototype `/private/tmp/claude-501/-Users-robbertvanhintum-Documents-claude-code-open-minions/e90acccf-30eb-47dd-abc3-b29dffdff04d/scratchpad/t54/proto.py` = the SQLAlchemy Core form; final index set = new live_queue + ix_minion_task_queue + team_status + team_created + pk)

| Case (20k/40k history, 5 pending per task unless noted) | main: buf / ms | walk: buf / probes / ms (lit = gen) |
|---|---|---|
| 12 live | 7 / 0.1 | 20 / 12 / 0.15 |
| 200 - 2k live, mixed or same priority, ten models | 18-90 / 0.06-2.3 | 80-98 / 12 / 0.15-0.2 |
| 5k live | 152-171 / 2.6-6.5 | 80-98 / 12 / 0.15-0.2 |
| **10k live (all scenarios with work)** | 288-310 / 4.8-13 (gen everywhere; lit for h40000) | **48-98 / 12 / 0.12-0.5** |
| 10k live, all drained (inherent O(live): each task needs one probe) | 274-654 / 7-15 | 654 / 10000 / 4-16 (noisy) |
| 10k live, team preference | 199-221 / 0.5-0.6 | 80-96 / 12 / 0.2-0.3 |
| window 256, 1 pending per task, 10k | 780-1045 / 0.6-14 | 3074 / 768 / 2-5 |
| TASK-30 5k unrunnable (existing test) | 151 / 0.7 | 161 / 3 / 0.7 (bound 230 still holds) |
| seed_big 12 x 8000 pending (existing) | 11 | 16 / 3 |
| 2000 empty live tasks ahead (existing) | 4066 / 2002 probes | 4071 / 2003 probes |
| stale stats: ANALYZE at 0/1/12 live, then +10k live, no re-analyze | -- | walk kept, 48-192 buf |
| full `claim` statement, 10k same-priority live, after 7 executions | -- | custom 137 buf / 0.8 ms, generic 113 / 0.45 ms, walk in both |

- Oracle equality: the prototype equals `old_candidates_stmt` for 60 random seeds x 7 preferences x limits
  {1, 16, 256} (2001 rows checked), and in all 220 grid cases.
- Plan shape: ordered walk (Index Scan on the live-queue index with ROW Index Cond) in every case with >= 200 live
  tasks. Two generic plans at 12 live used a per-step bitmap on the tiny set (<= 34 buffers). `ix_minion_task_queue`
  was never chosen by the walk.

## 4. TASK-29 decision: keep `ix_minion_task_queue` (no migration)

Users found by grep and EXPLAIN (10k live + 40k history, scratch `other.py`):

| Query | with the index | without it |
|---|---|---|
| splitter claim `repositories/tasks.py:213-224` (status = SPLITTING, polled by every server worker) | Index Scan, **6 buf** (lit + gen) | falls back to a full `ix_minion_task_team_status` scan, 59 buf, grows with history |
| watchdog cancel sweep `repositories/tasks.py:450-460` (status = CANCELLED, finished_at window) | lit 1392 buf / 0.9 ms | team_status scan, about the same |
| settleable `repositories/completion.py:368-395` | lit uses it (275 buf); the new live-queue index serves it equally | -- |
| Grafana live panel, `minion_ro.task` | Seq Scan either way (TASK-34 "JUSTIFIED") | same |
| acquire candidate walk | never chosen after this change | -- |

Decision: keep the index. It is the only status-leading index, and it keeps the splitter poll O(SPLITTING) instead of
O(all tasks). The original TASK-29 worry (the bitmap on it displacing the walk) goes away with the recursive walk.
Record the reason in a comment at `models.py:85`, in ARCHITECTURE §3.2 and in the TASK-29 notes. Evidence:
EXPLAIN tests (step 3) and `pg_stat_user_indexes` deltas from the scale run (step 4/5).

## 5. Steps

### Step 1: migration 0015 + model (about 80 lines)
- New `migrations/versions/0015_live_queue_expression_index.py` (revises 0014). Upgrade:
  `SET LOCAL lock_timeout = '5s'`, `DROP INDEX ix_minion_task_live_queue`,
  `CREATE INDEX ix_minion_task_live_queue ON minion_task ((-priority), created_at, team, project, task_name) WHERE
  status IN ('PENDING','RUNNING')`. Both run in one transaction, so there is no window without the index. It is not
  CONCURRENTLY: minion_task is small, the same reasoning as 0009/0014. Downgrade: same lock_timeout, then recreate
  the 0009 definition (`priority DESC`). Keep the **name** (tests, the scale explain and the docs reference it).
- `models.py:87-95`: `Index("ix_minion_task_live_queue", text("(-priority)"), "created_at", "team", "project",
  "task_name", postgresql_where=...)`, with a comment saying why it uses an expression (uniform-direction row-compare
  cursor). `models.py:85` gets a comment on why ix_minion_task_queue stays (TASK-29).
- Tests (integration): `test_migrations.py`: `test_0015_live_queue_expression_round_trip`. It checks that
  `pg_indexes.indexdef` contains `(- priority)` at head and `priority DESC` after downgrade to 0014, then upgrades
  again. `test_migrated_schema_matches_models` must stay `[]` (checked in scratch: alembic `compare_metadata` reports
  no diff for the expression index).
- Invariants: none touched. Risk: `-priority` overflows only for priority = INT_MIN. The API allows -1000..1000.
  A direct-SQL row like that would make the CREATE INDEX or its INSERT fail loudly, never silently. No CHECK
  constraint is added (that would be a separate schema decision).

### Step 2: `candidates_stmt` as the recursive walk (about 200 lines incl. docstring)
- `acquisition.py:76-180`: keep the signature `candidates_stmt(*, engines, model_ids, memory_gb, pool, preference,
  limit=CANDIDATE_LIMIT, with_priority=False) -> Select[Any]` and the output columns and their order (11, plus
  priority). Validate `1 <= limit <= MAX_WINDOW` (ValueError). Render every LIMIT (`W`, the count's `W`, the step's
  `1`) and `got < W` as `literal_column(str(int(limit)))`, like `status_literal`: stable plans and no 10 % heuristic.
  Render the sentinel as `literal_column` constants with explicit casts. The SQLAlchemy Core skeleton is in
  `/private/tmp/claude-501/-Users-robbertvanhintum-Documents-claude-code-open-minions/e90acccf-30eb-47dd-abc3-b29dffdff04d/scratchpad/t54/proto.py` and passed the oracle. **Note:** the count subquery needs `.correlate(Task)`, otherwise
  it silently becomes a cartesian product (this was hit while prototyping). Use `select(...).cte("walk",
  recursive=True)` + `.union_all(...)`, `tuple_(-Task.priority, ...) > tuple_(walk.c....)`, a `.lateral("nx")`
  step, the probes via a helper `_pending_probe(key_cols, k, *cols)` with `ORDER BY subtask_id LIMIT k`, and
  `.scalar_subquery().is_not(None)` for has-pending.
- Rewrite the docstring: the walk, the termination proof, why every probe has an ORDER BY (seq-scan / semi-join
  traps), and the cost: O(contributing tasks x 3 probes + skipped live tasks). Filtered tasks cost a heap filter
  only; drained tasks cost one probe each.
- `candidates()` (`:183-223`) and `claim()` (`:331-448`) stay as they are. Ownership, the post-win re-check and the
  key cross-check are unchanged (invariant 2, ADR-0013 D3).
- Tests (integration, `test_candidate_index_path.py`):
  - `test_new_statement_matches_old_order` (`:145`): add limit 256. This is the FR-22 oracle with ties and
    preferences.
  - Update `assert_index_paths` (`:263-275`). Require `Recursive Union` and an Index Scan on
    `ix_minion_task_live_queue` whose Index Cond contains `ROW(`. No Seq Scan on minion_task or minion_subtask. Drop
    the "Sort above a subtask scan" rule (the final bounded Sort is now above the per-task lateral). Instead, every
    Sort's actual rows must be <= max(W, pending per task in the seed).
  - Re-run the existing bounds: 5k unrunnable (`:424`, keep 230 buffers / 12 probes; update the comment at
    `:380-385`, there is no bitmap on ix_minion_task_queue any more), empty-live-tasks (`:348`), queue-length
    growth (`:342`), guard (`:493`), oracle + claim wins (`:436`).
  - `test_acquire_claim*.py`, `test_acquire*.py`, `test_protocol_races.py` (concurrent acquire race) must be green
    unchanged.
- Invariants touched: 2 (the read stays unlocked, the arbiter is unchanged). FR-22 / ADR-0013 semantics are proven
  by the oracle test.

### Step 3: plan-shape scale tests, both plan kinds (about 250 lines, new `packages/minion-server/tests/test_candidate_walk_scale.py`, `integration`)
- One module-scoped migrated DB. Each case TRUNCATEs and reseeds with `generate_series`, using the seed template
  from `/private/tmp/claude-501/-Users-robbertvanhintum-Documents-claude-code-open-minions/e90acccf-30eb-47dd-abc3-b29dffdff04d/scratchpad/t54/seedtpl.sql` (history SUCCEEDED tasks + n live tasks over 20 teams + `per` PENDING
  subtasks), then ANALYZE. Helpers: literal EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) and generic (PREPARE +
  `SET plan_cache_mode = force_generic_plan`). Reuse or extend `_literal_sql` / `_prepared` with `limit`.
- Parametrize n in {12, 200, 1000, 2000, 5000, 10000} x kind {literal, generic} x scenario {mixed priority
  `g % 3`, same priority, ten models}. History is 20k (plus one 40k case at 10k). Assert rows == oracle. For
  n >= 1000 also assert walk shape (above) and no Bitmap Heap Scan on minion_task. For all n assert buffers <= 150
  (measured 20-98) and subtask probes <= 15 (measured 12).
- Extra cases at 10k:
  - all drained: probes == n (one per live task, inherent) and buffers <= n/10 + 50 (measured 654).
  - team preference: buffers <= 150.
  - window 256 with 1 pending per task: probes <= 3 x 256, buffers <= 3500.
  - stale stats: ANALYZE with 0 and with 1 live task and `autovacuum_enabled=false` on both tables, insert 10k live
    without ANALYZE. The walk must be kept and buffers <= 250.
  - full claim: the 10k same-priority claim wins and after 7 executions the generic plan still walks. This can be
    checked by capturing the statement via a `before_cursor_execute` hook, as in `/private/tmp/claude-501/-Users-robbertvanhintum-Documents-claude-code-open-minions/e90acccf-30eb-47dd-abc3-b29dffdff04d/scratchpad/t54/claim_check.py`,
    and EXPLAINing it in a rolled-back transaction.
- TASK-29 evidence tests (same module): at 10k live + 40k history, the splitter claim select uses
  `ix_minion_task_queue` (lit + gen, buffers <= 20), and no candidate plan names `ix_minion_task_queue`.
- Runtime: about 30 cases x (seed <= 1 s + 6 EXPLAINs). Keep it under about 90 s. Use timings only as a loose
  `< 50 ms` check. Buffers and probes are the real assertions.

### Step 4: scale harness additions (about 150 lines)
- `packages/minion-scale/src/minion_scale/explain.py:62-125`: add a `plan_kind: "generic"` row for `candidate` and
  `candidate_preference` (PREPARE with the asyncpg-compiled SQL, `SET LOCAL plan_cache_mode =
  force_generic_plan`, EXPLAIN EXECUTE, DEALLOCATE, still in the READ ONLY rolled-back transaction).
- `dbstats.py:41-75`: add a `pg_stat_user_indexes` reading (indexrelname, idx_scan, idx_tup_read for the minion
  tables) to `snapshot` and its before/after delta, so the report shows which indexes the run used (TASK-29).
- `docs/bench/scale-report-schema.md`: document the new fields. Add unit tests in the existing minion-scale tests
  (plan-kind rows, delta shape).

### Step 5: verifier scale run + docs (no code)
- Scale stack only: `deploy/compose.scale.yml`, project `open-minions-scale`, loopback ports (for example
  MINION_PG_PORT=55432, MINION_SERVER_HOST_PORT=58080), **never 8080**.
- Scenario: seed 20 teams (`--team t01..t20`) x 500 tasks x 5 subtasks = 10k live / 50k PENDING, with tokens for
  100 executors. First drain one such round to create 10k SUCCEEDED history, then seed again.
- Measure:
  - `minion-scale explain` (custom + generic) at 10k live.
  - `run` with 100 sims, short work (for example 0.1 s) until drained.
  - Report the acquire p50/p99, pg_stat_statements mean/max of the claim statement, and the pg_stat_user_indexes
    deltas (`ix_minion_task_live_queue` used by acquire, `ix_minion_task_queue` used by splitter polls).
- Run the same scenario on `main` as the baseline, on the same machine and in the same session. Expect a claim mean
  of about 10-15 ms on main and about 1 ms or less on the branch.
- Write `docs/bench/m-task54-acquire-walk-apple-m3-pro.{md,json}`.
- Docs (docs-keeper):
  - **ADR-0021** "Acquire candidate query as a recursive ordered walk". Not a paper deviation, but it records the
    index change and the rejected options from section 2.
  - ARCHITECTURE §3.2 notes (`docs/ARCHITECTURE.md:386-402`: index definition, cost model, the 5k unrunnable
    paragraph no longer bitmaps).
  - ADR-0020 addendum: the "Bound" paragraph (`:40`) and the "Index change" bullet (`:61`). The order index changed
    form; the filters are still heap Filters.
  - The TASK-29 decision in its notes.
- CLAUDE.md commands are unchanged.

## 6. Risks / open questions
- **Per-task probe can sort a large task's whole PENDING set (pre-existing, not caused by this change).** When the
  per-task estimate is about 1 row, the planner probes via `ix_minion_subtask_progress` + Sort instead of the ordered
  `ix_minion_subtask_pending`. Measured with one 20k-subtask task ahead of 10k small ones: main 606 buf / 16.6 ms,
  walk 978 buf / 7.8 ms (3 probes on that task). At 200k pending the estimate flips back to the pending index
  (15 buf / 0.15 ms). In scratch, extending the progress index to `(team, project, task_name, status, subtask_id)`
  removed it (15 buf / 0.15 ms at 20k). That needs a CONCURRENTLY migration on the big table, so it is proposed as a
  sub-task below, not folded in here.
- Drained live tasks cost one probe each (10k drained: 654 buf, 4-16 ms). This is inherent and the same as today.
  The bound is teams x cap.
- Window 256 with tiny tasks: up to 256 recursion steps (about 3k buffers, 2-5 ms). This is still within NF-1.
- Generic plans at < 200 live may bitmap per step on the tiny set. This is bounded (<= 34 buffers measured); tests
  assert shape only from 1000 up.
- Planning time of the recursive statement is about 0.8 ms (custom). asyncpg caches the generic plan, so it is paid
  once per connection.
- Not run: PG 17/18 (compose pins 16). The design does not rely on version-specific planner behaviour, but the
  scale tests will tell if the pin moves.

## 7. Proposed sub-tasks / follow-ups (not created; the orchestrator decides)
```
backlog task create "Acquire per-task pending probe: no plan may sort a large task's PENDING set" -p TASK-54 \
  -d "ix_minion_subtask_progress + Sort chosen when the per-task estimate is ~1 row; one 20k-subtask task ahead of 10k small ones: 606 buf/16.6 ms (main), 978/7.8 ms (walk). Scratch fix: progress index -> (team, project, task_name, status, subtask_id) via CREATE/DROP INDEX CONCURRENTLY (15 buf/0.15 ms). Measure write cost and the other users of the progress index." \
  --ac "Probe plans use an ordered index without Sort for a skewed task mix, literal and generic" --ac "Migration up/down tested (CONCURRENTLY, lock_timeout)" -l perf,db
backlog task create "Watchdog settle + cancel sweep: status literals so generic plans keep index paths" \
  -d "settleable_stmt (completion.py:368) binds status IN params: generic plan Seq Scan on minion_task, 994 buf/7.4 ms vs 275/1.0 literal at 10k live + 40k history. cancel_stuck_pending 'recent' subquery (tasks.py:450): generic pk scan 72k buf/21 ms vs 1.4k/0.9 ms. Use status_literal like acquisition." \
  --ac "Generic and literal plans equal for both statements (EXPLAIN test)" -l perf
```

## 8. Summary for the orchestrator
- TASK-54: replace the candidate query with a `WITH RECURSIVE` ordered walk on a uniform-direction expression index
  (`(-priority), created_at, team, project, task_name`, same name, migration 0015). The prototype matches the FR-22
  oracle. At 10k live it uses 48-98 buffers / about 0.2 ms in both plan kinds (main: 288-310 / 5-13 ms) and keeps
  the walk under stale statistics.
- TASK-29: keep `ix_minion_task_queue`. The splitter claim (6 vs 59+ buffers, O(SPLITTING)) and the cancel sweep
  need it, and the walk no longer picks it.
- 5 steps (migration, query, scale plan tests, harness generic EXPLAIN + index stats, verifier scale run + ADR-0021
  and docs). Two follow-ups proposed (skewed per-task probe; watchdog generic plans).
<!-- SECTION:PLAN:END -->
