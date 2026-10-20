---
id: TASK-34
title: Bound watchdog settleable_tasks scan and minion_ro.task lateral aggregate cost
status: In Progress
assignee: []
created_date: '2026-10-18 10:22'
updated_date: '2026-10-20 01:06'
labels:
  - perf
dependencies: []
ordinal: 73000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Follow-up from TASK-7.4.8 review/security (LOW/NIT). settleable_tasks sums shards of every live task each tick (LIMIT after HAVING); minion_ro.task LEFT JOIN LATERAL aggregate runs per task row even for count-by-status Grafana panels. EXPLAIN both at scale (add to minion-scale explain set) and bound if they show up.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 EXPLAIN of settleable_tasks and Grafana status panel in minion-scale explain set
- [x] #2 Bounded or justified by measurement
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
# TASK-34 plan: bound settleable_tasks scan and minion_ro.task lateral aggregate cost

Worktree `/Users/robbertvanhintum/Documents/claude_code/open-minions-task34`, branch `perf/task-34-settle-scan-bound`.

## Goal / AC mapping
- AC#1: `minion-scale explain` also EXPLAINs (a) the watchdog backstop `settleable_tasks` (ADR-0015) and
  (b) the Grafana task status panels on `minion_ro.task` (count-by-status, live-tasks table).
  Maps to NF-5 (stateless watchdog/server, all state in PG), ADR-0015 "Backstop" (settle within one watchdog
  interval), FR Grafana observability (ARCHITECTURE "Grafana reading PostgreSQL").
- AC#2: a bench doc with numbers at 1M subtasks and a decision per query: "justified" (plan bounded by
  live tasks / panel cheap) or "bounded" (code/migration change + before/after numbers).

## Touchpoints
- `packages/minion-server/src/minion_server/repositories/completion.py:318-337` `settleable_tasks`: JOIN
  task x counter, WHERE status IN (PENDING,RUNNING) AND total>0, GROUP BY, HAVING sum>=total, LIMIT.
  LIMIT applies after HAVING, so in steady state (0 settleable tasks) it reads all live tasks x 16 shards.
- `packages/minion-server/src/minion_server/services/reclaim.py:315-343` `settle_stuck(batch=100, max_rounds=50)`;
  called once per tick from `packages/minion-watchdog/src/minion_watchdog/main.py:64-68` (interval 60 s default,
  `settings.py:22`).
- `migrations/versions/0011_task_counter_shards.py:25-39` `minion_ro.task` = `minion_task LEFT JOIN LATERAL
  (sum over minion_task_counter) ON true`. Applied: never edit; any change is a new `0013`.
- Grafana SQL: `deploy/grafana/dashboards/tasks.json:42` `SELECT status, count(*) AS tasks FROM minion_ro.task
  GROUP BY status ORDER BY status` (refresh 30 s, line 11); `tasks.json:350` live-tasks table
  (`WHERE status IN ('SPLITTING','PENDING','RUNNING') ORDER BY created_at DESC LIMIT 50`, reads the counters).
  Also `SELECT status, count(*) AS subtasks FROM minion_ro.subtask GROUP BY status ORDER BY status`
  (1M-row scan per refresh; same panel row, cheap to include).
- Indexes on minion_task: PK (team,project,task_name); `ix_minion_task_queue (status, priority DESC, created_at)`
  (0001:128); partial `ix_minion_task_live_queue` WHERE status IN (PENDING,RUNNING) (0009); counter PK
  (team,project,task_name,shard) (0011).
- `packages/minion-scale/src/minion_scale/explain.py:43-64` `hot_statements`, `:84-102` `explain_all`
  (compiles Select with literal binds, READ ONLY tx, 60 s timeout); CLI `cli.py:391-422` (runs `ANALYZE` first).
- Tests: `packages/minion-scale/tests/test_report_and_deploy.py:10` (unit, imports hot_statements),
  `packages/minion-scale/tests/test_smoke.py:146-151` (integration, exact explain name set).
- Docs: `docs/bench/scale-report-schema.md` section `explain` (lists the four names).

Open question to settle by measurement (not assumption): PostgreSQL may already remove the LEFT JOIN LATERAL for
count-by-status (the subquery is an aggregate without GROUP BY, so at most one row, and no column of it is
referenced); a lateral reference may block that removal. The review note says it runs per row; the EXPLAIN decides.

## Step 1: explain set (code, about 120 lines)
Files:
- `repositories/completion.py`: extract the builder, no behaviour change:
  ```python
  def settleable_stmt(limit: int) -> Select[tuple[str, str, str]]: ...  # exact current select
  async def settleable_tasks(session, limit) -> list[TaskKey]:
      rows = (await session.execute(settleable_stmt(limit))).all()
  ```
- `minion_scale/explain.py`:
  ```python
  Statement = Select[Any] | str
  GRAFANA_STATEMENTS: dict[str, str] = {
      "grafana_task_status": "SELECT status, count(*) AS tasks FROM minion_ro.task GROUP BY status ORDER BY status",
      "grafana_live_tasks": "SELECT team, project, task_name, status, succeeded_subtasks, failed_subtasks, "
          "total_subtasks, updated_at FROM minion_ro.task WHERE status IN ('SPLITTING', 'PENDING', 'RUNNING') "
          "ORDER BY created_at DESC LIMIT 50",
      "grafana_subtask_status": "SELECT status, count(*) AS subtasks FROM minion_ro.subtask GROUP BY status ORDER BY status",
  }
  def hot_statements(..., settle_batch: int = 100) -> dict[str, Statement]:
      ... existing four ...,
      "settleable_tasks": settleable_stmt(settle_batch),   # 100 = settle_stuck default batch
      **GRAFANA_STATEMENTS
  async def explain_all(sm, statements: dict[str, Statement]) -> ...:
      sql = stmt if isinstance(stmt, str) else compile_literal(stmt)
  ```
  The settleable SQL with literals (what the report will carry):
  `SELECT minion_task.team, minion_task.project, minion_task.task_name FROM minion_task JOIN minion_task_counter
  ON <3-col equality> WHERE minion_task.status IN ('PENDING', 'RUNNING') AND minion_task.total_subtasks > 0
  GROUP BY minion_task.team, minion_task.project, minion_task.task_name, minion_task.total_subtasks
  HAVING sum(minion_task_counter.succeeded + minion_task_counter.failed) >= minion_task.total_subtasks LIMIT 100`.
  Grafana SQL has no parameters; it is run verbatim (as the minion role; view plans are the same as for
  minion_readonly). Realistic params: limit = 100 (`settle_stuck` default); Grafana = the panel text.
- `cli.py` `explain_cmd`: add `--settle-batch` (int, min 1, default 100); docstring lists the new names.
- `docs/bench/scale-report-schema.md` `explain`: list `settleable_tasks`, `grafana_task_status`,
  `grafana_live_tasks`, `grafana_subtask_status`; note Grafana entries are panel SQL copied verbatim and that
  `seq_scan` is expected for the count panels (judge by whether `minion_task_counter` appears in the plan).
Tests:
- unit (`test_report_and_deploy.py`): new names present; `compile_literal(settleable_stmt(100))` contains
  `HAVING` and `LIMIT 100`; drift guard: every `GRAFANA_STATEMENTS` value, whitespace-normalized, equals some
  `rawSql` in `deploy/grafana/dashboards/tasks.json` (walk the JSON).
- integration: extend the name set in `test_smoke.py:146`; `execution_ms` not None for all.
- existing settle tests (`test_settle_unit.py`, `test_completion.py`, `test_reclaim_races.py`) must stay green.
Invariants: none touched (read-only tx, operator tooling; server change is a pure refactor in repositories/).

## Step 2: measurement (no code; bench doc + JSON)
Output: `docs/bench/m-task34-settle-scan-apple-m3-pro.md` + `.json` (merged explain reports + fixture params).
Stack (scale project only, loopback ports, never 8080; no server and no watchdog, so nothing mutates the fixture):
```bash
export MINION_PG_PORT=55432 MINION_S3_PORT=59000 MINION_SERVER_HOST_PORT=58080 MINION_GRAFANA_PORT=53000
DC="docker compose -p open-minions-scale -f deploy/compose.yml -f deploy/compose.scale.yml"
$DC --profile app up -d --build --wait postgres s3 s3-init migrate
export MINION_DATABASE_URL=postgresql+asyncpg://minion:minion@localhost:55432/minion
export MINION_S3_ENDPOINT_URL=http://localhost:59000 MINION_S3_ACCESS_KEY=minion MINION_S3_SECRET_KEY=minion-secret
uv run minion-scale seed --tasks $L --subtasks $((1000000 / L)) --executors 0   # no tokens issued
psql_f() { $DC exec -T postgres psql -U minion -d minion -v ON_ERROR_STOP=1 "$@"; }
```
Fixture SQL (scratch file outside the repo, quoted verbatim in the bench doc):
1. Live progress (all L live tasks, 30 % done, none settleable; half RUNNING):
   ```sql
   INSERT INTO minion_task_counter (team, project, task_name, shard, succeeded, failed)
   SELECT team, project, task_name, sh, (total_subtasks * 3 / 10) / 16, 0
   FROM minion_task CROSS JOIN generate_series(0, 15) sh WHERE task_name LIKE 'scale-%';
   UPDATE minion_task SET status = 'RUNNING' WHERE task_name LIKE 'scale-%' AND right(task_name, 5)::int % 2 = 0;
   ```
2. Five settleable live tasks (positive path, a lost settle):
   `UPDATE minion_task_counter k SET succeeded = CASE WHEN k.shard = 0 THEN t.total_subtasks ELSE 0 END
    FROM minion_task t WHERE (k.team, k.project, k.task_name) = (t.team, t.project, t.task_name)
    AND t.task_name IN ('scale-00001','scale-00002','scale-00003','scale-00004','scale-00005');`
3. History H (terminal tasks, 16 full shards each, no subtasks; added incrementally H0 -> H1):
   ```sql
   INSERT INTO minion_task (team, project, task_name, type, model_id, engine, model_memory_gb, input_uri,
       output_uri, records_per_subtask, max_attempts, allowed_pool, status, total_subtasks, created_by,
       created_at, finished_at)
   SELECT t.team, t.project, 'hist-' || lpad(g::text, 6, '0'), t.type, t.model_id, t.engine, t.model_memory_gb,
       t.input_uri, t.output_uri || 'hist-' || g || '/', t.records_per_subtask, t.max_attempts, t.allowed_pool,
       (ARRAY['SUCCEEDED','SUCCEEDED','SUCCEEDED','PARTIAL','FAILED','CANCELLED'])[1 + g % 6], 16, t.created_by,
       now() - g * interval '10 minutes', now() - g * interval '10 minutes' + interval '5 minutes'
   FROM minion_task t CROSS JOIN generate_series(:h0 + 1, :h1) g WHERE t.task_name = 'scale-00001';
   INSERT INTO minion_task_counter (team, project, task_name, shard, succeeded, failed)
   SELECT team, project, task_name, sh, 1, 0 FROM minion_task CROSS JOIN generate_series(0, 15) sh
   WHERE task_name LIKE 'hist-%' ON CONFLICT DO NOTHING;
   ```
   (created_by stays `minion-scale`, so `check_scale_db` accepts the DB; output_uri is unique per row, 0004.
   If a CHECK in 0001 rejects a status/finished_at combination, adjust the fixture, not the schema.)
Matrix: L in {100, 1,000 (realistic), 10,000 (stress)} live tasks sharing 1M PENDING subtasks (fresh stack
per L: `down -v`, up, seed); per L, H in {0, 10,000, 100,000} added incrementally. Per cell:
`uv run minion-scale explain --out e-L$L-H$H-r$i.json` 3 times (explain runs ANALYZE itself); report the first
(cold-ish) and the median warm `execution_ms`, buffers, and whether the plan touches `minion_task_counter`,
`Seq Scan on minion_task`/`minion_task_counter`. Rationale for "realistic": NF-1 scope is 1M PENDING with 500
executors; a few hundred to 1,000 concurrent tasks is the plausible live set; minion_task is never purged
(no retention in SPEC), so history grows: 100k terminal tasks is about 3 years at 100 tasks/day.
Optional cross-check of the generic (prepared) plan at L=10,000/H=100k:
`SELECT pg_stat_statements_reset();` then `MINION_WATCHDOG_INTERVAL_S=5 $DC --profile app up -d watchdog`, 60 s,
read `mean_exec_ms` of the settle statement from `pg_stat_statements`, `$DC stop watchdog` (it settles the
5 positives: do this last).
Teardown: no tokens issued (`--executors 0`); if any step issued tokens, `uv run minion-scale cleanup
--tokens-file $T` first, then `$DC down -v` (scale project only).

## Step 3: decision rule
- settleable_tasks is JUSTIFIED if all hold: (a) warm execution <= 20 ms at L=1,000 for every H and <= 200 ms
  at L=10,000 (budget: one statement per 60 s tick, well inside the 2.4 s TASK-35 tick target); (b) cost does
  not grow with history: at fixed L, H=100k <= 1.5x H=0 (plan reaches live tasks via `ix_minion_task_live_queue`
  or `ix_minion_task_queue` and counters via the PK, no `Seq Scan on minion_task_counter`); (c) the 5
  positives are returned. Else BOUND (3a).
- grafana_task_status is JUSTIFIED if the plan has no `minion_task_counter` node (join removed), or warm
  execution <= 50 ms at L=1,000/H=100k (30 s refresh, one dashboard viewer). Else BOUND (3b).
- grafana_live_tasks JUSTIFIED if <= 50 ms at L=10,000/H=100k (it should touch only live tasks x 16 shards).
- grafana_subtask_status: informational (1M-row scan per refresh); record the number, no change unless > 500 ms,
  then a separate follow-up task (out of scope).
Record the verdict per query in the bench doc and tick AC#2 with the doc link.

## Step 4 (conditional) candidate bounds
3a settleable_tasks (only if 3(a) or 3(b) fails):
- If (b) fails (history leaks in): planner picks a bad path. First fix = statistics/plan shape, e.g. drive from
  live tasks explicitly: `FROM (SELECT ... FROM minion_task WHERE status IN (...) AND total_subtasks > 0) t
  JOIN LATERAL (SELECT sum(succeeded+failed) s FROM minion_task_counter k WHERE <pk prefix = t>) c ON true
  WHERE c.s >= t.total_subtasks LIMIT :n` (same result set; per live task one PK range scan). No migration.
- If (a) fails with a good plan (cost is truly O(live x 16)): keyset rotation, a hard per-statement bound.
  `settleable_stmt(limit, scan, after: tuple | None)` scans the next `scan` (e.g. 1,000) live tasks in
  `ix_minion_task_live_queue` order (priority DESC, created_at, team, project, task_name) after `after`, then
  aggregates only those; `settle_stuck` keeps the cursor in process memory and wraps at the end (restart = start
  over; several watchdogs = harmless duplicates, `set_task_terminal` is conditional). Note "LIMIT before HAVING"
  without a cursor would miss settleable tasks beyond the first page, so the cursor is required.
  Deviation: ADR-0015 says healing happens within one watchdog interval; this makes it ceil(L/scan) intervals
  -> ADR-0015 addendum (docs-keeper) + ARCHITECTURE.md:611 note. Tests: unit (cursor wraps, short page resets),
  integration (a settleable task beyond the first page is settled within ceil(L/scan) ticks; existing
  `test_reclaim_races.py` settle tests).
  Rejected: a NOT EXISTS live-subtask pre-filter on `ix_minion_subtask_progress` (still O(live), adds a 1M-row
  table probe); a counter-shard "updated_at" pre-filter (needs a hot-path column write per completion, which
  ADR-0015 removed).
3b minion_ro.task (only if 3 says BOUND): new migration `migrations/versions/0013_task_view_scalar_counters.py`
  (revises 0012), `CREATE OR REPLACE VIEW minion_ro.task` with the same 19 columns, names, order and types, the
  counters as correlated scalar subqueries:
  `coalesce((SELECT sum(k.succeeded) FROM public.minion_task_counter k WHERE k.team = t.team AND
  k.project = t.project AND k.task_name = t.task_name), 0)::integer AS succeeded_subtasks` (same for failed),
  then the 0011 `_GRANT` DO block. The view is then a simple single-table view: PostgreSQL drops unreferenced
  target entries after pull-up, so count-by-status never reads counters; the LIMIT 50 panel evaluates them only
  for its output rows. Downgrade = 0011's `_VIEW_SHARDED` + grant. No dashboard change, no new view to allowlist.
  Tests: `test_migrations.py` (column types unchanged at `:239`, upgrade/downgrade round trip, counts equal to
  the 0011 view on a fixture), `test_dashboards.py` unchanged and green; re-run the Step 2 cells for before/after.
  Alternative (only if a consumer needs it): a narrow `minion_ro.task_status` view without counters + panel
  change + grant; more surface, not preferred.

## Risks / notes
- EXPLAIN uses custom plans (literals); settleable has only constants and LIMIT, so the generic plan should match;
  the optional pg_stat_statements cross-check covers it.
- Fixture is synthetic (counters via SQL, history without subtasks/attempts): `minion-scale verify` is not
  meaningful on it and must not be run as evidence. The two measured queries read only minion_task and
  minion_task_counter, which the fixture populates realistically (16 shards per task, as with >= 16 subtasks).
- Docker VM timings: report the host, Docker version and shared_buffers as the earlier bench docs do.
- No invariant from CLAUDE.md is touched. 3a changes a documented ADR-0015 latency (ADR addendum required).

Size: Step 1 one implementer pass; Step 2 a verifier/test-engineer bench pass; Step 4 at most one pass each for
3a / 3b. No sub-tasks needed.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Measured at 1M subtasks, L live tasks x H history (docs/bench/m-task34-settle-scan-apple-m3-pro.md).
- settleable_tasks: BOUNDED. The join + GROUP BY plan scanned minion_task by PK and filtered out history
  (13.6 ms at L=1,000/H=100k, 2.97x the H=0 cost). Rewritten as a lateral driven from the live tasks
  (ix_minion_task_live_queue + counter PK): 5.3 ms, 1.10x. Same result set, no migration, ADR-0015 semantics unchanged.
- grafana_task_status: JUSTIFIED, the planner removes the lateral join (no counter node), about 19 ms seq scan at 101k tasks.
- grafana_live_tasks: JUSTIFIED (4.2 ms at L=10,000/H=100k; 85-96 ms at H<=10k, noted in the doc).
- grafana_subtask_status: informational, 41-66 ms.
Caveat: a CI pytest run on another branch shared the machine. The optional pg_stat_statements generic-plan cross-check was not run.
<!-- SECTION:NOTES:END -->
