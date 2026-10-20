# TASK-34 settleable_tasks and Grafana task panels at 1M subtasks: Apple M3 Pro

Raw data: [`m-task34-settle-scan-apple-m3-pro.json`](m-task34-settle-scan-apple-m3-pro.json) (every
`execution_ms` sample, the first plan of each cell, plan flags). Run on 2026-10-20 with
`minion-scale explain --settle-batch 100` (step 1 commit e92e3ea).

## Setup

- Apple M3 Pro, Docker Desktop (server 29.5.2), compose project `open-minions-scale`
  (`deploy/compose.scale.yml`), PostgreSQL 16, `shared_buffers` 1 GB, loopback ports only. No server, no
  watchdog, no executor tokens (`--executors 0`), so nothing mutates the fixture.
- Caveat: a full pytest CI run for another branch was running on the same Mac (load average about 5 on 12
  cores). Repeats within a cell agree to a few percent; no verdict is within 25 % of its threshold.
- Fixture per L: `minion-scale seed --tasks L --subtasks 1000000/L --executors 0`, then
  (1) 16 counter shards per live task at 30 % done, half of the tasks RUNNING, (2) five live tasks with
  `succeeded = total_subtasks` (settleable, the lost-settle positives), (3) history: H terminal tasks
  (`hist-NNNNNN`, status mix of SUCCEEDED/PARTIAL/FAILED/CANCELLED, 16 full counter shards each, no subtasks),
  added incrementally H = 0, 10,000, 100,000. SQL as in the TASK-34 plan (Step 2). The fixture is synthetic:
  `minion-scale verify` was not run and is not evidence.
- Matrix: L in {100, 1,000 (realistic), 10,000 (stress)} live tasks x H in {0, 10k, 100k}; 3 explains per
  cell (each runs `ANALYZE` first), median of the 3 reported. Fresh stack per L (`down -v`, on the scale
  project only). BEFORE = join + GROUP BY/HAVING (e92e3ea); AFTER = lateral rewrite below.
- Not done: the optional `pg_stat_statements` cross-check of the generic plan (the statement has only
  constants and a LIMIT, so custom and generic plans should coincide).

## Result: settleable_tasks (median execution ms, BEFORE -> AFTER)

| Live tasks L | H = 0 | H = 10,000 | H = 100,000 |
|---|---|---|---|
| 100 | 0.69 -> 0.59 | 0.80 -> 0.57 | 0.79 -> 0.59 |
| 1,000 | 4.56 -> 4.83 | 7.50 -> 6.26 | **13.55 -> 5.32** |
| 10,000 | 68.4 -> 77.4 | 106.6 -> 77.6 | 98.9 -> 88.8 |

All 18 cells return the 5 settleable positives.

Decision rule (a) warm <= 20 ms at L = 1,000 and <= 200 ms at L = 10,000: BEFORE and AFTER pass.
(b) cost does not grow with history (H = 100k <= 1.5x H = 0): BEFORE fails at L = 1,000 (13.55 / 4.56 =
2.97x; first sample series 14.0 ms, same) and borders at L = 10,000 (1.45x, 1.56x at H = 10k); AFTER passes
(L = 1,000: 1.10x, L = 10,000: 1.15x, L = 100: 1.00x).

Why: BEFORE the planner reached `minion_task` with a parallel scan of the primary key and filtered the
history out ("Rows Removed by Filter: 33333" per worker at L = 1,000, H = 100k) instead of using the partial
index `ix_minion_task_live_queue`. AFTER the statement is driven from the live tasks and sums each task's
counter shards in a lateral subquery (one `pk_minion_task_counter` range scan per live task; the plan is
`Index Scan using ix_minion_task_live_queue` + `Aggregate` over the counter PK, no history read).
Verdict: **BOUNDED** by a query rewrite, no migration, same result set (a live task without counter rows sums
to NULL and is not settleable, as the inner join excluded it). Remaining cost is O(live tasks x 16 shards)
(5 ms per 1,000 live tasks per tick, 60 s default interval); no keyset rotation needed, so the ADR-0015
"settled within one watchdog interval" semantics are unchanged (no ADR addendum).

## Result: Grafana panels on `minion_ro.task` (AFTER = BEFORE, the view is untouched)

grafana_task_status (count by status), median ms:

| L | H = 0 | H = 10,000 | H = 100,000 |
|---|---|---|---|
| 100 | 0.06 | 2.02 | 18.4 |
| 1,000 | 0.24 | 2.07 | 19.6 |
| 10,000 | 2.09 | 4.02 | 19.4 |

PostgreSQL removes the `LEFT JOIN LATERAL` (the aggregate has no GROUP BY, so it is at most one row, and no
column of it is used): no `minion_task_counter` node in any of the 9 plans, only a `Seq Scan` on `minion_task`
(about 19 ms at 101k rows). **JUSTIFIED** (rule: no counter node; also <= 50 ms at L = 1,000, H = 100k). The
review-note concern (aggregate per task row for the count panels) does not materialise; no migration 0013.

grafana_live_tasks (LIMIT 50 table), median ms:

| L | H = 0 | H = 10,000 | H = 100,000 |
|---|---|---|---|
| 100 | 0.63 | 0.60 | 0.63 |
| 1,000 | 5.05 | 6.51 | 8.71 |
| 10,000 | 84.9 | 95.7 | 4.23 |

**JUSTIFIED** (rule: <= 50 ms at L = 10,000, H = 100k: 4.2 ms, 50 counter lookups). Observation, not a
failure: at L = 10,000 with H <= 10k the planner evaluates the counter lateral for all 10,000 live rows
before the sort/limit (85 to 96 ms); with H = 100k it picks a bitmap scan + top-N and evaluates 50. A 10,000
live-task backlog refreshed every 30 s by one viewer is still far from a load problem.

grafana_subtask_status (informational, 1M-row `Seq Scan` on `minion_ro.subtask`): 41 to 66 ms in every
cell, far below the 500 ms follow-up threshold. No change.

## Verdicts

| Query | Verdict | Change |
|---|---|---|
| settleable_tasks | BOUNDED (rewrite) | lateral, driven from live tasks: 13.6 -> 5.3 ms at L = 1,000, H = 100k; history-independent |
| grafana_task_status | JUSTIFIED | none (join removed by the planner) |
| grafana_live_tasks | JUSTIFIED | none |
| grafana_subtask_status | informational, ok | none |
