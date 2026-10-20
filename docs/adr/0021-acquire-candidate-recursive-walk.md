# ADR-0021: Acquire candidate query as a recursive ordered walk

- Status: **Proposed** (TASK-54; measurements from the scale harness are added in step 5)
- Deviates from paper: no (query shape and one index only; ownership and FR-22 order are unchanged)

## Context

The candidate query read the live tasks in FR-22 order from `ix_minion_task_live_queue` and took each
task's first `window` PENDING subtasks. The planner priced the ordered walk (Incremental Sort over a
nested loop) above one bitmap scan plus a full sort, and flipped to the bitmap plan on
`ix_minion_task_queue` from about 1k live tasks: 61 buffers at 1k, 309 buffers / 10-13 ms at 10k. With
the per-team cap (ADR-0020), 20 teams x 500 = 10k live tasks is realistic. The flip depended on the
history size, so it could not be pinned with statistics.

## Decision

1. `candidates_stmt` is a `WITH RECURSIVE` walk. Each step reads the next 8 eligible live tasks after a
   row-compare cursor (no pending check), then takes the first with a PENDING subtask (probe `ORDER BY
   subtask_id LIMIT k`) or, if none, the chunk's last row as a skip row. It stops once the visited tasks
   hold `window` rows. The pending check only sees a bounded chunk, so a bad plan for the chunk read
   cannot multiply subtask probes. The final SELECT is the lateral subtask fetch + the same ORDER BY / LIMIT.
2. Migration 0016 recreates `ix_minion_task_live_queue` on `((-priority), created_at, team, project,
   task_name)` (same name and predicate) so all columns ascend and the cursor is a single Index Cond.
3. Every LIMIT and status is a SQL literal; `claim()` is unchanged (ownership: invariant 2).
4. `ix_minion_task_queue` is kept (TASK-29): the splitter claim and the cancel sweep use it.

## Rejected

- Literal LIMITs only: same plans. Statistics targets / extended statistics: no effect on the trade-off.
- Per-statement GUCs (`enable_bitmapscan`, `enable_sort`): leak into the claim transaction, cached
  generic plans keep the planning-time GUCs, behaviour differs across PG versions.
- Row-compare cursor on `priority DESC`: SQL lexicographic compare would sort created_at DESC as well.
- `EXISTS` / unordered `LIMIT 1` pending checks: flattened into a semi-join that drives the walk from
  `minion_subtask`, or a LIMIT-1 seq-scan trap.

## Consequences

- 10k live tasks: 96-127 buffers / ~0.3-0.5 ms in literal and generic plans (was 288-310 / 5-13 ms).
- A drained live task costs one probe (inherent; bounded by teams x cap): 10k of them cost 4018
  buffers / ~21 ms (main: 274-654). Window 256 with one pending subtask per task: 768 probes / ~3.1k buffers.
- **Stale statistics.** Tables ANALYZEd while empty and then filled without ANALYZE (restore,
  `pg_upgrade`, before autovacuum runs) can make the planner bitmap-scan the live tasks once per step.
  The unchunked walk then probed 39,994 times (281k buffers / 126 ms at 10k live); with the chunk it is
  ~1.2k buffers / ~13 ms, about main. Ops: `vacuumdb --analyze-in-stages` after restore or `pg_upgrade`
  (README Upgrading).
- Known residual (follow-up): with a very skewed task mix the per-task probe may use
  `ix_minion_subtask_progress` + Sort instead of `ix_minion_subtask_pending`.
- `-priority` overflows only for `INT_MIN`; the API allows -1000..1000. A direct-SQL row with that value
  makes the INSERT fail loudly, never silently. A CHECK (`priority BETWEEN -1000 AND 1000`) would be a
  separate migration (note only).
- Migration 0016 takes ACCESS EXCLUSIVE on `minion_task` (DROP INDEX then CREATE INDEX in one transaction,
  `lock_timeout` 5 s): stop server and watchdog first (README Upgrading).
