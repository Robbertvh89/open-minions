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

1. `candidates_stmt` is a `WITH RECURSIVE` walk with one `LIMIT 1` index step per task, a row-compare
   cursor, a per-task pending probe (`ORDER BY subtask_id LIMIT k`), and a stop once the visited tasks
   hold `window` rows. The final SELECT is the lateral subtask fetch + the same ORDER BY / LIMIT.
2. Migration 0015 recreates `ix_minion_task_live_queue` on `((-priority), created_at, team, project,
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

- 10k live tasks: ~96 buffers / ~0.3 ms in literal and generic plans (was 288-310 / 5-13 ms).
- A drained live task costs one probe (inherent; bounded by teams x cap). Window 256 with one pending
  subtask per task costs up to 768 probes / ~3k buffers.
- Known residual (follow-up): with a very skewed task mix the per-task probe may use
  `ix_minion_subtask_progress` + Sort instead of `ix_minion_subtask_pending`.
- `-priority` overflows only for `INT_MIN`; the API allows -1000..1000.
