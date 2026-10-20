---
id: TASK-53
title: Cancel of a fresh large task is pathological before ANALYZE
status: In Progress
assignee: []
created_date: '2026-10-20 02:19'
updated_date: '2026-10-20 03:07'
labels:
  - perf
  - server
dependencies: []
ordinal: 94000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Found in TASK-16 storm tests: cancelling a 25,000-subtask task right after the split (no ANALYZE yet) took 59 s (175 s with concurrent sweeps), CPU-bound; after ANALYZE minion_subtask 0.37 s. Statement: UPDATE ... WHERE subtask_id IN (SELECT ... FOR UPDATE SKIP LOCKED LIMIT 10000). Not a TASK-16 regression (plan unchanged), not measured on pre-TASK-16 code. Options: rewrite as UPDATE ... FROM (locking subquery) join / CTE, or ANALYZE (or a targeted statistics hint) after a large split.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 Cancel of a freshly split 25k-subtask task (no ANALYZE) completes in < 2 s
- [x] #2 Plan before/after recorded (EXPLAIN)
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Before (25k PENDING, no ANALYZE, autovacuum off): old UPDATE ... subtask_id IN (SELECT ... FOR UPDATE SKIP LOCKED LIMIT 10000) = Nested Loop Semi Join, rows=1 estimates, subquery re-executed per outer row (LockRows loops=25000, 319M buffer hits), 71.6 s EXPLAIN / 59.7 s cancel loop. After: WITH c AS MATERIALIZED (locking batch) UPDATE ... FROM c WHERE full PK = c.PK: CTE Scan c (loops=1) + Nested Loop with PK-prefix index scan per row, Execution 88 ms per 10k batch, whole 25k cancel ~1.6 s test-measured incl. 3 batches; sweep_cancelled got the same PK join. Test: packages/minion-server/tests/test_cancel_plan.py
<!-- SECTION:NOTES:END -->
