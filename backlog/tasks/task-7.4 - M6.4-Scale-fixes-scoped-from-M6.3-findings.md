---
id: TASK-7.4
title: M6.4 Scale fixes (scoped from M6.3 findings)
status: Done
assignee: []
created_date: '2026-10-17 20:36'
updated_date: '2026-10-18 18:18'
labels:
  - scale
dependencies: []
parent_task_id: TASK-7
ordinal: 55000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Candidates: (1) candidate query: two-step head-task pick in queue order, then per-task top-K from the partial PENDING index (same FR-22 order, no full sort); (2) head contention / conflict rounds; (3) minion_subtask autovacuum tuning via migration; (4) DB pool sizing; (5) bump_counters task-row hot lock. SKIP LOCKED would deviate from the paper -> ADR first. Never weaken invariant 2.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 Each fix has a before/after number from minion-scale in the PR
- [x] #2 Protocol race and concurrent-acquire tests stay green
- [x] #3 M6.3 report re-run shows NF-1 met or gap documented with ADR
<!-- AC:END -->
