---
id: TASK-54
title: Acquire candidate plan becomes O(live tasks) above ~1k live tasks
status: To Do
assignee: []
created_date: '2026-10-20 03:30'
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
