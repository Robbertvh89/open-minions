---
id: TASK-47
title: >-
  perf(server): cut acquire claim collisions at W > 1 (adaptive/larger
  MINION_ACQUIRE_WINDOW or per-worker offsets; invariant 2 unchanged)
status: To Do
assignee: []
created_date: '2026-10-19 23:03'
labels:
  - perf
  - scale
dependencies: []
ordinal: 87000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Follow-up from TASK-36 profile (docs/bench/m-task36-server-cpu-apple-m3-pro.md).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 claim calls per win <= 1.3 and claim mean <= 5 ms at W=8 S-sat
- [ ] #2 W=8 S-sat goodput >= 195/s
<!-- AC:END -->
