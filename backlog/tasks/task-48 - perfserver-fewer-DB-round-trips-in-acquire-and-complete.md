---
id: TASK-48
title: 'perf(server): fewer DB round trips in acquire and complete'
status: To Do
assignee: []
created_date: '2026-10-19 23:03'
labels:
  - perf
  - scale
dependencies: []
ordinal: 88000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Follow-up from TASK-36 profile (docs/bench/m-task36-server-cpu-apple-m3-pro.md).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 asyncpg self-share < 7 % at W=8 S-sat
- [ ] #2 statements per complete reduced (count before/after)
<!-- AC:END -->
