---
id: TASK-36
title: Profile server CPU per acquire under stress (4 workers saturated at ~169/s)
status: To Do
assignee: []
created_date: '2026-10-18 11:55'
labels:
  - perf
  - scale
dependencies: []
ordinal: 75000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
TASK-7.4.7: stress mix bound by server CPU (388% of 4 workers), Postgres ~94%. Profile (py-spy) acquire/complete path: pydantic, presign, structlog, metrics; measure gain from more workers/hosts.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Profile attached with top costs
- [ ] #2 Throughput vs MINION_SERVER_WORKERS curve in docs/bench
<!-- AC:END -->
