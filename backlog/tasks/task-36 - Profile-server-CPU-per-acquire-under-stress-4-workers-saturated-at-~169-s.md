---
id: TASK-36
title: Profile server CPU per acquire under stress (4 workers saturated at ~169/s)
status: To Do
assignee: []
created_date: '2026-10-18 11:55'
updated_date: '2026-10-18 12:07'
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
- [ ] #3 Decide the ADR-0001 revisit: a py-spy profile showing Python interpreter work dominating the acquire/complete path AND throughput not scaling roughly linearly with MINION_SERVER_WORKERS/hosts triggers a server-rewrite ADR; otherwise ADR-0001 stands (ADR-0016)
<!-- AC:END -->
