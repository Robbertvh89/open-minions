---
id: TASK-36
title: Profile server CPU per acquire under stress (4 workers saturated at ~169/s)
status: In Progress
assignee: []
created_date: '2026-10-18 11:55'
updated_date: '2026-10-19 23:02'
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
- [x] #1 Profile attached with top costs
- [x] #2 Throughput vs MINION_SERVER_WORKERS curve in docs/bench
- [x] #3 Decide the ADR-0001 revisit: a py-spy profile showing Python interpreter work dominating the acquire/complete path AND throughput not scaling roughly linearly with MINION_SERVER_WORKERS/hosts triggers a server-rewrite ADR; otherwise ADR-0001 stands (ADR-0016)
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Report: docs/bench/m-task36-server-cpu-apple-m3-pro.md (+ .json). Measured 2026-10-19/20 at fe631c9 on the M3 Pro. S-sat goodput (median of 3) W=1/2/4/6/8/12: 69.6/103.3/159.7/175.5/178.3/164.3 completes/s; stress W=4 169.8, W=8 194.2. C_srv 14.4 to 33.4 ms per subtask (W=1 to 8), claim mean 0.73 to 10.7 ms, claim calls per win 1.06 to 1.73. Profiles: SQLAlchemy 40 to 48 % self (sql/ construction+cache key 21 to 26 %), uvloop native 20 to 33 %, asyncpg 9 to 12 %; P_py 0.67 (P1), 0.80 (P2), cross-check P4 bytecode leaf 0.42. Decision rule: A met, B met, C NOT met (claim mean > 2x W=1) -> ADR-0001 stands, no ADR-0020. ADR-0001/0016 addenda written. Window experiment: MINION_ACQUIRE_WINDOW=64 at W=8 gives +11 % goodput. NF-5 multi-host not measured. Follow-up tasks proposed to the orchestrator (SQLAlchemy statement caching, claim collisions at W>1, asyncpg round trips, S3 promote cost).
<!-- SECTION:NOTES:END -->
