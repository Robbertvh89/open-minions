---
id: TASK-7.4.4
title: >-
  M6.4d Overload: acquire deadline before INSERT, pool timeout, 503 load
  shedding, owner-op headroom
status: To Do
assignee: []
created_date: '2026-10-18 02:21'
labels:
  - scale
dependencies: []
parent_task_id: TASK-7.4
ordinal: 63000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
MINION_DB_POOL_TIMEOUT_S (5), MINION_ACQUIRE_DEADLINE_S (10, < executor 30 s HTTP timeout) checked before the acquirement INSERT, MINION_ACQUIRE_CONCURRENCY per-process semaphore (pool+overflow-4) tried after _limited, pool TimeoutError -> 503 SERVER_BUSY + Retry-After (never 500); executor honours 503 Retry-After for acquire and complete. SPEC 6.2 codes, ARCHITECTURE 3.2.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 No acquirement commits after the deadline (test)
- [ ] #2 Pool exhaustion answers 503 with Retry-After, never 500
- [ ] #3 Heartbeat/complete succeed while acquires are shed
- [ ] #4 Executor honours 503 Retry-After (acquire and complete); the harness parity test is extended to 503
- [ ] #5 A stress re-run: 0 subtasks FAILED without injected faults; numbers in PR
<!-- AC:END -->
