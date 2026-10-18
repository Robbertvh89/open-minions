---
id: TASK-7.4.4
title: >-
  M6.4d Overload: acquire deadline before INSERT, pool timeout, 503 load
  shedding, owner-op headroom
status: Done
assignee: []
created_date: '2026-10-18 02:21'
updated_date: '2026-10-18 06:57'
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
- [x] #1 No acquirement commits after the deadline (test)
- [x] #2 Pool exhaustion answers 503 with Retry-After, never 500
- [x] #3 Heartbeat/complete succeed while acquires are shed
- [x] #4 Executor honours 503 Retry-After (acquire and complete); the harness parity test is extended to 503
- [x] #5 A stress re-run: 0 subtasks FAILED without injected faults; numbers in PR
<!-- AC:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Merged #50: acquire deadline from arrival (pre-claim check, statement_timeout, post-claim check), acquire slots + per-executor cap, pool timeout 5 s -> 503 SERVER_BUSY never 500; executor/sim honour 503 Retry-After, complete sheds don't burn retries and release SERVER_BUSY after budget. A stress: 0 FAILED (was 269), 0 non-503 5xx, 0 ghosts, 71.6/s. CI run 39105049639: 1993 passed.
<!-- SECTION:FINAL_SUMMARY:END -->
