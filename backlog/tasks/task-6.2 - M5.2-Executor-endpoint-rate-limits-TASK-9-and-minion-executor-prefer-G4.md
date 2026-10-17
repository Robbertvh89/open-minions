---
id: TASK-6.2
title: M5.2 Executor endpoint rate limits (TASK-9) and minion-executor prefer (G4)
status: To Do
assignee: []
created_date: '2026-10-17 03:33'
labels:
  - M5
dependencies: []
parent_task_id: TASK-6
ordinal: 33000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
minion_server/ratelimit.py: in-memory min interval per (executor_id, op), monotonic, bounded LRU. MINION_PRESENCE_MIN_INTERVAL_S (5), MINION_ACQUIRE_MIN_INTERVAL_S (0.5), MINION_REGISTER_MIN_INTERVAL_S (5); 0 disables. Applied in api/executor.py after auth, before any DB write/acquisition: 429 TOO_MANY_REQUESTS + Retry-After. Executor: loop.py register (:466) and acquire (:517) treat 429 as max(Retry-After, backoff) and continue; presence honours Retry-After. New CLI minion-executor prefer --server URL (--team T [--project P] [--task-name N] | --clear) -> PUT /executor/preference. Decision D4: per-replica soft state (NF-5 note: counters are not coordination state); per-IP limits stay at the proxy.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Presence/acquire/register faster than their interval get 429 with Retry-After and no DB write
- [ ] #2 Executor survives 429 on acquire/register/presence and waits at least Retry-After
- [ ] #3 Concurrent-acquire race test and existing e2e stay green
- [ ] #4 prefer sets and clears the preference; integration: next acquire prefers that team
- [ ] #5 The limiter never replaces the ON CONFLICT acquisition (invariant 2)
<!-- AC:END -->
