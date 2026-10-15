---
id: TASK-3.3
title: 'M2.3: Executor heartbeats, cancel/stale handling, fault e2e, docs'
status: Done
assignee: []
created_date: '2026-10-15 18:02'
updated_date: '2026-10-15 20:31'
labels:
  - M2
dependencies:
  - TASK-3.1
parent_task_id: TASK-3
priority: high
ordinal: 17000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Plan steps S6-S7 of TASK-3 (D6, D8). Executor kills worker via WorkerHandle.interrupt() (respawnable) on heartbeat cancel/409.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 Killed executor's subtask is re-run by another executor (e2e)
- [x] #2 Executor stops work on heartbeat cancel/409 and keeps polling
- [x] #3 ADR-0008 written; SPEC/ARCH match code
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Docs (S8): ADR-0008 written (docs/adr/0008-fault-tolerance-reclaim-and-cancel.md, Accepted) and linked from ARCHITECTURE §9. ARCHITECTURE §2.2 heartbeater section, §2.3 watchdog process section, §3.4 pseudocode (SKIP LOCKED + re-check, executor row after acquirement), §4 lock-order note and cancel/heartbeat-timeout hazards updated. SPEC FR-8, FR-14 status implemented; §4.4 M2 status with settings list; FR-17 still partial. CLAUDE.md run line gets --heartbeat-interval; bench marked not implemented (M3); watchdog line annotated. .env.example: MINION_RELEASE_LIMIT, MINION_WATCHDOG_MAX_BACKOFF_S, MINION_EXECUTOR_HEARTBEAT_INTERVAL_S added. Key code: packages/minion-server/src/minion_server/services/reclaim.py, repositories/reclaim.py, services/heartbeat.py, packages/minion-watchdog/src/minion_watchdog/main.py, packages/minion-executor/src/minion_executor/heartbeat.py, loop.py (_on_signal), main.py (--heartbeat-interval).

Resolved: isolated test_heartbeat PUT timeout fixed by SeaweedFS warm-up in s3 fixture (06ab6f2).
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
M2.3 done in code (executor heartbeats, cancel/409 interrupt, fault e2e). Docs synced to code: ADR-0008 (reclaim SKIP LOCKED + re-check, budget, lazy cancel, max lease, heartbeat rate limit, executor interrupt), ARCHITECTURE §2.2/2.3/3.4/4, SPEC FR-8/14/31a/32/34 status, CLAUDE.md and .env.example. Open: FR-17 grace period still missing; no index on acquirement.acquired_at (max-lease scan).
<!-- SECTION:FINAL_SUMMARY:END -->
