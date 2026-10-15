---
id: TASK-3
title: 'M2: Fault tolerance'
status: In Progress
assignee: []
created_date: '2026-10-15 14:02'
updated_date: '2026-10-15 17:52'
labels:
  - milestone
  - M2
dependencies: []
priority: high
ordinal: 3000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Heartbeats, watchdog reclaim, release, cancel, stale-acquirement 409. ARCHITECTURE §3.4.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Complete-vs-reclaim race test passes
- [ ] #2 Killed executor's subtask is re-run by another executor
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Carry-over from M1 (TASK-2.x reviews): (1) watchdog must count EXPIRED attempts toward max_attempts (budget = FAILED+EXPIRED; release does not consume it, ARCH §3.4 / FR-32); (2) executor leaves subtask RUNNING when complete retries exhaust on transport errors only — watchdog reclaim is the safety net; (3) add server-side per-subtask release cap so repeated releases (e.g. proxy/S3 gateway resetting one object, RemoteProtocolError) cannot livelock a subtask; (4) heartbeat endpoint not implemented yet (SPEC §6 marks it); executor must start sending heartbeats; (5) TASK-11 lease renewal relevant for multi-replica.
<!-- SECTION:NOTES:END -->
