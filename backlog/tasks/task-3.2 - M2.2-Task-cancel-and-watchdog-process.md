---
id: TASK-3.2
title: 'M2.2: Task cancel and watchdog process'
status: In Progress
assignee: []
created_date: '2026-10-15 18:02'
updated_date: '2026-10-15 18:36'
labels:
  - M2
dependencies:
  - TASK-3.1
parent_task_id: TASK-3
priority: high
ordinal: 16000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Plan steps S4-S5 of TASK-3 (D4, D7)
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Cancel endpoint: pending subtasks CANCELLED, running owners get heartbeat cancel=true, owner ops finish as CANCELLED
- [ ] #2 minion-watchdog reclaims stale acquirements and sweeps cancelled tasks; stops cleanly on SIGTERM
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
From TASK-3.1 security audit (LOW, deferred here): (a) max lease — watchdog also reclaims acquirements older than MINION_MAX_LEASE_S (acquired_at < now()-max_lease) so a heartbeating-but-never-finishing executor can't hold a subtask forever; (b) heartbeat rate limit — per-executor minimum interval (429 when beats arrive faster than ~1/10 of interval) or document reverse-proxy rate limit.
<!-- SECTION:NOTES:END -->
