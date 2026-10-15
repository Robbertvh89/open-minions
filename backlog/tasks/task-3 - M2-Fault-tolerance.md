---
id: TASK-3
title: 'M2: Fault tolerance'
status: To Do
assignee: []
created_date: '2026-10-15 14:02'
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
