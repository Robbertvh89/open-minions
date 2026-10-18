---
id: TASK-30
title: Guard against many unrunnable live tasks slowing acquire
status: To Do
assignee: []
created_date: '2026-10-18 04:08'
labels:
  - security
dependencies: []
ordinal: 68000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Security LOW from TASK-7.4.2: the live-task walk filters engine/model/memory/pool as Filter, not Index Cond, so a user can create many high-priority tasks no executor can run (e.g. model_memory_gb above any fleet budget) and every acquire walks past them (~1 us each). Options: per-team cap on live tasks; feasibility check at submission (model in allow-list with memory_gb <= max profile memory); index on (priority, created_at) including model_id.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A per-team live-task cap or submit-time feasibility check exists with a clear error code
- [ ] #2 Test: 5k unrunnable tasks don't push acquire beyond a stated bound
<!-- AC:END -->
