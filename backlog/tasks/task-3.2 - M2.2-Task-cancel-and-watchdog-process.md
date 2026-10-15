---
id: TASK-3.2
title: 'M2.2: Task cancel and watchdog process'
status: To Do
assignee: []
created_date: '2026-10-15 18:02'
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
