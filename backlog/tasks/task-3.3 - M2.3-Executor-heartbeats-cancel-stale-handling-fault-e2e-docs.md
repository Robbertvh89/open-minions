---
id: TASK-3.3
title: 'M2.3: Executor heartbeats, cancel/stale handling, fault e2e, docs'
status: To Do
assignee: []
created_date: '2026-10-15 18:02'
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
- [ ] #1 Killed executor's subtask is re-run by another executor (e2e)
- [ ] #2 Executor stops work on heartbeat cancel/409 and keeps polling
- [ ] #3 ADR-0008 written; SPEC/ARCH match code
<!-- AC:END -->
