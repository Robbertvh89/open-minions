---
id: TASK-43.1
title: Persist GPU quarantine expiry across executor restarts
status: In Progress
assignee: []
created_date: '2026-10-20 00:16'
updated_date: '2026-10-20 02:30'
labels:
  - executor
  - gpu
dependencies: []
parent_task_id: TASK-43
ordinal: 93000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Deferred from TASK-43 (see scratchpad plan / TASK-43 notes): persist per-engine gpu quarantine expiry in the state dir (0600, unsafe-state-dir rules like status.json/control.json) so a restart does not reset it. Low priority: the sandboxed worker cannot force a restart.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 Quarantine expiry survives an executor restart
- [x] #2 State file 0600, refused on unsafe state dir
<!-- AC:END -->
