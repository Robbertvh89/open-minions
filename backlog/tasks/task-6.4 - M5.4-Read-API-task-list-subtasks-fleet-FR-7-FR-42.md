---
id: TASK-6.4
title: 'M5.4 Read API: task list, subtasks, fleet (FR-7, FR-42)'
status: In Progress
assignee: []
created_date: '2026-10-17 03:33'
updated_date: '2026-10-17 07:20'
labels:
  - M5
dependencies:
  - TASK-6.1
parent_task_id: TASK-6
ordinal: 35000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
minion-common: TaskPage, SubtaskOut, SubtaskPage, ExecutorOut (no token_hash; online/busy per ARCHITECTURE 7); client methods. repositories list_tasks (keyset created_at DESC + key, limit<=100, one grouped counts query per page), list_subtasks (keyset on subtask_id, status filter), list_fleet. Routes: GET /api/v1/tasks, GET /api/v1/tasks/{team}/{project}/{task_name}/subtasks, GET /api/v1/executors (require_operator, api/fleet.py). Update SPEC 6.1.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 GET /tasks returns only the caller's teams; filtering another team gives 403
- [x] #2 Pagination stable under concurrent inserts; tampered cursor gives 422
- [x] #3 /subtasks paginates and filters by status with the same 403/404 rules as get_task
- [x] #4 /executors 403 for non-operators, never returns token_hash, busy/online correct in integration
<!-- AC:END -->
