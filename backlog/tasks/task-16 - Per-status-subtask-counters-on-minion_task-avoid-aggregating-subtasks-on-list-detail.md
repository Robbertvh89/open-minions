---
id: TASK-16
title: >-
  Per-status subtask counters on minion_task (avoid aggregating subtasks on
  list/detail)
status: In Progress
assignee: []
created_date: '2026-10-17 07:22'
updated_date: '2026-10-20 01:36'
labels:
  - server
  - performance
dependencies: []
ordinal: 43000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
GET /tasks aggregates every subtask of up to 100 tasks per page (up to ~10M rows); keep counters on minion_task maintained in the same transactions as status changes, or cache. From TASK-6.4 security review.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 List/detail counts read O(1) per task
- [ ] #2 Counters stay consistent under concurrent acquire/complete/reclaim (integration test)
<!-- AC:END -->
