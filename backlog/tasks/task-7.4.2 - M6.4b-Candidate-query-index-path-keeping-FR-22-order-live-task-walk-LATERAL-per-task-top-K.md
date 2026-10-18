---
id: TASK-7.4.2
title: >-
  M6.4b Candidate query: index path keeping FR-22 order (live-task walk +
  LATERAL per-task top-K)
status: In Progress
assignee: []
created_date: '2026-10-18 02:21'
updated_date: '2026-10-18 03:23'
labels:
  - scale
dependencies: []
parent_task_id: TASK-7.4
ordinal: 61000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Migration 0009 ix_minion_task_live_queue (priority DESC, created_at, team, project, task_name) WHERE status IN ('PENDING','RUNNING'); candidates_stmt rewritten as live-task subquery CROSS JOIN LATERAL top-K PENDING per task over ix_minion_subtask_pending; same name/signature/columns; status values as SQL literals so asyncpg generic plans still match partial indexes; preference filters moved onto minion_task.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Property test: identical rows and order vs the old statement incl. preference shapes and filters
- [ ] #2 EXPLAIN (literal and GENERIC_PLAN) at 100k: no Seq Scan/Sort on minion_subtask, both indexes used
- [ ] #3 minion-scale explain at 1M <= 5 ms (was 214 ms); knee 1/10/50 numbers in PR
- [ ] #4 Race and test_acquire tests green
<!-- AC:END -->
