---
id: TASK-54.1
title: 'Acquire per-task pending probe: no plan may sort a large task''s PENDING set'
status: To Do
assignee: []
created_date: '2026-10-20 04:54'
labels:
  - perf
  - db
dependencies: []
parent_task_id: TASK-54
ordinal: 99000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
ix_minion_subtask_progress + Sort chosen when the per-task estimate is ~1 row; one 20k-subtask task ahead of 10k small ones: 606 buf/16.6 ms (main), 978/7.8 ms (walk). Scratch fix: progress index -> (team, project, task_name, status, subtask_id) via CREATE/DROP INDEX CONCURRENTLY (15 buf/0.15 ms). Measure write cost and the other users of the progress index.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Probe plans use an ordered index without Sort for a skewed task mix, literal and generic
- [ ] #2 Migration up/down tested (CONCURRENTLY, lock_timeout)
<!-- AC:END -->
