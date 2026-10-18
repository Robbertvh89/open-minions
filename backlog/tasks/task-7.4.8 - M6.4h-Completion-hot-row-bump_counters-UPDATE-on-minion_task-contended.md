---
id: TASK-7.4.8
title: 'M6.4h Completion hot row: bump_counters UPDATE on minion_task contended'
status: In Progress
assignee: []
created_date: '2026-10-18 05:20'
updated_date: '2026-10-18 09:10'
labels:
  - scale
dependencies: []
parent_task_id: TASK-7.4
ordinal: 69000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
After TASK-7.4.3 the top pg_stat_statements entry is UPDATE minion_task SET succeeded_subtasks=... (bump_counters, repositories/completion.py:204): 15.6 ms mean in B-20k, 32.6 ms in A stress — every completion of a task row-locks the same task row. Options: batch/deferred counters (per-subtask terminal status is the source of truth; derive counts periodically or via a counter table sharded by subtask_id % N summed on read), or skip the task-row UPDATE until the last subtask (detect completion via count of non-terminal subtasks). Must keep task status derivation correct (NF-2/NF-3) and the verify counter checks.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Completion path no longer serializes on one task row (pg_stat_statements before/after)
- [ ] #2 Task status/counters stay correct (verify passes; existing completion/derive tests green)
- [ ] #3 B-20k and A stress before/after numbers in PR
<!-- AC:END -->
