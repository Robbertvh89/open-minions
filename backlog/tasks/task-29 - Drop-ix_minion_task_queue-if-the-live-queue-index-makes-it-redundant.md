---
id: TASK-29
title: Drop ix_minion_task_queue if the live-queue index makes it redundant
status: To Do
assignee: []
created_date: '2026-10-18 04:08'
labels:
  - db
dependencies: []
ordinal: 67000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
After TASK-7.4.2 the candidate query uses ix_minion_task_live_queue; ix_minion_task_queue (status, priority DESC, created_at) may be unused. Note: at ~2k live tasks the planner still bitmap-scanned ix_minion_task_queue + sort, so measure before dropping. Check all queries (list_tasks, fleet, splitter, watchdog, minion_ro views/Grafana) via pg_stat_user_indexes on the scale stack; if unused, drop in a migration with lock_timeout in upgrade and downgrade.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Evidence (pg_stat_user_indexes / EXPLAIN) that no query needs it, or it stays with a recorded reason
- [ ] #2 If dropped: migration up/down tested, candidate plan tests still green at 2k live tasks
<!-- AC:END -->
