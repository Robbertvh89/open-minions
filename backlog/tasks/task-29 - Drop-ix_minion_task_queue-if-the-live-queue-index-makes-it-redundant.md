---
id: TASK-29
title: Drop ix_minion_task_queue if the live-queue index makes it redundant
status: Done
assignee: []
created_date: '2026-10-18 04:08'
updated_date: '2026-10-20 04:54'
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
- [x] #1 Evidence (pg_stat_user_indexes / EXPLAIN) that no query needs it, or it stays with a recorded reason
- [ ] #2 If dropped: migration up/down tested, candidate plan tests still green at 2k live tasks
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
2026-10-20 KEEP ix_minion_task_queue (TASK-54 analysis, scratchpad task54-plan.md): it is the only index leading with status. The splitter claim (polled by every server worker) uses it at 6 buffers; without it the query scans the whole (team, status) index, which grows with history. The cancel sweep also uses it. The original concern (its bitmap displacing the live-queue walk) disappears with TASK-54's recursive walk. AC#2 n/a (not dropped).
<!-- SECTION:NOTES:END -->
