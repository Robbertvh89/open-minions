---
id: TASK-7.4.6
title: >-
  M6.4f Deployment: multi-worker server, pool sizing, minion_subtask autovacuum
  migration
status: Done
assignee: []
created_date: '2026-10-18 02:21'
updated_date: '2026-10-18 09:10'
labels:
  - scale
dependencies: []
parent_task_id: TASK-7.4
ordinal: 65000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
MINION_SERVER_WORKERS (uvicorn factory workers, default 1), prometheus multiprocess metrics dir on /tmp tmpfs, startup warning when workers x pool exceeds connection limit, migration 0010 autovacuum reloptions on minion_subtask, compose.scale 4 workers pool 8+4, ARCHITECTURE 6 sizing, ADR-0012 note (per-worker limiter).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 /metrics aggregates all workers
- [x] #2 Migration 0010 up/down tested
- [x] #3 1 vs 4 workers before/after numbers (B-20k, A stress) in PR
<!-- AC:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Merged #52: MINION_SERVER_WORKERS (uvicorn factory workers), multiprocess metrics in a private validated dir, connection-budget warning with reserve, migration 0010 autovacuum on minion_subtask, compose.scale 4 workers. 1->4 workers: B-20k 62->121/s, A stress 70->172/s p99 0.5 s, 0 FAILED. CI run 39113368320: 2034 passed.
<!-- SECTION:FINAL_SUMMARY:END -->
