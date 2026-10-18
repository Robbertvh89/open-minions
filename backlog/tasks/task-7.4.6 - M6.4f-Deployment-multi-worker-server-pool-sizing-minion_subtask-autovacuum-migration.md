---
id: TASK-7.4.6
title: >-
  M6.4f Deployment: multi-worker server, pool sizing, minion_subtask autovacuum
  migration
status: To Do
assignee: []
created_date: '2026-10-18 02:21'
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
- [ ] #1 /metrics aggregates all workers
- [ ] #2 Migration 0010 up/down tested
- [ ] #3 1 vs 4 workers before/after numbers (B-20k, A stress) in PR
<!-- AC:END -->
