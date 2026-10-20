---
id: TASK-57
title: >-
  Watchdog settle + cancel sweep: status literals so generic plans keep index
  paths
status: To Do
assignee: []
created_date: '2026-10-20 04:54'
labels:
  - perf
dependencies: []
ordinal: 100000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
settleable_stmt (completion.py) binds status IN params: generic plan Seq Scan on minion_task, 994 buf/7.4 ms vs 275/1.0 literal at 10k live + 40k history. cancel_stuck_pending 'recent' subquery (tasks.py): generic pk scan 72k buf/21 ms vs 1.4k/0.9 ms. Use status_literal like acquisition.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Generic and literal plans equal for both statements (EXPLAIN test)
<!-- AC:END -->
