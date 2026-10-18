---
id: TASK-31
title: 'minion_acquire_total: separate exhausted outcome label'
status: To Do
assignee: []
created_date: '2026-10-18 05:20'
labels:
  - observability
dependencies: []
ordinal: 70000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
TASK-7.4.3 logs acquire_rounds_exhausted but counts exhausted acquires as 'empty' in minion_acquire_total, hiding contention in Grafana. Add an 'exhausted' outcome (fixed label set assigned|empty|exhausted|error), update the fleet dashboard panel and docs.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Exhausted acquires counted as outcome=exhausted (test)
- [ ] #2 Grafana panel and docs updated
<!-- AC:END -->
