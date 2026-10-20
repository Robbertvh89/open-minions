---
id: TASK-31
title: 'minion_acquire_total: separate exhausted outcome label'
status: Done
assignee: []
created_date: '2026-10-18 05:20'
updated_date: '2026-10-20 02:08'
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
- [x] #1 Exhausted acquires counted as outcome=exhausted (test)
- [x] #2 Docs updated (no Grafana panel reads minion_acquire_total; dashboards are PostgreSQL-only)
<!-- AC:END -->





## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
No Grafana panel reads minion_acquire_total (dashboards query PostgreSQL views); docs updated only (SPEC, ARCHITECTURE, ADR-0013). 'error' remains timed in minion_acquire_seconds only, not counted.
<!-- SECTION:NOTES:END -->
