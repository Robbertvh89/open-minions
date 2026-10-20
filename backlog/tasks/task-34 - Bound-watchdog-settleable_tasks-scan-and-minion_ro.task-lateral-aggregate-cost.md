---
id: TASK-34
title: Bound watchdog settleable_tasks scan and minion_ro.task lateral aggregate cost
status: In Progress
assignee: []
created_date: '2026-10-18 10:22'
updated_date: '2026-10-20 01:00'
labels:
  - perf
dependencies: []
ordinal: 73000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Follow-up from TASK-7.4.8 review/security (LOW/NIT). settleable_tasks sums shards of every live task each tick (LIMIT after HAVING); minion_ro.task LEFT JOIN LATERAL aggregate runs per task row even for count-by-status Grafana panels. EXPLAIN both at scale (add to minion-scale explain set) and bound if they show up.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 EXPLAIN of settleable_tasks and Grafana status panel in minion-scale explain set
- [ ] #2 Bounded or justified by measurement
<!-- AC:END -->
