---
id: TASK-46
title: 'perf(server): build hot SQL statements once (acquire claim, complete path)'
status: To Do
assignee: []
created_date: '2026-10-19 23:03'
labels:
  - perf
  - scale
dependencies: []
ordinal: 86000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Follow-up from TASK-36 profile (docs/bench/m-task36-server-cpu-apple-m3-pro.md).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 sqlalchemy/sql/* self-share < 8 % in a py-spy rerun at W=1 and W=4
- [ ] #2 W=1 S-sat goodput >= 1.25x the 69.6/s TASK-36 baseline
<!-- AC:END -->
