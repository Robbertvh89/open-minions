---
id: TASK-5.2
title: M4.2 FR-17 graceful stop with 30 s grace
status: To Do
assignee: []
created_date: '2026-10-16 13:32'
labels:
  - M4
dependencies: []
parent_task_id: TASK-5
ordinal: 25000
---

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 SIGTERM: no new acquires; in-flight finishes within grace -> completed, else killed and released at deadline; exit 0
- [ ] #2 Second signal stops immediately; grace 0 = M1 behaviour
- [ ] #3 e2e: short subtask SUCCEEDED on SIGTERM; long subtask RELEASED within grace+5 s
<!-- AC:END -->
