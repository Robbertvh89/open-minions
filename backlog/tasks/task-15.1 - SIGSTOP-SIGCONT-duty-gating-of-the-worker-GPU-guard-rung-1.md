---
id: TASK-15.1
title: SIGSTOP/SIGCONT duty gating of the worker (GPU guard rung 1)
status: In Progress
assignee: []
created_date: '2026-10-18 21:07'
updated_date: '2026-10-20 04:14'
labels:
  - executor
  - security
dependencies: []
parent_task_id: TASK-15
ordinal: 79000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Split from TASK-15 plan step 4: suspend/resume the worker under _sig_lock to throttle over-duty GPU use before the kill rung; orphan test (control SIGKILLed while worker stopped), setpgid fallback; gpu test 10 Hz throttle same checksum.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Over-duty worker throttled before kill (gpu test)
- [ ] #2 No stranded stopped worker when control dies (macos test)
<!-- AC:END -->
