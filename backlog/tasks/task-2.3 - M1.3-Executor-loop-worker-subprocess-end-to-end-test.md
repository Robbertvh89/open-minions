---
id: TASK-2.3
title: 'M1.3: Executor loop, worker subprocess, end-to-end test'
status: To Do
assignee: []
created_date: '2026-10-15 14:26'
labels:
  - M1
dependencies:
  - TASK-2.2
parent_task_id: TASK-2
priority: high
ordinal: 11000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Plan steps C1, C2, E1 of TASK-2: Runner protocol + fake runner, spawned worker subprocess, static profile budgets; control loop (backoff, 409 discard, BAD_INPUT, worker crash -> retryable fail, SIGTERM release), CLI run wiring; E2E test with 2 executors; ADR-0004 and ADR-0007; SPEC/ARCH/CLAUDE.md updates.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 E2E: task submitted -> split -> 2 executors with fake runner -> all outputs promoted in S3, no duplicates
- [ ] #2 Worker crash reported as retryable fail; SIGTERM releases current subtask
- [ ] #3 ADR-0004 and ADR-0007 written; docs match code
<!-- AC:END -->
