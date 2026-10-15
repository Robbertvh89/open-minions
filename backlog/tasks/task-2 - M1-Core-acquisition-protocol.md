---
id: TASK-2
title: 'M1: Core acquisition protocol'
status: To Do
assignee: []
created_date: '2026-10-15 14:02'
labels:
  - milestone
  - M1
dependencies: []
priority: high
ordinal: 2000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Task create + async splitter, acquire/complete/fail endpoints, executor loop + worker subprocess with fake runner. See ARCHITECTURE §3.1-3.3.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Concurrent-acquire race test (>=50 parallel) yields unique owners
- [ ] #2 End-to-end: task submitted, split, executed by fake runner, outputs promoted in MinIO
<!-- AC:END -->
