---
id: TASK-22
title: Fix flaky test_prefer_command_makes_the_next_acquire_prefer_that_team
status: In Progress
assignee: []
created_date: '2026-10-17 13:45'
updated_date: '2026-10-17 13:45'
labels:
  - executor
  - test
dependencies: []
ordinal: 47000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
After --clear the test assumed strict FIFO, but acquisition shuffles the candidate head to spread contention (services/acquisition.py SHUFFLE_HEAD), so it sometimes gets the newer task. Assert the clear via the preference row instead. Found in TASK-6.3 CI.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 The clear is asserted deterministically (preference row gone) without relying on FIFO order
- [x] #2 Preferred-team acquires still asserted
<!-- AC:END -->
