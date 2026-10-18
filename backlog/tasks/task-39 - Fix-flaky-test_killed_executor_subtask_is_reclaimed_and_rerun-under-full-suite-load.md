---
id: TASK-39
title: >-
  Fix flaky test_killed_executor_subtask_is_reclaimed_and_rerun under full-suite
  load
status: To Do
assignee: []
created_date: '2026-10-18 20:49'
labels:
  - test
dependencies: []
ordinal: 78000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Failed twice in full local runs (TASK-19, TASK-32): once unspecified, once reclaim reason NOT_ACKNOWLEDGED instead of HEARTBEAT_TIMEOUT (ack heartbeat raced the kill, ADR-0014). Passes in isolation. Make the test wait for the ack before killing, or accept either reason with a justified assertion.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Root cause stated
- [ ] #2 Test passes 20/20 under parallel load
<!-- AC:END -->
