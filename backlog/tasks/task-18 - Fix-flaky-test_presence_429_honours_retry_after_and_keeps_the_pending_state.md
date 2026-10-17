---
id: TASK-18
title: Fix flaky test_presence_429_honours_retry_after_and_keeps_the_pending_state
status: Done
assignee: []
created_date: '2026-10-17 12:47'
updated_date: '2026-10-17 12:57'
labels:
  - executor
  - test
dependencies: []
ordinal: 45000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
The test advances the fake clock 35 s past a 30 s Retry-After, but _rate_limit_delay adds 0-20% jitter (block until 1030..1036), so ~17% of runs time out. Found in the TASK-6.3 review.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 Test advances the fake clock past the maximum jittered block (deterministic)
- [x] #2 Test passes 30 consecutive isolated runs
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Last fake-clock step raised from 25 s to 31 s so the clock passes the maximum jittered block (1036 s). 30/30 isolated runs pass.
<!-- SECTION:NOTES:END -->
