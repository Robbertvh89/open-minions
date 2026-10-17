---
id: TASK-23
title: Fix flaky test_complete_vs_reclaim_randomized coverage assertion
status: In Progress
assignee: []
created_date: '2026-10-17 14:14'
updated_date: '2026-10-17 14:15'
labels:
  - server
  - test
dependencies: []
ordinal: 48000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
The test runs a fixed 4 rounds and asserts both race orderings occurred (0 < complete_wins < n*rounds). On a slow CI runner complete lost every race (complete_wins == 0) although every per-round correctness check passed. Run rounds until both orderings are observed (min 4, max 16) and widen the reclaim delay window. Found in TASK-6.3 CI.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 Rounds continue until both orderings are seen (bounded), so the coverage check no longer depends on runner speed
- [x] #2 Per-round correctness assertions unchanged
<!-- AC:END -->
