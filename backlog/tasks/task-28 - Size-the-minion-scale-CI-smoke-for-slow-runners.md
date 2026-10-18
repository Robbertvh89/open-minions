---
id: TASK-28
title: Size the minion-scale CI smoke for slow runners
status: Done
assignee: []
created_date: '2026-10-17 23:31'
updated_date: '2026-10-18 00:46'
labels:
  - test
dependencies: []
ordinal: 59000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
The integration smoke (20 sims, 2k subtasks, 90 s budget) failed twice on GitHub runners that were ~5x slower at acquire (p50 500 ms vs 100 ms locally; ~21 subtasks/s; runs 39075463773, 39078078923), while passing in ~25 s locally. Reduce the smoke workload (e.g. 1000 subtasks) so it fits slow runners with margin, keep every correctness assertion (drained, verify NF-2/NF-3, zero 5xx/transport, no secrets in output) and the 90 s budget. Record the CI acquire latency observation for TASK-7.3.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 Smoke workload sized for CI with margin; all correctness assertions unchanged
- [x] #2 Smoke green on CI in 3 consecutive runs
<!-- AC:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Merged #44: smoke sized to 1000 subtasks; CI 3/3 green (run 39079700012).
<!-- SECTION:FINAL_SUMMARY:END -->
