---
id: TASK-40
title: Fix minion-scale test_sims_drain_a_queue_over_rest_only conflicts_409 flake
status: To Do
assignee: []
created_date: '2026-10-18 21:55'
labels:
  - test
  - scale
dependencies: []
ordinal: 80000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Reviewer of TASK-33 saw packages/minion-scale/tests/test_sim.py::test_sims_drain_a_queue_over_rest_only fail with conflicts_409 == 1..2 (3/3 in their runs, also on main), while full local suites passed. Likely late/stop-time release 409 (cf. M6.4 B-100k 12 release 409s) counted by an assertion that expects 0. Reproduce, root-cause, fix assertion or sim.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Root cause stated
- [ ] #2 Passes 20/20
<!-- AC:END -->
