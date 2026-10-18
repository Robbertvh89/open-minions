---
id: TASK-35
title: >-
  Investigate watchdog reclaim tick slowdown (9 s per 500 stale, was ~2 s in
  M6.3)
status: To Do
assignee: []
created_date: '2026-10-18 11:55'
labels:
  - perf
  - scale
dependencies: []
ordinal: 74000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
TASK-7.4.7 scenario C: tick 8.8-9.3 s for 500-705 stale rows; DB execution ~0.4 s, rest host round trips. Probably per-row statements (reclaim_stale loop, settle_stuck). Profile and batch.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Root cause identified with numbers
- [ ] #2 Tick for 500 stale back to <= M6.3 (2.4 s) or ADR
<!-- AC:END -->
