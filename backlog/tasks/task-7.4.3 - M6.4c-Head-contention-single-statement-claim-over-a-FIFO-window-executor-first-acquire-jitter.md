---
id: TASK-7.4.3
title: >-
  M6.4c Head contention: single-statement claim over a FIFO window + executor
  first-acquire jitter
status: Done
assignee: []
created_date: '2026-10-18 02:21'
updated_date: '2026-10-18 05:20'
labels:
  - scale
dependencies: []
parent_task_id: TASK-7.4
ordinal: 62000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Fused CTE: candidates (b) LIMIT window, random pick within the max priority present, INSERT ... ON CONFLICT DO NOTHING RETURNING; re-check UPDATE, heartbeat insert and task update kept. MINION_ACQUIRE_WINDOW (16, 1..256). ADR-0013 + SPEC FR-22 note (random within top window of highest priority; never priority inversion). MINION_EXECUTOR_FIRST_ACQUIRE_JITTER_S (15, 0..300). Also resolve ARCHITECTURE 3.2 citing a non-existent ADR-0002 (SKIP LOCKED): record the decision not to use it.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 Invariant 2 unchanged: ownership only via the acquirement INSERT ON CONFLICT DO NOTHING
- [x] #2 200 concurrent acquires on 64 pending: 64 unique owners; no priority inversion
- [x] #3 INSERT attempts per win and candidate reads per acquire before/after (B-20k, A stress) in PR
- [x] #4 ADR-0013 and SPEC FR-22 updated; ARCHITECTURE ADR-0002 reference fixed
<!-- AC:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Merged #49: single-statement claim (WITH cand/pick/ins, ON CONFLICT DO NOTHING; RETURNING key check; re-check UPDATE kept), FIFO window (ADR-0013), executor first-acquire jitter. Claim statements per win 1.28 (was 19.4); B-20k 56/s (was 30); A stress 65/s (was 2). CI run 39099406160: 1950 passed.
<!-- SECTION:FINAL_SUMMARY:END -->
