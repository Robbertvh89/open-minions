---
id: TASK-7.4.3
title: >-
  M6.4c Head contention: single-statement claim over a FIFO window + executor
  first-acquire jitter
status: To Do
assignee: []
created_date: '2026-10-18 02:21'
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
- [ ] #1 Invariant 2 unchanged: ownership only via the acquirement INSERT ON CONFLICT DO NOTHING
- [ ] #2 200 concurrent acquires on 64 pending: 64 unique owners; no priority inversion
- [ ] #3 INSERT attempts per win and candidate reads per acquire before/after (B-20k, A stress) in PR
- [ ] #4 ADR-0013 and SPEC FR-22 updated; ARCHITECTURE ADR-0002 reference fixed
<!-- AC:END -->
