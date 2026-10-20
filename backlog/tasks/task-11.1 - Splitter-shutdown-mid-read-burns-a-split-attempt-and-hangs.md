---
id: TASK-11.1
title: Splitter shutdown mid-read burns a split attempt and hangs
status: To Do
assignee: []
created_date: '2026-10-20 04:22'
updated_date: '2026-10-20 04:52'
labels:
  - server
  - splitter
dependencies: []
parent_task_id: TASK-11
ordinal: 96000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Found while planning TASK-11: cancelling the splitter during a to_thread object read turns CancelledError into ValueError('generator already executing'); _retry burns a split attempt and run_splitter swallows the cancel, so shutdown hangs and rolling restarts can drive a valid task to SPLIT_ATTEMPTS_EXHAUSTED. Fix together with TASK-11 (same read loop): shield the thread read, set the stop flag, wait <= 15 s for the thread, close the generator only after the thread is done.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 Cancel mid-read shuts down without hanging, writes no backoff and releases the lease; the next claim still counts an attempt (test)
- [ ] #2 Cancel mid-read shuts down without hanging, writes no backoff and releases the lease; the next claim still counts an attempt (test)
<!-- AC:END -->
