---
id: TASK-7.4.1
title: >-
  M6.4a Harness parity: sim complete retry, 503 Retry-After, first-acquire
  jitter, cheap progress poll
status: Done
assignee: []
created_date: '2026-10-18 02:20'
updated_date: '2026-10-18 03:23'
labels:
  - scale
dependencies: []
parent_task_id: TASK-7.4
ordinal: 60000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Before the other M6.4 fixes, so before/after numbers are honest. sim.py:410 gives up after one failed complete vs loop.py:1158 retries; cli.py polls count(*) at 1 Hz. Add sim complete retry parity, 503 Retry-After handling for acquire/complete, --first-acquire-jitter-s (default 0), --poll-s (default 5) using a counter sum.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 Sim retries complete on transport/5xx like the executor; same-owner repeat counted once
- [x] #2 Sim honours Retry-After on 503 for acquire and complete
- [x] #3 --first-acquire-jitter-s and --poll-s (default 5, counter sum) exist
- [x] #4 knee-10 and B-20k re-run numbers in the PR
<!-- AC:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Merged #47: sim complete retry/give-up parity with the executor, 503 Retry-After, first-acquire jitter, counter-sum progress poll; knee-10 and B-20k baselines unchanged (server-bound). CI run 39092769947: 1917 passed.
<!-- SECTION:FINAL_SUMMARY:END -->
