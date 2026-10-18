---
id: TASK-7.4.5
title: >-
  M6.4e Retry budget: unacknowledged EXPIRED attempts do not consume
  max_attempts (capped)
status: Done
assignee: []
created_date: '2026-10-18 02:21'
updated_date: '2026-10-18 08:03'
labels:
  - scale
dependencies: []
parent_task_id: TASK-7.4
ordinal: 64000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Executor acks with an immediate heartbeat before worker start; reclaim detects last_seen_at = acquired_at under the lock -> EXPIRED with NOT_ACKNOWLEDGED, not budget-counted until MINION_RELEASE_LIMIT is reached (bounded, no livelock). SPEC FR-31/FR-32 + ADR-0014. No schema change. Document old executors without ack.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 Never-beaten reclaim requeues without consuming budget; beaten reclaim consumes
- [x] #2 Free unacked expiries are capped (no livelock)
- [x] #3 Executor sends the ack before worker start; 429/5xx non-fatal
- [x] #4 SPEC FR-31/32 and ADR-0014 merged in the same PR
<!-- AC:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Merged #51: executor ack heartbeat after acquire (409 drop, CANCEL release, retry delay kept); never-acknowledged reclaim = EXPIRED NOT_ACKNOWLEDGED, free up to MINION_RELEASE_LIMIT per subtask (watchdog reads it, 0..100); first beat exempt from min interval; ADR-0014 + SPEC FR-31/32. CI run 39109083184: 2014 passed.
<!-- SECTION:FINAL_SUMMARY:END -->
