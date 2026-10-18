---
id: TASK-7.4.5
title: >-
  M6.4e Retry budget: unacknowledged EXPIRED attempts do not consume
  max_attempts (capped)
status: In Progress
assignee: []
created_date: '2026-10-18 02:21'
updated_date: '2026-10-18 06:57'
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
- [ ] #1 Never-beaten reclaim requeues without consuming budget; beaten reclaim consumes
- [ ] #2 Free unacked expiries are capped (no livelock)
- [ ] #3 Executor sends the ack before worker start; 429/5xx non-fatal
- [ ] #4 SPEC FR-31/32 and ADR-0014 merged in the same PR
<!-- AC:END -->
