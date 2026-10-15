---
id: TASK-3.1
title: 'M2.1: Heartbeat endpoint, watchdog reclaim service, release cap'
status: In Progress
assignee: []
created_date: '2026-10-15 18:02'
updated_date: '2026-10-15 18:02'
labels:
  - M2
dependencies: []
parent_task_id: TASK-3
priority: high
ordinal: 15000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Plan steps S1-S3 of TASK-3 (D1-D3, D5)
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Complete-vs-reclaim race test passes (randomized + both deterministic orderings)
- [ ] #2 Heartbeat-vs-reclaim and two-watchdog races safe; stale 409 on heartbeat/complete/fail/release
- [ ] #3 Budget = FAILED+EXPIRED, release free up to MINION_RELEASE_LIMIT
<!-- AC:END -->
