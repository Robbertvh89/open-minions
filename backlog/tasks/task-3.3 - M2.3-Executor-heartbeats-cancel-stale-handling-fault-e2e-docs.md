---
id: TASK-3.3
title: 'M2.3: Executor heartbeats, cancel/stale handling, fault e2e, docs'
status: To Do
assignee: []
created_date: '2026-10-15 18:02'
updated_date: '2026-10-15 18:32'
labels:
  - M2
dependencies:
  - TASK-3.1
parent_task_id: TASK-3
priority: high
ordinal: 17000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Plan steps S6-S7 of TASK-3 (D6, D8). Executor kills worker via WorkerHandle.interrupt() (respawnable) on heartbeat cancel/409.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Killed executor's subtask is re-run by another executor (e2e)
- [ ] #2 Executor stops work on heartbeat cancel/409 and keeps polling
- [ ] #3 ADR-0008 written; SPEC/ARCH match code
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
From TASK-3.1 review (must land before closing M2): ARCHITECTURE §2.3 and §3.4 pseudocode still describe DELETE...RETURNING + UPSERT heartbeat — update to SKIP LOCKED + re-check + executor-row update; add §4 lock-order note (acquirement → heartbeat → subtask → task; executor row after acquirement); ADR-0008 for the reclaim deviation from the paper.

Test hygiene: test_heartbeat.py::test_heartbeat_updates_state_and_refreshes_url times out on raw httpx PUT to presigned URL when run in isolation (passes in full suite; predates TASK-3.1 polish). Investigate fixture ordering / public endpoint when touching e2e.
<!-- SECTION:NOTES:END -->
