---
id: TASK-33
title: Per-executor limit on open acquirements
status: In Progress
assignee: []
created_date: '2026-10-18 08:03'
updated_date: '2026-10-18 18:19'
labels:
  - security
dependencies: []
ordinal: 72000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Security LOW from TASK-7.4.5: a rogue executor with a valid token can acquire subtasks and never ack, holding each up to cap x heartbeat_timeout (~2.5 h at defaults) before budget is used; acked executors can already hold until max_lease_s. Add a per-executor cap on concurrently open acquirements (server-side, checked in the claim path) and/or count free unacked expiries per executor; token revocation stays the primary response.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Executor with N open acquirements gets 429/409 on further acquires (configurable N, default sized for normal executors)
- [ ] #2 Test: hoarding executor cannot hold more than N subtasks
<!-- AC:END -->
