---
id: TASK-56
title: 'Splitter: count split failures separately from claims'
status: In Progress
assignee: []
created_date: '2026-10-20 04:46'
updated_date: '2026-10-20 05:27'
labels:
  - server
  - splitter
dependencies: []
ordinal: 98000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
From TASK-11 review: claim_splitting bumps split_attempts on every claim and the exhaustion check uses it, so each rolling restart during a long split still consumes one of the 5 attempts (SPLIT_ATTEMPTS_EXHAUSTED on a valid task after enough restarts). split_attempts is also the lease fencing token, so it must not be decremented (ABA). Add a separate failure counter (new migration) incremented only on real split failures (read/parse/S3 errors, deadline), and base exhaustion on it.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 N restarts during one long split never exhaust a valid task (test)
- [x] #2 Fencing token semantics unchanged; migration up/down tested
<!-- AC:END -->
