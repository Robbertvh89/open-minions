---
id: TASK-32
title: Pre-auth rate limiting or dedicated auth pool for executor endpoints
status: In Progress
assignee: []
created_date: '2026-10-18 06:57'
updated_date: '2026-10-18 18:19'
labels:
  - security
dependencies: []
ordinal: 71000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Security LOW from TASK-7.4.4: every executor request (incl. unauthenticated bearer floods and acquires that get shed) does the token lookup on a pooled connection before any limiter, so the connections kept back from acquires are not reserved for owner ops. Options: per-IP/global token bucket before current_executor; small dedicated auth pool; short-TTL token-hash lookup cache (with revocation latency bound).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Unauthenticated flood cannot make owner ops (heartbeat/complete) wait on the pool (test)
- [ ] #2 Revocation still takes effect within a documented bound
<!-- AC:END -->
