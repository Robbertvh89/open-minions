---
id: TASK-7.3
title: M6.3 Run the scale test on this Mac and report NF-1..NF-3
status: In Progress
assignee: []
created_date: '2026-10-17 20:36'
updated_date: '2026-10-18 00:46'
labels:
  - scale
dependencies: []
parent_task_id: TASK-7
ordinal: 54000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Scenarios: A) NF-1: 1M PENDING (>=10 tasks), 500 sims, stress (work 1-3 s) and realistic (30-120 s), 10 min each; B) drain + chaos: 100k subtasks, vanish/release/late-complete -> verify NF-2/NF-3; C) watchdog tick on 500 stale acquirements with 1M rows, with/without 0008; D) rate limits: 429 + Retry-After, no DB writes. Results docs/bench/m6-scale-apple-m3-pro.{md,json} with hardware, Docker VM, PG config, versions, explicit limits/extrapolation (co-location, loopback, single replica, no TLS, short duration, 1M drain not run). NF-1 failures become M6.4 items.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Report gives acquire p50/p95/p99 (client + server/pg_stat_statements) at 1M pending; pass/fail vs NF-1 stated
- [ ] #2 Scenario B verify passes (zero double-completion, zero lost subtasks) or failures filed
- [ ] #3 Watchdog tick time and EXPLAIN plans recorded
- [ ] #4 Limits/extrapolation section present; SPEC NF-1..NF-3 rows link the report
<!-- AC:END -->
