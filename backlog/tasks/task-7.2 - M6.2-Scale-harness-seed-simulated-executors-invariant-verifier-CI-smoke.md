---
id: TASK-7.2
title: 'M6.2 Scale harness: seed, simulated executors, invariant verifier, CI smoke'
status: Done
assignee: []
created_date: '2026-10-17 20:36'
updated_date: '2026-10-17 23:31'
labels:
  - scale
dependencies: []
parent_task_id: TASK-7
ordinal: 53000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Dev-only workspace package packages/minion-scale (console script minion-scale; not in the server image). seed: operator-side bulk INSERT...SELECT generate_series of N tasks x M PENDING subtasks (fake model from deploy/scale/models.scale.yaml, slices point at one small input object) + executor tokens (memory/0600 file, never printed). run: E async sims via minion_common ExecutorClient over the real REST API (no MLX, no DB): register, acquire, work sleep, heartbeat, PUT tiny output to presigned URL, complete; executor-equivalent back-off (204 jitter; 429 Retry-After). Faults: vanish, release, retryable fail, late complete (expect 409). --procs; event-loop lag recorded. Collect per-call latency histograms, throughput, 409/429/5xx, conflict rounds; pg_stat_statements, pg_stat_user_tables, connections; docker stats. explain: EXPLAIN (ANALYZE, BUFFERS) of candidate/stale/over-lease queries. verify: NF-2 (exactly one SUCCEEDED attempt per succeeded subtask; final-object count == succeeded), NF-3 (all terminal, no orphan acquirements), task counters == actual. deploy/compose.scale.yml override. Server-side minion_acquire_seconds histogram (outcome label only).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 Sims use only the REST API and presigned URLs (invariants 1, 4); test: minion_scale.sim imports no sqlalchemy/asyncpg
- [x] #2 Back-off/429 handling matches the executor (unit test against executor constants)
- [x] #3 verify detects a seeded double-success and an orphan acquirement
- [x] #4 Integration smoke (CI): 20 sims, 2k subtasks, faults on, < 90 s, verify passes, zero 5xx
- [x] #5 JSON report schema documented; no tokens or payload in output
<!-- AC:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Merged #42: dev-only minion-scale harness (seed/run/verify/explain/report/cleanup), REST-only sims with executor-parity back-off and faults, fail-fast drained flag + stall detector, NF-2/NF-3 snapshot verifier, isolated compose.scale overlay, minion_acquire_seconds histogram, non-dev DB guard. CI run 39077098028: 1816 passed. Smoke sizing for slow runners: TASK-28.
<!-- SECTION:FINAL_SUMMARY:END -->
