---
id: TASK-6.5
title: >-
  M5.5 Observability: read-only views, release reason, Grafana dashboards,
  /metrics (FR-40, FR-41)
status: In Progress
assignee: []
created_date: '2026-10-17 03:33'
updated_date: '2026-10-17 08:42'
labels:
  - M5
dependencies:
  - TASK-6.1
parent_task_id: TASK-6
ordinal: 36000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Migration 0007: schema minion_ro with views (executor w/o token_hash, task w/o params, subtask w/o input keys, attempt, heartbeat); role minion_readonly NOLOGIN in a DO block (NOTICE+skip without CREATEROLE), USAGE+SELECT on minion_ro only. deploy/postgres initdb creates minion_grafana LOGIN IN ROLE minion_readonly, statement_timeout 10s; datasource switches to it. Dashboards (tasks + fleet) per ARCHITECTURE 7. Optional ReleaseRequest{code} stored in attempt.error; executor passes a code at each _release call site. /metrics via prometheus-client (route template, method, status, acquire outcome; no ids/teams); MINION_METRICS_ENABLED. Docs: SPEC 7, ARCHITECTURE 7.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 Every panel's SQL (macros substituted) runs as minion_readonly on the migrated DB (integration)
- [x] #2 As minion_readonly, selecting token_hash gives permission denied; static test: every panel queries minion_ro only
- [x] #3 A release with a code shows in the attempt view; a release without body still works; ownership/409 unchanged
- [x] #4 /metrics exposes counters with no ids, team names or payload
- [x] #5 Migration upgrade/downgrade test passes; live: compose shows both dashboards with data from an e2e run
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Deviation from plan: metrics shipped counters-only (HTTP requests by route template, acquire outcomes) and off by default (MINION_METRICS_ENABLED=false); no gauges/histograms. Fix round: online column in minion_ro.executor, role hardening (connection limit, temp_file_limit, TEMPORARY revoked), code-shaped error_code, release/fail code normalization in the executor.
<!-- SECTION:NOTES:END -->
