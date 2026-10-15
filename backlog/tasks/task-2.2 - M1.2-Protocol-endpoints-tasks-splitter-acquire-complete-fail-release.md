---
id: TASK-2.2
title: 'M1.2: Protocol endpoints: tasks, splitter, acquire, complete/fail/release'
status: In Progress
assignee: []
created_date: '2026-10-15 14:26'
updated_date: '2026-10-15 16:46'
labels:
  - M1
dependencies:
  - TASK-2.1
parent_task_id: TASK-2
priority: high
ordinal: 10000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Plan steps B3, B4, B5 of TASK-2 (D5, D6). Decision: release does NOT consume the retry budget — max_attempts is compared against the count of FAILED+EXPIRED attempts, not the raw attempt counter.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 >=50 parallel acquires yield unique owners (service and HTTP level)
- [x] #2 Two-pass acquire: preference first, then global FIFO; engine, memory and allowed_pool filters enforced
- [x] #3 Complete promotes attempt-scoped output via CopyObject; stale or foreign acquirement gets 409
- [x] #4 Splitter is resumable and slices on newline boundaries (unit + integration)
- [x] #5 Release requeues without consuming retry budget
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Implemented: POST /api/v1/tasks, GET /tasks/{team}/{project}/{task_name}; executor register, preference, acquire (two-pass, allowed_pool), complete/fail/release. Migrations 0003 (minion_task.error), 0004 (split_attempts, next_split_at, unique output_uri). Key files: api/tasks.py, api/executor.py, services/splitter.py, services/acquisition.py, services/completion.py, repositories/tasks.py, repositories/acquisition.py, limits.py (body cap), uris.py (canonical URIs), storage.py (CopySourceIfMatch). Deviations: splitter claim uses FOR NO KEY UPDATE SKIP LOCKED; acquire retries up to MAX_ROUNDS=10 when all candidates lose races; complete is idempotent for the same owner (204 on retry); tests use a fresh_url fixture; per-team default pools deferred to TASK-10. Docs: SPEC §6/§6.2 and ARCHITECTURE §3.1-3.3 synced; heartbeat, presence, list, subtasks, cancel and /executors not implemented yet.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
TASK-2.2 delivers task API, resumable splitter, two-pass acquire and complete/fail/release with fencing and retry-budget-aware release. Docs synced (SPEC §6, ARCHITECTURE §3), .env.example lists every MINION_* setting. Open: heartbeat/presence and the remaining task endpoints.
<!-- SECTION:FINAL_SUMMARY:END -->
