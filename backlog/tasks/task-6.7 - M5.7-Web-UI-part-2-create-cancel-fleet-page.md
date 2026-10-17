---
id: TASK-6.7
title: 'M5.7 Web UI part 2: create, cancel, fleet page'
status: Done
assignee: []
created_date: '2026-10-17 03:33'
updated_date: '2026-10-17 11:46'
labels:
  - M5
dependencies:
  - TASK-6.6
parent_task_id: TASK-6
ordinal: 38000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Move create_task/cancel_task logic from api/tasks.py into services/tasks.py (thin handlers); API and UI share it. UI create form (TaskCreate fields, params JSON via parse_params, same error codes), cancel with confirm + CSRF, operator-only fleet page (per-executor online/busy/profile/budget/paused_reason; memory by profile; paused by reason).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 Creating a task in the UI gives the same row and validation as POST /tasks (422 codes shown, 409 TASK_EXISTS)
- [x] #2 Cancel in the UI follows FR-8 (idempotent; 409 TASK_FINISHED)
- [x] #3 Fleet page 403 for non-operators
- [x] #4 Existing task API tests stay green after the service extraction
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Integration evidence from CI (PR #28, run 39029375327): test_ui_actions (create parity, error codes, escaping, FR-8 cancel, CSRF/Origin, operator-only fleet) and test_read_api budget/summary all PASSED; 38 task API tests green. Local docker was wedged during this task; implementer also ran the UI integration tests against local Postgres + moto and a headless-Chrome flow.
<!-- SECTION:NOTES:END -->
