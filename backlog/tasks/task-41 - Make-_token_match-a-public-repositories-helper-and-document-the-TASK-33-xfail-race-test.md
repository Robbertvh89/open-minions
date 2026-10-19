---
id: TASK-41
title: >-
  Make _token_match a public repositories helper and document the TASK-33 xfail
  race test
status: To Do
assignee: []
created_date: '2026-10-19 06:58'
labels:
  - chore
dependencies: []
ordinal: 81000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
From PR #57 review NITs: repositories/acquisition.py imports private _token_match from repositories/executors.py; rename to public helper. Add docstring to test_acquire_cap.py overshoot xfail test noting the three-workers test is the real proof. Security note: lock helper skips hash check when token_hash is None; make the parameter required.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 token_match public, used by both repositories
- [ ] #2 token_hash required in lock_executor_for_claim
- [ ] #3 xfail test docstring explains its role
<!-- AC:END -->
