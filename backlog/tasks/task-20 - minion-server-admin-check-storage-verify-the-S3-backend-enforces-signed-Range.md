---
id: TASK-20
title: 'minion-server-admin check-storage: verify the S3 backend enforces signed Range'
status: To Do
assignee: []
created_date: '2026-10-17 13:16'
labels:
  - server
  - security
dependencies: []
ordinal: 47000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Input GET URLs are slice-scoped only if the S3 backend enforces SigV4 signed headers (AWS S3, MinIO). Add an operator preflight that runs the signed-Range probe (other/no Range must be 403; If-Range must not widen the body) against the production bucket and exits non-zero if it fails; reference it in the deploy docs. Also: move the best-effort oversize delete in services/completion.py out of the row-locked transaction. From TASK-6.3 security re-audit.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 check-storage exits 0 on an enforcing backend and non-zero (with the observed tuple) on a non-enforcing one
- [ ] #2 Deploy docs require running it before production
- [ ] #3 Oversize delete runs after the transaction
<!-- AC:END -->
