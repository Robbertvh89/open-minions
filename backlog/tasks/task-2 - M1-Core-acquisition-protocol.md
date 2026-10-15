---
id: TASK-2
title: 'M1: Core acquisition protocol'
status: To Do
assignee: []
created_date: '2026-10-15 14:02'
updated_date: '2026-10-15 14:18'
labels:
  - milestone
  - M1
dependencies: []
priority: high
ordinal: 2000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Task create + async splitter, acquire/complete/fail endpoints, executor loop + worker subprocess with fake runner. See ARCHITECTURE §3.1-3.3.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Concurrent-acquire race test (>=50 parallel) yields unique owners
- [ ] #2 End-to-end: task submitted, split, executed by fake runner, outputs promoted in MinIO
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Security carry-forward from TASK-1 audit: (1) user/API-key table with hashed key + team mapping; Task.created_by → FK. (2) Executor tokens: rotation/expiry column, hmac.compare_digest, never logged. (3) Acquire enforces allowed_pool vs Executor.pool; every executor endpoint checks acquirement.executor_id == caller. (4) Attempt-scoped output keys; restrict input_uri/output_uri to configured bucket/prefixes (SSRF). (5) Grafana read-only DB role; no dev passwords in prod config. (6) Executor --server: require https except localhost; token in Keychain. (7) Prod S3 creds bucket-scoped, server-only.
<!-- SECTION:NOTES:END -->
