---
id: TASK-19
title: >-
  Presigned POST with content-length-range for output uploads (upload-time size
  cap)
status: In Progress
assignee: []
created_date: '2026-10-17 12:59'
updated_date: '2026-10-18 18:19'
labels:
  - server
  - executor
  - security
dependencies: []
ordinal: 46000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Presigned PUT URLs accept any size until TTL; switch outputs to presigned POST with content-length-range so S3 rejects oversize uploads. Wire-contract change for executors. From TASK-6.3 security review.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Output uploads above the cap are rejected by S3
- [ ] #2 Executors handle the POST form upload; back-compat plan documented
<!-- AC:END -->
