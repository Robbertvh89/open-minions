---
id: TASK-52
title: 'Server: log ResponseValidationError without input values'
status: To Do
assignee: []
created_date: '2026-10-20 00:15'
labels:
  - security
  - server
dependencies: []
ordinal: 92000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
From TASK-17 security audit (LOW, pre-existing): an unhandled FastAPI ResponseValidationError message includes the offending input values, which for an acquire response could be presigned URLs; uvicorn logs the exception message. Add an app-level handler that logs only the exception class and error locations (loc/type), returns a generic 500.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 ResponseValidationError logged with class + loc/type only; test with a presigned-URL-like value asserts it is absent
- [ ] #2 Client gets a generic 500 body
<!-- AC:END -->
