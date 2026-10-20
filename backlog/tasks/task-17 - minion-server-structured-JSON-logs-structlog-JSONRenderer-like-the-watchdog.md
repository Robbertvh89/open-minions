---
id: TASK-17
title: 'minion-server: structured JSON logs (structlog JSONRenderer) like the watchdog'
status: Done
assignee: []
created_date: '2026-10-17 10:23'
updated_date: '2026-10-20 00:57'
labels:
  - server
dependencies: []
ordinal: 44000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
The server never configures structlog, so it logs with the default console renderer instead of JSON (CLAUDE.md: structured JSON logs). The watchdog configures JSON (minion_watchdog/main.py). From TASK-6.6 security review.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 minion-server main() configures structlog JSON output (timestamps, level)
- [x] #2 No payload/secret fields in server logs (test)
<!-- AC:END -->
