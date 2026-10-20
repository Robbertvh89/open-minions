---
id: TASK-51
title: Make executor token_hash required end to end (mypy-enforced)
status: In Progress
assignee: []
created_date: '2026-10-20 00:14'
updated_date: '2026-10-20 01:00'
labels:
  - security
  - server
dependencies: []
ordinal: 91000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Follow-up from TASK-41 review/security (LOW): ExecutorPrincipal.token_hash is str | None and register/touch/report_state/token_match skip the hash check on None. No production path produces None (auth.py digest, preload from NOT NULL column), but it relies on two construction sites. Make ExecutorPrincipal.token_hash: str (repr=False), token_match/register/touch/report_state token_hash: str required, ExecutorRevokedError.token_hash: str; remove the dead None guards (services/acquisition.py, app.py revoked handler); update test callers (use an explicitly named helper for test/internal calls that need no check).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 mypy enforces non-None token_hash on every executor mutation
- [ ] #2 No 'is None' token_hash guards remain in server src
- [ ] #3 Server tests pass
<!-- AC:END -->
