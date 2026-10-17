---
id: TASK-10
title: Per-team allowed/default executor pools (SPEC S-1)
status: Done
assignee: []
created_date: '2026-10-15 15:44'
updated_date: '2026-10-17 06:51'
labels:
  - security
  - M5
dependencies: []
ordinal: 13000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
From TASK-2.2 security audit (LOW): allowed_pool defaults to NULL = any pool and there is no team→permitted-pools mapping, so confidential team data can go to any pool's laptops unless the submitter opts in. Add per-team default and allowed pools (API keys file or M5 auth model); task create applies default and rejects pools outside the allowed set.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Task without allowed_pool gets the team's default pool
- [ ] #2 Task with a pool outside the team's allowed set is rejected with 403/422
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Absorbed by TASK-6.1 (per-team pools: default_pool/allowed_pools in the keys file, PR #24).
<!-- SECTION:NOTES:END -->
