---
id: TASK-9
title: Security hardening follow-ups from TASK-2.1 audit
status: To Do
assignee: []
created_date: '2026-10-15 14:41'
updated_date: '2026-10-15 22:49'
labels:
  - security
dependencies: []
ordinal: 12000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Deferred MEDIUM/LOW findings from TASK-2.1 security audit: (1) pin HF model revision (commit SHA) in ModelEntry + models.yaml, pass to loaders (SPEC S-7); (2) presigned GET scoped to subtask byte range (sign Range header) and PUT size cap; (3) allow-list task params keys per task type + size cap (block trust_remote_code etc.).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 ModelEntry requires revision SHA and runners load by revision
- [ ] #2 Presigned GET signs Range; oversize PUT rejected
- [ ] #3 Unknown task params rejected with 422
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Items #1 (revision pin) and #3 (params allow-list) delivered by TASK-4.1; #2 (Range-signed GET, PUT size cap) remains.

Follow-ups from TASK-4.1 review (not yet implemented): (a) absolute per-batch wall-clock cap, and count a step tick toward liveness only when records_done increases, so a stuck batch cannot be kept alive by ticks; (b) server-side cap on records_per_subtask x max_tokens for completions (reject or split before acquire).
<!-- SECTION:NOTES:END -->
