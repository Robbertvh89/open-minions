---
id: TASK-6.3
title: 'M5.3 Presigned scope: Range-signed GET and output size cap (TASK-9 #2)'
status: Done
assignee: []
created_date: '2026-10-17 03:33'
updated_date: '2026-10-17 14:32'
labels:
  - M5
dependencies: []
parent_task_id: TASK-6
ordinal: 34000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
storage.py presign_get(key, ttl, byte_range) signs Range=bytes=s-e; services/acquisition.py passes the subtask range (executor already sends the exact header, io.py:31). services/completion.py: attempt object above MINION_MAX_OUTPUT_BYTES (4 GiB) -> 422 OUTPUT_TOO_LARGE, never promoted. Executor refuses upload above the same cap (fail OUTPUT_TOO_LARGE, non-retryable). Document S3 lifecycle rule on _attempts/. PUT content-length-range stays deferred. Docs: SPEC S-2, ARCHITECTURE 3.3.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 Integration (SeaweedFS): signed-range GET gives 206; no/other Range gives 403, or the PR records SeaweedFS does not enforce it
- [x] #2 Existing e2e passes unchanged
- [x] #3 Oversize attempt object never promoted (422); retried then FAILED after max_attempts
- [x] #4 Executor refuses an oversize upload with OUTPUT_TOO_LARGE
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
CI run 39040593000: SeaweedFS enforces the signed Range (no warning). All ACs verified in CI integration.
<!-- SECTION:NOTES:END -->
