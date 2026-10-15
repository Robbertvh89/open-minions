---
id: TASK-2.2
title: 'M1.2: Protocol endpoints: tasks, splitter, acquire, complete/fail/release'
status: To Do
assignee: []
created_date: '2026-10-15 14:26'
updated_date: '2026-10-15 14:53'
labels:
  - M1
dependencies:
  - TASK-2.1
parent_task_id: TASK-2
priority: high
ordinal: 10000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Plan steps B3, B4, B5 of TASK-2 (D5, D6). Decision: release does NOT consume the retry budget — max_attempts is compared against the count of FAILED+EXPIRED attempts, not the raw attempt counter.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 >=50 parallel acquires yield unique owners (service and HTTP level)
- [ ] #2 Two-pass acquire: preference first, then global FIFO; engine, memory and allowed_pool filters enforced
- [ ] #3 Complete promotes attempt-scoped output via CopyObject; stale or foreign acquirement gets 409
- [ ] #4 Splitter is resumable and slices on newline boundaries (unit + integration)
- [ ] #5 Release requeues without consuming retry budget
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Carried from TASK-2.1 review: (a) migration for TASK-2.2 must add minion_task.error column (TaskOut.error needs it; currently only minion_subtask.error exists). (b) Team-scope URI prefixes: allowed prefixes become {prefix}{team}/ per configured prefix, with separate input and output lists, and reject any key under outputs/_attempts/. (c) Register must not un-revoke a REVOKED executor; add a test for it.

Doc conflicts to resolve in this task (found during TASK-2.1 docs sync): (a) ARCHITECTURE §3.3 attempt key literal outputs/_attempts/{subtask_id}/{attempt}.jsonl vs plan D6 {output_prefix}/_attempts/{subtask_id:06d}/... — pick one, align doc+code; (b) ARCHITECTURE §3.1 says server validates 'S3 prefix readable' on task create — implement (list/head under input prefix) or drop from doc.
<!-- SECTION:NOTES:END -->
