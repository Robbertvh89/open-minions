---
id: TASK-49
title: 'perf(server): reduce S3 promote cost at complete'
status: To Do
assignee: []
created_date: '2026-10-19 23:03'
updated_date: '2026-10-20 04:41'
labels:
  - perf
  - scale
dependencies: []
ordinal: 89000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Follow-up from TASK-36 profile (docs/bench/m-task36-server-cpu-apple-m3-pro.md).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 presign/boto self-share < 3 % at W=8 S-sat
- [ ] #2 invariant 5 (idempotent outputs) kept, tested
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
2026-10-20 DEFERRED after planning (plan: scratchpad task49-plan.md; micro-benchmarks scratchpad bench49/). Cost is boto client machinery (HEAD+copy ~780 µs, two presigns ~150 µs client CPU per subtask). Best option C (direct botocore SigV4 signing + URLLib3Session, same protocol/If-Match/HEAD) gives ~-60 % of that, i.e. only ~3-4 % lower server CPU per subtask, at the price of owning security-sensitive S3 request signing. Re-evaluate after TASK-46/47 (SQLAlchemy statement building is 21-26 %); rerun the TASK-36 baseline first.
<!-- SECTION:NOTES:END -->
