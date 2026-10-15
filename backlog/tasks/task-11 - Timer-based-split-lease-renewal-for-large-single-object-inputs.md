---
id: TASK-11
title: Timer-based split lease renewal for large single-object inputs
status: To Do
assignee: []
created_date: '2026-10-15 16:51'
labels:
  - M2
dependencies: []
ordinal: 14000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
From TASK-2.2 review (MINOR): split lease is renewed and wall-clock budget checked only per 1000-subtask batch. Listing plus one large batch (single object up to 50 GiB) can exceed lease_s (900s); with >1 server replica another splitter claims, first gets LeaseLostError, repeats can end a valid task as SPLIT_ATTEMPTS_EXHAUSTED. Renew + check deadline on a timer (e.g. every lease_s/3) inside the read loop (smaller to_thread reads or background renew task). Also: note in ARCHITECTURE that If-Match must be honoured by the S3 backend (same-size replacement otherwise undetected) or compare GET ETag to listed ETag.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Lease renewed at least every lease_s/3 during a single long object read
- [ ] #2 Test: lease_s shorter than one batch's read time still completes with 2 concurrent splitters
- [ ] #3 GET response ETag compared to listed ETag
<!-- AC:END -->
