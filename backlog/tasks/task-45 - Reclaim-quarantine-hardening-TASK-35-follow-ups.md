---
id: TASK-45
title: Reclaim quarantine hardening (TASK-35 follow-ups)
status: Done
assignee: []
created_date: '2026-10-19 20:35'
updated_date: '2026-10-19 23:39'
labels:
  - watchdog
  - reliability
dependencies: []
ordinal: 85000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
TASK-35 security LOWs + review NIT: (1) services/reclaim.py:95-107 any non-transient error (code bug AttributeError/TypeError, XX000, 25P02) quarantines up to 1000 healthy rows for 600 s; quarantine only when isolated (another row of the same chunk succeeded row-by-row), else systemic -> retry next tick. (2) :216 full quarantine (1000) stops excluding; evict soonest-expiring + count reclaim_quarantine_full. (3) :217 quarantining is silent; log reclaim_quarantined (acquirement_id, until) + quarantine size gauge. (4) test_reclaim_races.py:1174 SQLSTATE test uses a stub orig; add one real-DB case (lock_timeout + held row lock -> 55P03 not quarantined).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 Systemic failure (every row of a chunk fails) quarantines nothing
- [x] #2 Full quarantine evicts soonest-expiring and is counted
- [x] #3 Quarantine entry logged without payloads; size observable
- [x] #4 Real-DB 55P03 test passes
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Implemented in services/reclaim.py: quarantine only when a peer row of the chunk committed an expiry, else reclaim_chunk_systemic (error) and retry next tick; full quarantine evicts soonest-expiring (reclaim_quarantine_full, evicted_total); logs reclaim_quarantined (acquirement_id, until, error_type) + per-call reclaim_quarantine summary (size observable via structlog only; watchdog has no /metrics). Tests in test_reclaim_races.py incl. real-DB 55P03 (lock_timeout + held row lock). ADR-0008 + ARCHITECTURE updated.
<!-- SECTION:NOTES:END -->
