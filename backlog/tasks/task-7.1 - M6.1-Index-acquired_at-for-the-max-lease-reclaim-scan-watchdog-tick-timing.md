---
id: TASK-7.1
title: M6.1 Index acquired_at for the max-lease reclaim scan + watchdog tick timing
status: Done
assignee: []
created_date: '2026-10-17 20:36'
updated_date: '2026-10-17 21:10'
labels:
  - server
dependencies: []
parent_task_id: TASK-7
ordinal: 52000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Migration 0008: ix_minion_subtask_acquirement_acquired_at (plain create_index; table holds only running work) + matching Index in db/models.py SubtaskAcquirement.__table_args__. Watchdog logs tick duration_ms in the watchdog_reclaim event. Integration test seeds ~20k acquirements via generate_series, ANALYZE, asserts EXPLAIN of over_lease_ids uses the index. Update ADR-0008 consequence, ARCHITECTURE 5.1.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 0008 upgrade/downgrade round-trips; test_migrated_schema_matches_models passes
- [x] #2 EXPLAIN of the over-lease scan uses the new index on a seeded table (integration test)
- [x] #3 watchdog_reclaim log carries duration_ms; no payload/ids added
- [x] #4 ADR-0008 and ARCHITECTURE 5.1 updated
<!-- AC:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Merged #40: migration 0008 acquired_at index (lock_timeout 5s), over_lease_stmt builder, watchdog_tick duration_ms/rounds + over_lease_scan scan_ms, ADR-0008/ARCHITECTURE updated.
<!-- SECTION:FINAL_SUMMARY:END -->
