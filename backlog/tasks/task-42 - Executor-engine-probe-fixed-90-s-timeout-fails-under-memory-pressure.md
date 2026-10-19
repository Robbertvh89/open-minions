---
id: TASK-42
title: 'Executor engine probe: fixed 90 s timeout fails under memory pressure'
status: To Do
assignee: []
created_date: '2026-10-19 14:11'
labels:
  - bug
dependencies: []
ordinal: 82000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
During TASK-38 verification minion-executor run failed repeatedly with 'error: worker timed out' while macOS swapped (memory pressure warn): PROBE_TIMEOUT_S=90 in main.py is fixed. Options: make it configurable (MINION_EXECUTOR_PROBE_TIMEOUT_S), retry once, or wait for memory pressure to clear before probing; clearer error text pointing at memory pressure. Also investigate a pytest process that reached ~101 GB RSS for >2 h in the main checkout during parallel runs (which test?).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Probe survives a slow worker start (configurable or adaptive timeout, tested)
- [ ] #2 Error message names the likely cause
- [ ] #3 Large-RSS pytest source identified or ruled out
<!-- AC:END -->
