---
id: TASK-42
title: 'Executor engine probe: fixed 90 s timeout fails under memory pressure'
status: Done
assignee: []
created_date: '2026-10-19 14:11'
updated_date: '2026-10-19 18:58'
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
- [x] #1 Probe survives a slow worker start (configurable or adaptive timeout, tested)
- [x] #2 Error message names the likely cause
- [ ] #3 Large-RSS pytest source identified or ruled out
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Probe timeout now MINION_EXECUTOR_PROBE_TIMEOUT_S (default 90, 10..900), doubled, capped at 300 s, never below the set value, under warn/critical memory pressure (logged engine_probe_timeout_extended), timeout error names memory pressure + knob; shared by service install. RSS: no repro (mlx extra not installed in worktree, gpu tests skipped). Prime suspect by inspection: test_gpu_guard_gpu.py::test_burn_hook_saturates_the_gpu runs fake._gpu_burn in a thread of the pytest process with no mx memory/cache limit, plus module-scoped in-process MLX runner fixtures in test_mlx_runners_gpu.py; fix proposed as follow-up (run burn in a subprocess / set mx limits).

AC#3 moved to TASK-44 (not reproduced here: no mlx extra in worktree).
<!-- SECTION:NOTES:END -->
