---
id: TASK-44
title: Find and fix the pytest process that grew to ~101 GB RSS
status: In Progress
assignee: []
created_date: '2026-10-19 18:32'
updated_date: '2026-10-20 00:17'
labels:
  - bug
dependencies: []
ordinal: 84000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Seen during parallel runs (2026-10-19): a pytest in the main checkout reached ~101 GB RSS for > 2 h and caused memory pressure. Suspects: test_gpu_guard_gpu.py::test_burn_hook_saturates_the_gpu runs fake._gpu_burn (4096^2 fp16 matmuls, async_eval) in-process with no mx memory/cache limit; test_mlx_runners_gpu.py loads Qwen/BGE in-process. Fix: run the burn in a subprocess or set mx.set_memory_limit/set_cache_limit and assert RSS; confirm with a real gpu run (mlx extra).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 Source confirmed, or not reproducible on the full suite and a guard that names the culprit test is in place (evidence attached)
- [x] #2 GPU tests bounded in memory (test or limit)
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Measurements (macOS 36 GB, mlx+hf extras, footprint guard on; peak memory footprint / max RSS):
- burn test alone: SKIPPED on 4 tries (host GPU 32-38% busy by other processes), so its share/footprint asserts have not run here. Direct child probe of _gpu_burn(10): old unbounded 0.383 GiB, bounded 0.32 GiB. The burn is not the culprit.
- test_fg_probe_gpu 0.10/0.13 GB; test_gpu_guard_gpu 0.10/0.85 GB; test_hf_runners_gpu 3.79/2.78 GB; test_mlx_runners_gpu 1.72/1.18 GB.
- -m "gpu or model": 3.79/2.78 GB. -m "not integration": 0.76/0.95 GB.
- FULL suite incl. integration (2326 passed, 55 skipped, 1 xfailed; gpu tests skip without -m gpu): peak footprint 0.82 GB, max RSS 0.98 GB; top-10 peaks 0.76 GB (watchdog sigterm test, ui tests). Not reproducible: nothing near 101 GB on any selection.
- Highest single tests under -m gpu: hf test_unload_frees_model_memory 3.16 GB, hf embedding 2.4, mlx completion 2.17.
Conclusion: the 101 GB run is not reproducible; guard (16 GB default, names the test, dumps stacks to real stderr, exit 3, breach record in MINION_TEST_RSS_LOG) is in place. e2e_*_through_sandboxed_worker fail with JSONDecodeError under the agent sandbox (likely nested Seatbelt, unverified).
<!-- SECTION:NOTES:END -->
