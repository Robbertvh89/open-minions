---
id: TASK-44
title: Find and fix the pytest process that grew to ~101 GB RSS
status: In Progress
assignee: []
created_date: '2026-10-19 18:32'
updated_date: '2026-10-19 23:36'
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
- [ ] #1 Source confirmed with an RSS measurement
- [x] #2 GPU tests bounded in memory (test or limit)
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Measurements (macOS, 36 GB, mlx+hf extras, footprint guard on; peak memory footprint / max RSS):
- burn test alone: SKIPPED x4 (host GPU 32-38% busy: WindowServer/WallpaperAerials). Direct child probe of _gpu_burn(10): old unbounded 0.383 GiB, new bounded 0.32 GiB.
- test_fg_probe_gpu 0.10 / 0.13 GB; test_gpu_guard_gpu 0.10 / 0.85 GB (4 pass, 3 skip busy GPU); test_hf_runners_gpu 3.79 / 2.78 GB (2 e2e fail); test_mlx_runners_gpu 1.72 / 1.18 GB (2 e2e fail).
- -m "gpu or model": 3.79 / 2.78 GB (43 pass, 4 fail, 3 skip). -m "not integration": 0.76 / 0.95 GB (2016 pass). Full suite (incl. integration) not run.
- Top peaks per test (MINION_TEST_RSS_LOG): hf test_unload_frees_model_memory 3.16 GB, hf embedding tests 2.4, mlx completion 2.17. Nothing near 101 GB.
Verdict: burn NOT the culprit (0.38 GiB unbounded); no test reproduces growth. The 101 GB run is unexplained; the 16 GB footprint guard (MINION_TEST_MAX_FOOTPRINT_GB, abort exit 3 + nodeid + stack) now catches any recurrence. e2e_*_through_sandboxed_worker fail with JSONDecodeError here (probably nested Seatbelt under the agent sandbox; unverified).
<!-- SECTION:NOTES:END -->
