---
id: TASK-44
title: Find and fix the pytest process that grew to ~101 GB RSS
status: In Progress
assignee: []
created_date: '2026-10-19 18:32'
updated_date: '2026-10-19 23:11'
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
- [ ] #2 GPU tests bounded in memory (test or limit)
<!-- AC:END -->
