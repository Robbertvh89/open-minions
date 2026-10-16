---
id: TASK-4.4
title: 'M3.4: minion-executor bench, ADR-0005 confirmation and docs'
status: Done
assignee: []
created_date: '2026-10-15 20:42'
updated_date: '2026-10-16 13:18'
labels:
  - M3
dependencies:
  - TASK-4.3
parent_task_id: TASK-4
priority: high
ordinal: 21000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Plan steps S9-S10 of TASK-4 (D13).
<!-- SECTION:DESCRIPTION:END -->


## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
S9 (6e6c145, 972e0de, e0e7cf1): bench.py (run_bench, BenchConfig, load_bench_models, render_table, JSON report schema 1), main.py bench command (--models/--model/--records/--batch-sizes/--max-tokens/--profile/--out), deploy/bench-models.yaml (pinned mlx/hf models, ollama entries in pending:), docs/bench/m3-apple-m3-pro.md + .json (Apple M3 Pro 36 GB, one run per row). Real path: ModelCache, sandboxed WorkerHandle, runner; no server/token. S10 (docs sync): ADR-0005 status and benchmark outcome section; SPEC open questions Q5/Q6 marked (Q5 open, Q6 partly answered); ARCHITECTURE §2.2 bench implemented, §3.3 hf memory cap, §8 M3 done; .env.example lists MINION_EXECUTOR_DEV, TEST_HOOKS, TOKEN as comments. CLAUDE.md bench line verified against main.py, no change. Ollama not measured (opt-in, ADR-0009); TASK-13.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
M3 bench on Apple M3 Pro: MLX is 1.22x to 2.07x faster than HF/mps for Qwen2.5-0.5B completions at batch 1/8/32 and uses less worker RSS (1.42 vs 2.26 GB). MLX stays default (ADR-0005 Accepted); HF remains fallback. Ollama not measured, deferred to TASK-13.
<!-- SECTION:FINAL_SUMMARY:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 bench reports tok/s, records/h, peak memory for mlx vs hf on the same model (Ollama measurement deferred to TASK-13 by project decision; results in docs/bench/)
- [x] #2 ADR-0005 updated with bench numbers; SPEC/ARCHITECTURE/CLAUDE.md match code
<!-- AC:END -->
