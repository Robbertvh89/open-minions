---
id: TASK-4.3
title: 'M3.3: MLX, Ollama and HF runners'
status: In Progress
assignee: []
created_date: '2026-10-15 20:42'
updated_date: '2026-10-16 03:24'
labels:
  - M3
dependencies:
  - TASK-4.2
parent_task_id: TASK-4
priority: high
ordinal: 20000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Plan steps S6-S8 of TASK-4 (D10, D11, D12, D14).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 MLX completion (BatchGenerator) and embedding runners pass gpu+model tests with tiny models; wired limit within budget
- [x] #2 Ollama runner (loopback only, digest pinned) passes MockTransport unit tests in CI
- [x] #3 HF mps runner: bge-small embeddings match MLX (cosine >= 0.99)
- [x] #4 CI stays green without engine extras (markers auto-skip; mypy strict clean)
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Implemented (S6-S8): MLX completion via mlx-lm BatchGenerator (wired limit reset to min(budget, recommended) after construction; mlx_common.py, mlx_completion.py); MLX embedding via own BERT encoder on mlx.nn, pooling from 1_Pooling config, XLM-R and mean_sqrt_len refused as MODEL_UNSUPPORTED (mlx_embedding.py, pooling.py; ADR-0005 addendum); Ollama runner over loopback HTTP with digest-pinned tag, opt-in only, bounded keep_alive, release on every kill and load-phase errors (ollama.py; ADR-0009); HF transformers runner on mps, trust_remote_code=False, safetensors only (hf.py); worker deadlines watchdog-enforced, strict reply validation, RunnerError.release carried over the pipe (worker/process.py); server 422 for ollama embedding with normalize=false (api/tasks.py); extras mlx and hf in packages/minion-executor/pyproject.toml; pytest markers gpu, model, ollama (pyproject.toml, conftest.py).
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
MLX (completion via BatchGenerator, own BERT embedding encoder), opt-in Ollama (loopback, digest-pinned) and HF mps runners landed with worker deadline hardening. GPU/model tests pass locally; CI runs without engine extras. Bench is TASK-4.4.
<!-- SECTION:FINAL_SUMMARY:END -->
