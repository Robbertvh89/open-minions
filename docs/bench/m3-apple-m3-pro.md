# M3 engine benchmark: Apple M3 Pro

Evidence for TASK-4.4 AC#1 and the "Revisit" outcome of
[ADR-0005](../adr/0005-mlx-default-inference-engine.md). Raw data: [`m3-apple-m3-pro.json`](m3-apple-m3-pro.json).

## Hardware and software

| | |
|---|---|
| Chip / memory | Apple M3 Pro, 36 GB unified memory |
| macOS | 27.0.1 |
| Python | 3.12.7 |
| Libraries | mlx 0.32.4, mlx-lm 0.32.0, torch 2.14.1, transformers 5.20.0, safetensors 0.8.0 |
| Ollama | not measured: opt-in engine outside the sandbox (ADR-0009), not installed on the bench Mac; see TASK-13 |

## Method

Two runs, merged into the one JSON (the `records` column and `config.records_by_type` give the count):

```bash
uv sync --all-packages --extra mlx --extra hf
# completions: 256 records per row
uv run minion-executor bench --models deploy/bench-models.yaml \
  --model qwen2.5-0.5b-mlx --model qwen2.5-0.5b-hf \
  --model qwen2.5-0.5b-ollama --model minilm-ollama \
  --records 256 --batch-sizes 1,8,32 --max-tokens 128 --out comp.json
# embeddings: 2048 records per row
uv run minion-executor bench --models deploy/bench-models.yaml \
  --model minilm-mlx --model minilm-hf --records 2048 --batch-sizes 1,8,32 --out emb.json
```

- Real path: model cache (pinned revision, verified), then a sandboxed `WorkerHandle` (Seatbelt), then
  the runner. Profile `full` (memory budget 70% of RAM, duty cycle 1.0, nice 0). No server, no token.
- Per (model, batch size) a fresh worker loads the model (`load_s`, weights already on disk), runs one
  warm-up batch of that size (excluded), then 256 (completion) or 2048 (embedding) deterministic synthetic records (seed 0) in batches of
  that size. Only those `run` calls are timed (`run_s`). A fresh worker per row makes the memory peaks
  per row, not a running maximum.
- Completion: temperature 0, `max_tokens` 128. Nearly every record ran to the limit (about 32.5k of
  32.8k possible completion tokens per row; exact counts in the JSON), so tok/s is comparable across rows.
  Embedding: default params.
- `tok_s` = completion tokens / `run_s`; `prompt_tok_s` = prompt tokens / `run_s`;
  `records_per_h` = records / `run_s` * 3600. For embeddings the runners report per-record input `tokens`; the bench counts them as prompt tokens, so `prompt tok/s` is the embedding input rate and `tok/s` stays `-`.  Records that failed are excluded from token counts, and a row whose records all failed has no rates.
- `engine peak GB`: `mx.get_peak_memory` (mlx, active allocations) or `torch.mps.driver_allocated_memory`
  sampled at checkpoints (hf, driver pool). These are **not comparable across engines**; compare only
  within an engine. Cross-engine memory conclusions use `worker RSS peak GB` (`ru_maxrss` of the worker
  process).
- `load_s` covers worker spawn, sandbox setup, engine import and weight load (weights already on disk).
- Embedding models use 2048 records because 256 finish in under a second; the per-row `records`
  column is authoritative.
- `bench` ignores the `run --engines ollama` opt-in: putting an ollama entry under `models:` makes the
  bench pull into `~/.ollama`, outside the model cache cap (ADR-0009).
- Stderr during mlx/hf runs shows sandbox-denied writes to the `com.apple.MetalPerformanceShadersGraph`
  cache directory. Runs succeed, but without MPSGraph cache reuse the numbers may be slightly
  pessimistic.
- Models, same model at matching precision (see `deploy/bench-models.yaml`):
  - Qwen2.5-0.5B-Instruct: mlx `mlx-community/Qwen2.5-0.5B-Instruct-bf16` (56d07e76), hf
    `Qwen/Qwen2.5-0.5B-Instruct` (7ae55760), both 16-bit.
  - all-MiniLM-L6-v2: mlx `mlx-community/all-MiniLM-L6-v2-bf16` (b6691709), hf
    `sentence-transformers/all-MiniLM-L6-v2` (1110a243). Caveat: the hf checkpoint is stored as fp32 and
    the hf runner casts to fp16, while mlx uses the bf16 conversion, so precision matches only
    approximately.
- Ollama entries (`qwen2.5:0.5b-instruct-fp16`, `all-minilm:l6-v2`) are in the `pending:` list of the
  bench file (no validated manifest digest without an Ollama install) and are reported `unavailable`
  with the reason "not measured". Follow-up: TASK-13.

## Results

| model | engine | type | batch | status | records | load s | run s | tok/s | prompt tok/s | records/h | engine peak GB | worker RSS peak GB |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| qwen2.5-0.5b-mlx | mlx | completion | 1 | ok | 256 | 4.19 | 280.34 | 116.5 | 39.2 | 3287 | 1.00 | 1.42 |
| qwen2.5-0.5b-mlx | mlx | completion | 8 | ok | 256 | 3.75 | 40.07 | 809.7 | 273.9 | 23001 | 1.36 | 1.42 |
| qwen2.5-0.5b-mlx | mlx | completion | 32 | ok | 256 | 4.45 | 18.88 | 1718.4 | 580.8 | 48815 | 1.40 | 1.42 |
| qwen2.5-0.5b-hf | hf | completion | 1 | ok | 256 | 6.43 | 341.48 | 95.6 | 32.1 | 2699 | 1.03 | 2.26 |
| qwen2.5-0.5b-hf | hf | completion | 8 | ok | 256 | 4.17 | 82.93 | 391.5 | 132.4 | 11113 | 1.06 | 2.26 |
| qwen2.5-0.5b-hf | hf | completion | 32 | ok | 256 | 4.17 | 35.29 | 919.8 | 310.7 | 26111 | 2.20 | 2.26 |
| minilm-mlx | mlx | embedding | 1 | ok | 2048 | 3.68 | 5.18 | - | 24213.9 | 1423878 | 0.06 | 0.38 |
| minilm-mlx | mlx | embedding | 8 | ok | 2048 | 4.04 | 2.14 | - | 58659.2 | 3448329 | 0.11 | 0.39 |
| minilm-mlx | mlx | embedding | 32 | ok | 2048 | 3.99 | 1.90 | - | 66085.6 | 3881428 | 0.16 | 0.39 |
| minilm-hf | hf | embedding | 1 | ok | 2048 | 4.35 | 8.72 | - | 14374.5 | 845280 | 1.02 | 0.50 |
| minilm-hf | hf | embedding | 8 | ok | 2048 | 4.11 | 2.82 | - | 44516.0 | 2616910 | 1.02 | 0.52 |
| minilm-hf | hf | embedding | 32 | ok | 2048 | 3.92 | 2.20 | - | 56961.5 | 3345543 | 1.02 | 0.52 |
| qwen2.5-0.5b-ollama | ollama | completion | - | unavailable (not measured: Ollama is opt-in and not installed on the bench Mac; see TASK-13) | - | - | - | - | - | - | - | - |
| minilm-ollama | ollama | embedding | - | unavailable (not measured: Ollama is opt-in and not installed on the bench Mac; see TASK-13) | - | - | - | - | - | - | - | - |

## Interpretation

- Completions: MLX is faster than HF/MPS at every batch size, by 1.22x at batch 1 (116.5 vs 95.6 tok/s,
  decode-bound on one sequence), 2.07x at batch 8 (809.7 vs 391.5) and 1.87x at batch 32 (1718.4 vs
  919.8). Both scale strongly with batching (MLX 14.8x from batch 1 to 32), so batching matters more
  than engine choice.
- Embeddings (input tokens/s): MLX 24214 / 58659 / 66086 vs HF 14375 / 44516 / 56962 at batch 1 / 8 /
  32, i.e. 1.68x, 1.32x and 1.16x. With 2048 records per row runs last 1.9 to 8.7 s, so these are
  steadier than a 256-record run, but the gap at batch 32 is small; still a single run per row.
- Memory (worker RSS, the only cross-engine comparable column): the HF worker peaks at 2.26 GB vs
  1.42 GB for MLX on the 0.5B model (torch runtime), and 0.52 GB vs 0.39 GB for MiniLM. The engine-peak
  columns are per-engine allocator views (see Method); the HF embedding value (1.02 GB for a 45 MB
  model) is the MPS driver pool, not model size. All rows fit well inside the `balanced` 40% budget on
  this 36 GB Mac.
- Conclusion for ADR-0005: MLX as the default engine is confirmed on this hardware for both task types.
  HF remains a reasonable fallback (about 54% of MLX completion throughput at batch 32). Ollama was
  not measured: it is an opt-in extra that runs outside the sandbox (ADR-0009) and is not installed on
  the bench Mac by project decision. Measurement is deferred to TASK-13 (pin digests, run on a Mac that
  already has Ollama).
- Limits: one Mac, one small model per task type, one run per row (no repetitions or confidence
  intervals), duty cycle 1.0. Larger models and the `balanced` and `light` profiles are not covered.
