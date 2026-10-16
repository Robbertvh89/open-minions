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
| Ollama | not measured: Ollama not installed on the bench Mac (pending approval) |

## Method

Command (one run, default records and max tokens):

```bash
uv sync --package minion-executor --extra mlx --extra hf
uv run minion-executor bench --models deploy/bench-models.yaml \
  --model qwen2.5-0.5b-mlx --model qwen2.5-0.5b-hf --model minilm-mlx --model minilm-hf \
  --model qwen2.5-0.5b-ollama --model minilm-ollama --batch-sizes 1,8,32 --out m3-apple-m3-pro.json
```

- Real path: model cache (pinned revision, verified), then a sandboxed `WorkerHandle` (Seatbelt), then
  the runner. Profile `full` (memory budget 70% of RAM, duty cycle 1.0, nice 0). No server, no token.
- Per (model, batch size) a fresh worker loads the model (`load_s`, weights already on disk), runs one
  warm-up batch of that size (excluded), then 256 deterministic synthetic records (seed 0) in batches of
  that size. Only those `run` calls are timed (`run_s`). A fresh worker per row makes the memory peaks
  per row, not a running maximum.
- Completion: temperature 0, `max_tokens` 128. Nearly every record ran to the limit (about 32.5k of
  32.8k possible completion tokens per row; exact counts in the JSON), so tok/s is comparable across rows.
  Embedding: default params.
- `tok_s` = completion tokens / `run_s`; `prompt_tok_s` = prompt tokens / `run_s`;
  `records_per_h` = records / `run_s` * 3600. Embedding runners report no token counts, so those are `-`.
- `engine peak GB`: `mx.get_peak_memory` (mlx) or max `torch.mps.driver_allocated_memory` (hf);
  `worker RSS peak GB`: `ru_maxrss` of the worker process.
- Models, same model at matching precision (see `deploy/bench-models.yaml`):
  - Qwen2.5-0.5B-Instruct: mlx `mlx-community/Qwen2.5-0.5B-Instruct-bf16` (56d07e76), hf
    `Qwen/Qwen2.5-0.5B-Instruct` (7ae55760), both 16-bit.
  - all-MiniLM-L6-v2: mlx `mlx-community/all-MiniLM-L6-v2-bf16` (b6691709), hf
    `sentence-transformers/all-MiniLM-L6-v2` (1110a243). Caveat: the hf checkpoint is stored as fp32 and
    the hf runner casts to fp16, while mlx uses the bf16 conversion, so precision matches only
    approximately.
- Ollama entries (`qwen2.5:0.5b-instruct-fp16`, `all-minilm:l6-v2`) are in the `pending:` list of the
  bench file (no validated manifest digest without an Ollama install) and are reported `unavailable`.

## Results

| model | engine | type | batch | status | load s | run s | tok/s | prompt tok/s | records/h | engine peak GB | worker RSS peak GB |
|---|---|---|---|---|---|---|---|---|---|---|---|
| qwen2.5-0.5b-mlx | mlx | completion | 1 | ok | 5.36 | 301.11 | 108.4 | 36.5 | 3061 | 1.00 | 1.42 |
| qwen2.5-0.5b-mlx | mlx | completion | 8 | ok | 5.49 | 43.19 | 751.2 | 254.1 | 21340 | 1.37 | 1.42 |
| qwen2.5-0.5b-mlx | mlx | completion | 32 | ok | 5.47 | 20.25 | 1602.2 | 541.5 | 45514 | 1.40 | 1.42 |
| qwen2.5-0.5b-hf | hf | completion | 1 | ok | 5.71 | 336.15 | 97.1 | 32.7 | 2742 | 1.03 | 2.26 |
| qwen2.5-0.5b-hf | hf | completion | 8 | ok | 5.74 | 89.17 | 364.1 | 123.1 | 10335 | 1.06 | 2.26 |
| qwen2.5-0.5b-hf | hf | completion | 32 | ok | 5.80 | 36.33 | 893.7 | 301.8 | 25370 | 2.20 | 2.26 |
| minilm-mlx | mlx | embedding | 1 | ok | 5.05 | 0.74 | - | - | 1252390 | 0.06 | 0.38 |
| minilm-mlx | mlx | embedding | 8 | ok | 4.96 | 0.31 | - | - | 3000690 | 0.11 | 0.39 |
| minilm-mlx | mlx | embedding | 32 | ok | 5.18 | 0.31 | - | - | 2975219 | 0.16 | 0.39 |
| minilm-hf | hf | embedding | 1 | ok | 5.40 | 1.37 | - | - | 671763 | 1.02 | 0.50 |
| minilm-hf | hf | embedding | 8 | ok | 5.39 | 0.64 | - | - | 1440452 | 1.02 | 0.51 |
| minilm-hf | hf | embedding | 32 | ok | 5.41 | 0.34 | - | - | 2676697 | 1.02 | 0.51 |
| qwen2.5-0.5b-ollama | ollama | completion | - | unavailable (manifest digest not pinned (Ollama not installed on the bench Mac, pending approval)) | - | - | - | - | - | - | - |
| minilm-ollama | ollama | embedding | - | unavailable (manifest digest not pinned (Ollama not installed on the bench Mac, pending approval)) | - | - | - | - | - | - | - |

## Interpretation

- Completions: MLX is faster than HF/MPS at every batch size, by 1.1x at batch 1 (108 vs 97 tok/s,
  decode-bound on one sequence), 2.1x at batch 8 and 1.8x at batch 32 (1602 vs 894 tok/s). Both scale
  strongly with batching (MLX 15x from batch 1 to 32), so batching matters more than engine choice at
  batch 1.
- Embeddings: MLX is 1.9x faster at batch 1 and 1.1x to 2.1x at larger batches. Runs last 0.3 to 1.4 s,
  so these numbers are noisy (batch 8 vs 32 on MLX is within noise); use them as order of magnitude.
- Memory: MLX engine peak is 1.0 to 1.4 GB for the 0.5B model (weights about 1 GB plus KV cache); HF
  stays at 1.0 GB until batch 32 (2.2 GB) and its worker RSS is 2.3 GB vs 1.4 GB for MLX (torch
  runtime). The HF embedding peak (1.02 GB for a 45 MB model) is the MPS driver allocation pool, not
  model size. All rows fit well inside the `balanced` 40% budget on this 36 GB Mac.
- Conclusion for ADR-0005: MLX as the default engine is confirmed on this hardware for both task types.
  HF remains a reasonable fallback (about 55% of MLX throughput at batch 32). The Ollama comparison is
  outstanding until Ollama is installed on a bench Mac (needs the user's approval); re-run with the two
  pending entries pinned and add the numbers here.
- Limits: one Mac, one small model per task type, one run per row (no repetitions or confidence
  intervals), duty cycle 1.0. Larger models and the `balanced` and `light` profiles are not covered.
