# ADR-0005: MLX as default inference engine

- Status: **Accepted** (defaults to be confirmed by the M3 benchmark)
- Date: 2026-10-15
- Deciders: project owner
- Deviates from paper: **yes** — the paper uses HuggingFace transformers (embeddings) and Ollama (completions).

## Context

Executors run batch workloads: each subtask carries hundreds of records. Throughput per Mac (records
per hour) is the metric that matters, not single-request latency.

Findings (October 2026):
- **Ollama ≥ 0.19 uses MLX on Apple Silicon** (preview since 2026-03-30), roughly doubling decode
  speed vs. its llama.cpp path in Ollama's own benchmark (58 → 112 tok/s, Qwen3.5-35B-A3B). The MLX path
  applies only to safetensors models; **GGUF models still run on llama.cpp**.
- **mlx-lm used directly** remains faster for single-stream generation in independent tests
  (e.g. 107.5 vs 90.5 tok/s on M3 Ultra; 131.8 vs 48 tok/s vs. pre-MLX Ollama on M4 Max).
- **Batching** is the larger lever for our workload. Ollama is designed for interactive, low-concurrency
  use. `mlx-lm`'s `BatchGenerator` implements token-level (continuous) batching; vllm-mlx, which builds
  on it, self-reports 1.5–3.4× throughput at 5 concurrent requests on M4 Max (unverified).
- No independent batched benchmark of mlx-lm vs Ollama vs llama.cpp on Apple Silicon was found → we
  must measure ourselves.
- `mlx-embeddings` provides MLX-native embedding models as an alternative to `transformers` on `mps`.

Sources: [Ollama MLX blog](https://registry.ollama.ai/blog/mlx),
[terminalbytes 2026 comparison](https://terminalbytes.com/ollama-vs-llama-cpp-vs-mlx-mac-2026/),
[GGUF caveat](https://levelup.gitconnected.com/ollama-mlx-nearly-doubles-llm-speed-on-your-mac-58-to-112-tok-s-and-your-old-models-get-none-of-fd1a9ec531fb),
[arXiv 2511.05502](https://arxiv.org/pdf/2511.05502),
[vllm-mlx continuous batching](https://vllm-mlx.is-a.dev/guides/continuous-batching/).

## Decision

1. **Default engine `mlx`**, running in-process in the executor's inference worker subprocess:
   - completions: `mlx-lm` with `BatchGenerator` (batched over a subtask's records);
   - embeddings: a BERT-family encoder on `mlx` (see Addendum 2026-10-16; not `mlx-embeddings`).
2. **Fallback engines** behind the same `Runner` protocol:
   - `ollama` — for models only available as GGUF / Ollama tags (paper-compatible path);
   - `hf` — `transformers` / `sentence-transformers` on `mps` for models without an MLX port.
3. Engine is chosen **per model** in the server's `models.yaml` allow-list, not per executor. Executors
   advertise which engines they have installed; the server only offers matching subtasks.
4. Models are pulled from the HF hub in MLX format (e.g. `mlx-community/*`) with
   `trust_remote_code=False`; only allow-listed repos/revisions.
5. M3 ships `minion-executor bench` to measure each engine on the same model (tok/s, records/h, peak
   memory, at several batch sizes and capacity profiles). Defaults are revisited with those numbers.

## Consequences

**Positive**
- Highest expected throughput per Mac, especially for MoE models and batched decode.
- Direct control over the generation loop → enables the duty-cycle throttle and batch-size caps
  required for partial capacity allocation (SPEC §4.6, ADR-0006). This is hard with Ollama, which
  owns its own scheduling.
- Direct control over memory (`mx.set_memory_limit`, `mx.set_cache_limit`) for budget enforcement.
- No external daemon required for the default path.

**Negative / risks**
- The executor now owns model memory management that Ollama used to handle → mitigated by running
  inference in a **worker subprocess** (crash isolation; kill to fully free unified memory) and by
  admission control on `memory_gb`.
- MLX and mlx-lm APIs move quickly → pin versions in `uv.lock`; integration test per supported model.
- Model coverage gaps (no MLX port) → `ollama` / `hf` fallbacks.
- Duty cycling on the Ollama fallback is coarser (per request, not per step) → fallback models get
  conservative batch sizes under `light`/`balanced` profiles.

**Revisit when**
- The M3 benchmark shows Ollama (MLX) within ~10 % of mlx-lm batched throughput *and* acceptable
  throttling control → consider Ollama as default for simplicity.
- vllm-mlx (or similar) matures into a stable library with paged KV cache and clear embedding support
  → consider it as the `mlx` runner implementation.

## Addendum 2026-10-16 (TASK-4.3 S6): own MLX encoder instead of `mlx-embeddings`

`mlx-embeddings==0.1.0` depends on `mlx-vlm` and `mlx-audio`, which drag in opencv-python, scipy,
pandas, pyarrow, datasets, sounddevice, miniaudio and more (about 36 extra packages), all of which
would be shipped to every Mac and loaded into the sandboxed worker. Its BERT `text_embeds` also
mean-pools unconditionally, which is wrong for CLS-pooled models such as bge. We therefore ship a
~60-line BERT encoder on `mlx.nn` (`runners/mlx_embedding.py`) with pooling taken from the model's
sentence-transformers `1_Pooling/config.json` (CLS when absent) and tokenisation via the
`transformers` tokenizer that `mlx-lm` already requires. Validated against `mlx-embeddings` on
`bge-small-en-v1.5-bf16` with mean pooling: cosine >= 0.9999 on every text. Supported
architectures: BERT encoders (XLM-R is excluded: its position ids start at `padding_idx + 1`); anything else fails the load with the non-retryable
`MODEL_UNSUPPORTED`.
