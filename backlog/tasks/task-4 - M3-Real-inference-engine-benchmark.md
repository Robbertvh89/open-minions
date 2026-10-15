---
id: TASK-4
title: 'M3: Real inference + engine benchmark'
status: In Progress
assignee: []
created_date: '2026-10-15 14:02'
updated_date: '2026-10-15 20:42'
labels:
  - milestone
  - M3
dependencies: []
ordinal: 4000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
MLX completion (BatchGenerator) and embedding runners, Ollama/HF fallbacks, models.yaml admission, minion-executor bench. Confirms ADR-0005.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 bench reports tok/s, records/h, peak memory for mlx vs ollama vs hf on same model
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
# TASK-4 plan: M3 real inference and engine benchmark

## Goal and AC mapping
Executors run real embedding and completion subtasks on the Apple GPU. The default engine is MLX: `mlx-lm` `BatchGenerator` for completions and `mlx-embeddings` for embeddings. Ollama (loopback HTTP) and HF transformers on `mps` are fallbacks. Inference always runs in a sandboxed, offline worker subprocess. Models come from a pinned, verified, size-bounded local cache. `minion-executor bench` measures each engine on the same model.

- **AC#1: bench reports tok/s, records/h and peak memory for mlx vs ollama vs hf on the same model.**
  - Covers ADR-0005 decision 5 and SPEC Q5/Q6 input.
  - Evidence: a bench run on this Mac (M3 Pro, 36 GB) at fp16/bf16 for all three engines. The results go into `docs/bench/` and ADR-0005.
- **Also in scope:**
  - FR-4/FR-4a (`revision` pin, typed params), FR-6 (output line fields), FR-11 (ensure-model step), FR-12 (cache), FR-13 (engines, warm worker, 10-min idle unload) and FR-16 (engine probe plus executor-side admission re-check).
  - FR-50 levers already promised by the budget: MLX memory/cache/wired limits, static GPU duty-cycle pacing and `max_batch_size`.
  - S-1, S-5 and S-7 (pinned revisions, no remote code, safetensors only, sandbox).
  - NF-6.
- **M1 carry-overs, all resolved here:**
  1. Runner v2 with per-step hook, progress ticks and `memory_in_use()`.
  2. Worker sandbox.
  3. Environment and proxy decision.
  4. `--runner mlx` exit 2.
  5. Separate load and step timeouts.
  6. Revision pinning.
- **TASK-9 triage:**
  - **#1 (revision SHA pin) moves into M3.** S-7 cannot hold once real weights are loaded.
  - **#3 (params allow-list) moves into M3.** M3 is the first time params reach an engine, and unvalidated keys such as `trust_remote_code` must not get there.
  - **#2 (Range-signed GET, PUT size cap) stays in TASK-9.** It is S3 hardening that is independent of engines and not needed for AC#1.
  - Proposed: edit TASK-9 to note that #1 and #3 are delivered by TASK-4.1.
- **Out of scope (M4):**
  - Governor and signals, adaptive profiles, mid-subtask budget change or downgrade-release, the FR-17 grace period and `custom` profile overrides.
  - The `throttle.py` pacer added here is static (fixed `gpu_duty_cycle` from the budget at load). M4 makes it dynamic.

## Touchpoints (current code)
- **Runner protocol** (`runners/base.py:12-26`): has `load(model, budget)`, `run(batch)` and `unload()`.
  - `RunnerError` has no `retryable` flag.
  - `create_runner(name)` (`runners/__init__.py:7-16`) knows only "fake".
- **Fake runner** (`runners/fake.py:25-59`): uses `text` for completions, takes no params, and has test hooks.
- **Worker** (`worker/process.py`)
  - Environment allowlist `:27-45` with the TODO(M3) at `:29`; `restrict_environ` `:60-69`.
  - `worker_main` `:89-138` creates one runner per process from `runner_name` and answers exactly one reply per op.
  - `WorkerHandle(runner_name, load_timeout_s=900, run_timeout_s=600)` `:150-177`.
  - `_call` uses a total deadline `:217-247`; `ensure_model` `:249-268`; `run` `:270-273`.
- **Loop** (`loop.py`)
  - `Worker` protocol `:98-103`.
  - `_execute` `:389-436` downloads, parses, runs, writes and uploads.
  - `_run_batches` `:438-452` calls `ensure_model` and then `run` per `max_batch_size`, and maps `RunnerError` to a retryable fail with the runner's code.
  - `Assignment.params` is never passed to the worker.
  - No idle unload.
- **CLI** (`main.py`): `DEFAULT_RUNNER="mlx"` `:52`; `_validate_runner` `:58-62` (mlx → `ValueError` → exit 2); `_run` hardcodes `engines=[MLX]` `:91`; `restrict_environ()` `:111`. `bench` does not exist.
- **Schemas** (`minion_common/schemas.py`)
  - `ModelEntry` `:29-34` has no `revision`, no ref validation and an unbounded `memory_gb`.
  - `TaskCreate.params` `:45` and `Assignment.params` `:116` are free dicts.
- **Server**
  - Task create (`api/tasks.py:105-130`): `UNKNOWN_MODEL` / `MODEL_TYPE_MISMATCH` only; params unchecked.
  - `models_registry.load_models` `:9-18`; `deploy/models.yaml` (2 mlx entries, no revision); `tests/models.test.yaml` (fake).
  - Acquire builds `Assignment.model` from the live registry, not from a task snapshot (`services/acquisition.py:73,107`).
  - `minion_task` snapshots only `engine` and `model_memory_gb` (`db/models.py:101-102`).
- **Build and test config**
  - Root `pyproject.toml`: markers (only `integration`) and mypy overrides.
  - `packages/minion-executor/pyproject.toml`: no engine dependencies.
  - CI `.github/workflows/ci.yml`: ubuntu, `uv sync --locked`, `pytest -v`.
- **No DB migration in M3** (see decision D9).

## Key decisions (with rationale)
- **D1. Runner v2 contract** (`runners/base.py`)
  ```
  class StepControl(Protocol):
      def checkpoint(self, records_done: int | None = None) -> None
          # call between generation steps / embedding batches
  class MemoryStats(TypedDict): active_bytes: int; peak_bytes: int
  class Runner(Protocol):
      def load(self, model: ModelEntry, model_path: Path | None,
               budget: Budget, cfg: EngineConfig) -> None
      def run(self, batch: list[Record], params: TaskParams, ctl: StepControl) -> list[Record]
      def memory_in_use(self) -> MemoryStats
      def unload(self) -> None
  RunnerError(code, message="", *, retryable=True)
  ```
  - `checkpoint` does three things:
    - (a) static duty-cycle pacing: sleep `t·(1−d)/d` after a step of duration `t`;
    - (b) a progress/liveness tick to the parent, at most 1 per second;
    - (c) the seam M4 uses to poll pause and budget messages.
  - Cancellation stays "kill the worker" (`interrupt()`, already reliable). There is no cooperative cancel flag, which keeps it simple.
  - `EngineConfig` is a small frozen dataclass: `ollama_url`, `max_prompt_tokens` default. It comes from executor settings via the `load` message, never from the environment.
- **D2. One worker serves one model, and the runner is chosen at `load`.**
  - `create_runner(engine, type, *, test_hooks)` dispatches as follows:
    - `(MLX, COMPLETION)` → `mlx_completion`
    - `(MLX, EMBEDDING)` → `mlx_embedding`
    - `OLLAMA` → `ollama`
    - `HF` → `hf`
    - `FAKE` → `fake`
  - `WorkerHandle(engines: Collection[Engine], …)` replaces `runner_name`. The worker refuses engines that are not in the set.
  - A model switch already kills the worker, so each process imports exactly one engine.
  - New worker ops:
    - `probe`: returns `{engine: version | None}` by importing in the child only, so the control process never imports engines (invariant 7 and CLAUDE.md).
    - `stats`: returns `memory_in_use()` plus `getrusage` `ru_maxrss`.
- **D3. Timeouts** (carry-over 5)
  - `load_timeout_s` (default 1800, `MINION_EXECUTOR_LOAD_TIMEOUT_S`): covers a large MLX load and Metal kernel compile. A local disk load needs no download time, because downloads happen before `load` (D5).
  - The `run` total deadline is replaced by an inactivity timeout `step_timeout_s` (default 300, `MINION_EXECUTOR_STEP_TIMEOUT_S`) that resets on every tick.
  - The total runtime is already bounded by the server's max lease (6 h).
  - `download_timeout_s` (default 3600) is separate and applies to the fetch process.
- **D4. Typed params** (TASK-9 #3), in `minion_common/schemas.py`
  - `CompletionParams(extra=forbid)`: `temperature` 0..2 (default 0.0); `top_p` (0,1] (1.0); `max_tokens` 1..4096 (256); `seed` int or None; `system` ≤ 8000 chars or None.
  - `EmbeddingParams(extra=forbid)`: `normalize: bool = True`.
  - `parse_params(type, dict) -> TaskParams`.
  - The server returns `422 INVALID_PARAMS` on create. The executor re-parses and turns a bad value into a non-retryable `fail` with code `BAD_PARAMS` (defence in depth for rows created before M3).
  - No `stop`, `trust_remote_code` or engine kwargs. Temperature defaults to 0, so reruns are near-deterministic, which supports invariant 5.
- **D5. Downloads happen in the control side; the worker is offline.**
  - `model_cache.py` (executor package, not `runners/`) runs `huggingface_hub.snapshot_download(repo, revision=<sha>, allow_patterns=SAFE)` in a short-lived `spawn` *fetch subprocess*. It is not an inference process.
    - The subprocess is killable on cancel or SIGTERM. A thread would block `asyncio.run` shutdown, which would break invariant 6's clean stop.
  - `SAFE` covers `*.safetensors`, `*.json`, `tokenizer.model`, `*.tiktoken`, `vocab.txt`, `merges.txt` and `*.jinja`. It never includes `*.py`, `*.bin`, `*.pt`, `*.pkl` or `*.gguf`.
  - Verification: every LFS file's sha256 and size against `model_info(revision, files_metadata=True)`.
  - Atomic rename `staging` → `<root>/hf/<org>--<name>/<sha>/` plus a `.minion-manifest.json` with files, sizes and sha256.
  - On every load, `ensure()` re-checks that the manifest is present and the sizes match. A missing or bad entry is deleted and fetched again.
  - Runners load from the local path with `HF_HUB_OFFLINE=1`, `TRANSFORMERS_OFFLINE=1`, `trust_remote_code=False` and `use_safetensors=True`.
  - huggingface_hub (>=1.31,<3) becomes a base executor dependency. It is pure Python and not an "engine".
- **D6. Cache policy** (FR-12)
  - Root: `MINION_EXECUTOR_MODEL_CACHE_DIR`, default `~/Library/Caches/open-minions/models` (`$XDG_CACHE_HOME/...` off macOS).
  - Cap: `MINION_EXECUTOR_MODEL_CACHE_GB`, default 50.
  - LRU by manifest mtime, which is touched on every use.
  - Eviction runs *before* a download, sized from hub metadata. It never evicts the model that is currently loaded.
  - An `fcntl.flock` on `<root>/.lock` serialises mutations (run and bench may coexist).
  - Model larger than the cap → retryable `fail MODEL_CACHE_FULL`, logged loudly. This is a per-Mac config issue, and other Macs may succeed.
  - Ollama models live in Ollama's own store, are tracked by tag and are not counted against the cap. This is documented.
- **D7. Environment and proxies** (carry-over 3, resolves the TODO at `worker/process.py:29`)
  - `ExecutorSettings` (pydantic-settings, new `settings.py`) reads `MINION_EXECUTOR_*`.
  - A `FetchEnv` captures `HTTP(S)_PROXY`, `ALL_PROXY`, `NO_PROXY`, `SSL_CERT_FILE`, `SSL_CERT_DIR`, `REQUESTS_CA_BUNDLE`, `HF_ENDPOINT` (must be https), `HF_HUB_*` and `HF_TOKEN` (SecretStr) *before* `restrict_environ()`.
    - These go only to the fetch subprocess, as an explicit env dict.
    - Credential-bearing proxy URLs are allowed there (the fetch process runs no model code), are redacted in logs and are never forwarded to the worker.
  - The worker allowlist does not grow. The worker gets `HF_HUB_OFFLINE=1`, `TRANSFORMERS_OFFLINE=1`, `HF_HUB_DISABLE_TELEMETRY=1`, and no proxy, CA or token.
  - `OLLAMA_HOST` → `MINION_EXECUTOR_OLLAMA_URL` (default `http://127.0.0.1:11434`) must be loopback, or the Ollama engine is reported unavailable. Prompts never leave the Mac (S-1). The URL reaches the runner via `EngineConfig`.
- **D8. Worker sandbox** (carry-over 2; new ADR-0009)
  - In `worker_main`, at the first `load`, before `create_runner` imports any engine, the worker calls `sandbox_init(profile, 0, &err)` through ctypes in a new `worker/sandbox.py`.
  - The profile is a pure function `build_profile(home, allow_read, allow_write, ollama_port | None) -> str`:
    ```
    (version 1)(allow default)
    (deny network*)                 + (allow network-outbound (remote tcp "localhost:<port>")) for ollama only
    (deny file-read* file-write* (subpath HOME))
    (allow file-read* (subpath <python prefixes/site-packages under HOME>) (subpath <cache dir>))
    (allow file-read* file-write* (subpath <worker tmp>))
    (deny mach-lookup (global-name "com.apple.SecurityServer") (global-name "com.apple.securityd.xpc"))
    ```
    - This blocks Keychain, `~/.aws`, `~/.ssh` and `~/.config/minion`.
    - The cache is read-only for the worker, so a compromised worker cannot poison other models.
  - Fail-closed on macOS: if `sandbox_init` errors, `load` fails with `SANDBOX_FAILED` and the CLI refuses to start (exit 2 at probe).
  - Opt-out is only possible with `MINION_EXECUTOR_SANDBOX=0` *and* `MINION_EXECUTOR_DEV=1`. Off macOS it is a no-op (only fake/dev runs there).
  - Rejected: a separate macOS user (needs admin/root install, so it belongs to M5 packaging) and a `sandbox-exec` wrapper (needs a hacked multiprocessing `set_executable` and an on-disk script).
- **D9. Revision pinning** (TASK-9 #1), in `ModelEntry`
  - `revision: str | None`. The validator requires 40-hex for `mlx`/`hf` and a 64-hex Ollama manifest digest (with the `sha256:` prefix normalised) for `ollama`. It is optional for `fake`.
  - `ref` regex: HF repo id `^[A-Za-z0-9][\w.-]*/[\w.-]+$` without `..`, or an Ollama tag `^[a-z0-9][a-z0-9._/-]*(:[\w.-]+)?$`.
  - `memory_gb` gets `le=1024`.
  - The executor validates too, because `Assignment.model` is parsed with the same model.
  - No migration: the registry stays the source of truth at acquire, as today.
  - Output lines carry `model` and `model_revision`, so mixed revisions are detectable.
  - Operator rule, documented: never change the revision of an existing `model_id`; add a new id instead.
- **D10. Output lines** (FR-6)
  - Embedding: `{id, embedding, tokens, model, model_revision}`.
  - Completion: `{id, completion, finish_reason, prompt_tokens, completion_tokens, model, model_revision}`.
  - The loop stamps `model` and `model_revision`, so the logic stays engine-agnostic.
  - Prompt input is either `prompt` (wrapped as a user message) or `messages` (roles system/user/assistant, string content), with `params.system` prepended.
  - A prompt longer than the context minus `max_tokens` yields the per-record line `{id, error: "PROMPT_TOO_LONG", ...}` instead of failing the whole subtask.
  - Malformed records (neither `prompt` nor `messages`, wrong types) → `RunnerError(BAD_INPUT, retryable=False)`.
  - SPEC FR-6 amendment.
- **D11. Engine extras.** Optional extras on minion-executor, forwarded by root extras:
  - `mlx = ["mlx==0.32.4", "mlx-lm==0.32.0", "mlx-embeddings==0.1.0"]` and `hf = ["torch>=2.6", "transformers>=5.14,<6"]`, all with the marker `sys_platform=='darwin' and platform_machine=='arm64'`. This keeps the linux lock resolution and CI light.
  - Ollama needs no extra: it uses the existing `httpx`, with no `ollama` client package.
  - Rejected: `sentence-transformers`. It pins `huggingface-hub<2` and pulls in scikit-learn/scipy. The HF embedding runner instead reads `modules.json` / `1_Pooling/config.json` (cls/mean/lasttoken) plus the Normalize module itself, in about 40 lines.
  - Dev setup: `uv sync --extra mlx --extra hf`. Note that a plain `uv sync` removes the extras.
  - mypy: an override with `ignore_missing_imports` and `follow_imports="skip"` for `mlx.*`, `mlx_lm.*`, `mlx_embeddings.*`, `torch.*` and `transformers.*`, so CI (no extras) and the Mac type-check the same way.
- **D12. Test tiers.** New markers in the root `pyproject.toml` and auto-skip in the root `conftest.py` (`pytest_collection_modifyitems`):

  | Marker | Needs | Runs where |
  |---|---|---|
  | `macos` | macOS, no GPU (sandbox tests) | Mac; optional CI job |
  | `gpu` | Apple GPU and engine extras | Mac only |
  | `model` | downloads real weights; opt-in `MINION_TEST_MODELS=1` | Mac only |
  | `ollama` | a running local daemon | Mac only |

  - CI stays `pytest -v`, and all four tiers auto-skip there.
  - Fakes cover everything else: the fake runner for the worker/loop/bench path, `httpx.MockTransport` for Ollama, and an injectable fetch function for the model cache.
  - Recommended: an extra `macos-14` CI job running `pytest -m macos`, because the sandbox is security-critical. GitHub's macOS VMs have no Metal/MPS, so `gpu` cannot run there.
- **D13. Bench**
  - Command: `minion-executor bench --models deploy/bench-models.yaml --model ID [--model ID…] [--records 256] [--batch-sizes 1,8,32] [--max-tokens 128] [--profile full] [--out FILE.json]`.
  - It runs the *real* path: model cache, then sandboxed `WorkerHandle`, then runner. No server or token is needed.
  - Input is deterministic synthetic records generated from a seed. Payloads are never logged.
  - One warm-up batch is excluded.
  - Reported per (model, batch size):
    - `load_s`, `run_s`
    - `prompt_tokens`, `completion_tokens`
    - `tok_s` (completion tokens/s), `prompt_tok_s`
    - `records_per_h`
    - `engine_peak_gb` (`mx.get_peak_memory`, `torch.mps.driver_allocated_memory` max, or Ollama `/api/ps` size), `worker_rss_peak_gb`
    - engine and library versions, chip, macOS version.
  - Output: a markdown table on stdout plus JSON. An engine that is unavailable shows as `unavailable`, not a crash.
  - `deploy/bench-models.yaml` defines the same model at matching precision on all three engines. The implementer verifies the refs and pins the SHAs/digests:
    - completion: Qwen2.5-0.5B-Instruct as mlx `mlx-community/Qwen2.5-0.5B-Instruct-bf16`, hf `Qwen/Qwen2.5-0.5B-Instruct`, ollama `qwen2.5:0.5b-instruct-fp16`;
    - embedding: all-MiniLM-L6-v2 as mlx `mlx-community/all-MiniLM-L6-v2-bf16`, hf `sentence-transformers/all-MiniLM-L6-v2`, ollama `all-minilm:l6-v2`.
- **D14. Invariant 6 levers that become real in M3**
  - MLX: `mx.set_memory_limit(budget)` and a small `mx.set_cache_limit`.
    - **`BatchGenerator.__init__` calls `maybe_set_recommended_wired_limit()`, which wires memory up to the recommended maximum.** The runner must reset it with `mx.set_wired_limit(min(budget_bytes, recommended))` right after construction and verify it in a gpu test.
  - HF: `torch.mps.set_per_process_memory_fraction(budget/recommended)`. An MPS OOM becomes `RunnerError(OOM, retryable)`.
  - Ollama: `keep_alive=0` on unload, plus admission by `memory_gb` only. Ollama runs inference in its own daemon, outside our worker and sandbox, so its memory and duty limits are coarse (per request). This is documented.
  - The static duty-cycle pacer applies to all engines.
  - Idle unload: the loop calls `worker.unload()` (a reset) after 10 min without a subtask (FR-13; setting `MINION_EXECUTOR_IDLE_UNLOAD_S`, default 600).

## Steps
Each step is ≤ ~300 changed lines (code plus tests) and lands as one implementer pass. Grouped into 4 sub-tasks.

### TASK-4.1: contracts and worker foundation (S1–S3)
**S1. Common/server contracts** (TASK-9 #1 schema and #3)
- `minion_common/schemas.py`:
  - `ModelEntry.revision` with the validators from D9;
  - `CompletionParams`, `EmbeddingParams`, `TaskParams = CompletionParams | EmbeddingParams`, `parse_params()`;
  - `memory_gb le=1024`.
- `api/tasks.py:105-130`: `parse_params` → `422 INVALID_PARAMS`, raised after the `MODEL_TYPE_MISMATCH` check.
- `deploy/models.yaml`: real 40-hex SHAs. The implementer looks them up with `huggingface_hub.HfApi().model_info(ref).sha`.
- `tests/models.test.yaml`: unchanged (fake needs no revision).
- `.env.example`: no change.
- Tests (CI unit):
  - `minion-common/tests/test_schemas.py`: revision required per engine, bad ref rejected, `sha256:` prefix normalised, params extra keys rejected, bounds.
  - `test_models_registry.py`: the default file has revisions.
  - `test_tasks_api.py` (integration): unknown key → 422 `INVALID_PARAMS`, valid params round-trip.
- Invariants: only the server API changes, and executors still use the REST API only (1).

**S2. Runner v2 and worker IPC** (D1–D3)
- `runners/base.py`: `StepControl`, `MemoryStats`, `EngineConfig`, `RunnerError(retryable)`.
- `runners/__init__.py`: `create_runner(engine, type, test_hooks)`.
- `runners/fake.py`:
  - accepts `prompt`/`messages`/`text`;
  - honours params (embedding `normalize`);
  - calls `ctl.checkpoint` per record;
  - `memory_in_use()` returns zeros;
  - a new test hook `sleep_steps` for tick tests.
- `worker/throttle.py`: `DutyCyclePacer(duty, clock, sleep)`, pure.
- `worker/process.py`:
  - `worker_main(engines, …)` creates the runner at `load`;
  - ops `probe` and `stats`;
  - a `_Ctl` that implements `StepControl` (pacer plus `{"tick": n}` messages, rate-limited);
  - `_call` consumes ticks, resets the inactivity deadline on each, and forwards `n` to an optional `on_progress` callback;
  - `RunnerError` serialises `retryable`;
  - `WorkerHandle(engines, …, load_timeout_s=1800, step_timeout_s=300)`;
  - `run(batch, params, on_progress=None)`;
  - `unload()` (reset, model forgotten, respawnable);
  - `probe()` (spawns, probes, shuts down).
- Tests (CI unit):
  - `test_runner_worker.py`: mechanical `WorkerHandle("fake")` → `WorkerHandle([Engine.FAKE])`; a tick keeps a long run alive past `step_timeout`; no tick → `WorkerCrashed`; a non-retryable `RunnerError` round-trips; an engine not in the set is refused; `probe` returns `{fake: version}`.
  - New `test_throttle.py` with a fake clock: d=0.5 sleeps t, d=1 never sleeps.
- Invariant 7 holds: `probe` imports engines in the child only.

**S3. Loop and CLI wiring** (D4, D10, D14 idle unload, carry-over 4)
- `settings.py` (new): `ExecutorSettings`.
- `loop.py`:
  - `Worker` protocol gains `run(batch, params, on_progress)` and `unload()`;
  - `_execute` parses params (`BAD_PARAMS`, non-retryable fail);
  - executor-side admission: if the model's engine is not advertised or `memory_gb > budget.memory_gb`, `release` with code `MODEL_NOT_ADMISSIBLE` (bounded by `MINION_RELEASE_LIMIT`);
  - `RunnerError.retryable` is honoured;
  - stamp `model`/`model_revision`;
  - `hb.records_done` is updated from ticks (`call_soon_threadsafe`);
  - idle unload timer in `run()`.
- `main.py`:
  - `--runner` stays hidden and dev-only (`fake`);
  - new `--engines auto|mlx,ollama,hf` (default `auto` = `WorkerHandle.probe()` ∩ requested);
  - no engine available → exit 2 with "no inference engine available (install extras: uv sync --extra mlx)";
  - `RegisterRequest.engines` comes from the probe.
- Tests (CI):
  - `test_loop.py`: params forwarded, `BAD_PARAMS` non-retryable, admission release, stamping, `retryable=False` → fail non-retryable, idle unload after a fake clock advance;
  - `test_cli.py`: `test_default_runner_is_real_engine_not_fake` rewritten as `auto` with no engines → exit 2 and fake never chosen implicitly;
  - `test_e2e.py` (integration, fake): still green, and output lines contain `model`.

### TASK-4.2: model cache, fetch process and worker sandbox (S4–S5)
**S4. Model cache and fetch subprocess** (D5–D7)
- `model_cache.py`:
  - `ModelCache(root, cap_bytes, fetch=_hf_fetch)`;
  - `async ensure(model) -> Path` (runs in a killable spawn subprocess; `CancelledError` → kill);
  - `evict_for(bytes, keep)`, `verify(path)`, `touch`;
  - `FetchEnv.capture()`;
  - `_hf_fetch` (in the child: `snapshot_download` plus sha256 verification against `files_metadata`);
  - Ollama models: `ensure` returns `None` (the runner pulls).
- `worker/process.py`: no allowlist change; add the forced offline variables to the worker env (D7); delete the TODO.
- `main.py`: capture `FetchEnv` and settings before `restrict_environ()`.
- `loop.py`: in `_run_batches`, call `cache.ensure` before `ensure_model`, then pass `model_path`.
- Error mapping:

  | Condition | Action | Code |
  |---|---|---|
  | network / timeout during fetch | release | `MODEL_DOWNLOAD_FAILED` |
  | hub 4xx/5xx | retryable fail | `MODEL_DOWNLOAD_FAILED` |
  | sha mismatch | retryable fail | `MODEL_INTEGRITY` |
  | cap too small | retryable fail | `MODEL_CACHE_FULL` |
  | disallowed files only / `auto_map` in `config.json` | non-retryable fail | `MODEL_UNSUPPORTED` |

- Tests:
  - CI unit `test_model_cache.py` with a fake fetch writing files: LRU order, cap eviction (keeps the in-use model), manifest/size verification → refetch, corrupt sha → `MODEL_INTEGRITY`, concurrent `ensure` serialised by the lock, cancel kills the fetch process, `FetchEnv` never logs the token or proxy credentials, allow-pattern constant excludes `.py`/`.bin`.
  - `model` marker: a real tiny download (`bge-small` bf16) verifies against hub metadata.
- Invariant 4 is unaffected (weights come from HF, not via the server). Invariant 6: a stop or cancel kills the fetch.

**S5. Worker sandbox** (D8; ADR-0009 stub)
- `worker/sandbox.py`: `build_profile(...)` (pure) and `apply(profile)` (ctypes `libsandbox` `sandbox_init` / `sandbox_free_error`).
- `worker_main` calls it at the first `load` with the model dir, the venv/`sys.prefix`/`sys.base_prefix`/site-packages paths and a worker tmp dir (`mkdtemp` created by the control side, passed in `load`).
- `ExecutorSettings.sandbox` (default True; False only with dev).
- `probe` fails → `SANDBOX_FAILED` → CLI exit 2.
- The fake runner gets test hooks `read_path` / `connect` (`test_hooks` only).
- Tests:
  - CI unit: `build_profile` contents (HOME denied, cache read-only, Ollama port allowed only when requested, paths escaped and quoted).
  - `macos` marker (fake engine, no GPU): the worker cannot read a dummy secret under the real HOME (test creates `~/.minion-sandbox-test/secret`, removes it after; stands in for `~/.aws`/Keychain files), cannot connect to `1.1.1.1:443`, cannot run `/usr/bin/security find-generic-password` successfully, *can* read the model dir and write the tmp dir, and IPC ticks still work.
- Docs: write `docs/adr/0009-offline-sandboxed-inference-worker.md` (status Proposed → Accepted at review). Its decisions are D5, D7 and D8, and it deviates from the paper (the paper has no sandbox).

### TASK-4.3: engine runners (S6–S8)
**S6. MLX runners and prompt helper** (D10, D14)
- `runners/prompts.py` (pure, no engine imports): `build_messages(record, params) -> list[Message]`, `validate_messages`, BAD_INPUT rules.
- `runners/mlx_completion.py`:
  - `mlx_lm.load(path)`;
  - `tokenizer.apply_chat_template(..., add_generation_prompt=True)` → token ids;
  - truncation check → `PROMPT_TOO_LONG` lines;
  - `BatchGenerator(model, max_tokens, completion_batch_size=budget.max_batch_size, prefill_batch_size=min(8, batch), sampler=make_sampler(temp, top_p))`;
  - reset the wired limit (D14);
  - `insert(prompts)` then a `next()` loop with `ctl.checkpoint(done)` after every step;
  - detokenize per uid; `close()` in `unload`;
  - `mx.random.seed` when a seed is given;
  - Metal OOM (`RuntimeError` text match) → `RunnerError(OOM)`.
- `runners/mlx_embedding.py`: `mlx_embeddings.load(path)`; `generate(model, proc, texts=batch, max_length=512)` → `text_embeds`; `normalize` param; token counts from the attention mask.
- `pyproject` extras and mypy overrides (D11); markers and auto-skip in `conftest.py` (D12).
- Tests:
  - CI unit `test_prompts.py`.
  - `gpu` + `model` (tiny `mlx-community/Qwen2.5-0.5B-Instruct-4bit`, `bge-small-en-v1.5-bf16` from `deploy/models.yaml`): N records in → N lines out; greedy determinism across two runs; `checkpoint` called at least once per step; peak memory below `memory_gb`; the wired limit is not above budget after load; an in-flight `interrupt()` frees memory (the worker exits); full executor e2e with a real MLX model through the sandbox.

**S7. Ollama runner** (CI-testable)
- `runners/ollama.py` (`httpx` only):
  - loopback URL check;
  - `load` = `/api/show`, or `/api/pull` (`stream=false`), then digest check against `revision` via `/api/tags` (mismatch → `RunnerError(MODEL_INTEGRITY, retryable=False)`);
  - completion = `/api/chat` with `options{temperature, top_p, seed, num_predict}`, concurrency `min(len(batch), max_batch_size)` via a small thread pool, `checkpoint` per finished request;
  - embedding = `/api/embed` with list input;
  - usage from `prompt_eval_count` / `eval_count`;
  - `memory_in_use` from `/api/ps`;
  - `unload` = `keep_alive: 0`;
  - `probe` = `/api/version`.
- Tests:
  - CI unit with `httpx.MockTransport`: request bodies (no unknown options), digest mismatch, a non-loopback URL refused, 404 → pull, transport error → `RunnerError(ENGINE_UNAVAILABLE, retryable)`.
  - `ollama` marker: a live tiny model.

**S8. HF transformers runner (mps)**
- `runners/hf.py`:
  - `AutoTokenizer` / `AutoModelForCausalLM` / `AutoModel` with `from_pretrained(path, local_files_only=True, trust_remote_code=False, use_safetensors=True, dtype=float16)`, then `.to("mps")`;
  - `set_per_process_memory_fraction`;
  - completion via `generate` with left padding, plus a `LogitsProcessor` that calls `ctl.checkpoint` each step;
  - embedding pooling from the sentence-transformers config files (D11);
  - MPS OOM → `RunnerError(OOM)`;
  - `memory_in_use` from `torch.mps.current_allocated_memory` / `driver_allocated_memory`.
- Tests:
  - CI unit: the pooling-config parser (pure).
  - `gpu` + `model`: `bge-small` hf vs mlx cosine ≥ 0.99 on 32 texts (cross-engine correctness); a completion smoke test.

### TASK-4.4: bench, evidence and docs (S9–S10)
**S9. `minion-executor bench`** (D13)
- `bench.py`: `BenchConfig`, `BenchResult`, `run_bench(cache, worker_factory, entries, …)` (engine-agnostic: it uses only `WorkerHandle`, `ModelCache` and `stats`), `synthetic_records(type, n, seed)`, `render_table`.
- `main.py`: the `bench` command.
- `deploy/bench-models.yaml`.
- Tests:
  - CI unit: bench with the fake engine (dev) produces every metric field, `records_per_h` math, an unavailable engine reported and not raised, JSON schema.
  - Mac manual run, which is the AC#1 evidence: `uv sync --extra mlx --extra hf; brew install ollama; ollama serve; uv run minion-executor bench --model qwen2.5-0.5b-{mlx,hf,ollama} --model minilm-{mlx,hf,ollama} --batch-sizes 1,8,32`.

**S10. Docs** (docs-keeper; facts from the implementer and verifier)
- SPEC:
  - FR-4a (`revision`, ref formats);
  - FR-4/FR-6 (typed params, output fields, `PROMPT_TOO_LONG`);
  - FR-12 (cache defaults);
  - FR-13 status and timeouts;
  - FR-16 (probe, executor-side re-check);
  - §6 error codes `INVALID_PARAMS`, `BAD_PARAMS`, `MODEL_*`, `SANDBOX_FAILED`;
  - S-7 (safetensors only, offline sandboxed worker);
  - §4.6 status (static duty pacer, MLX/MPS limits).
- ARCHITECTURE:
  - §2.2 as built (runners, `model_cache`, `settings`, sandbox, fetch process, env table, the new error-mapping rows);
  - §3.3 (download step);
  - §9 ADR index.
- ADR-0005: the "Revisit" outcome with the bench table and a link to `docs/bench/m3-<chip>.md`.
- ADR-0009: Accepted.
- CLAUDE.md:
  - commands: `uv sync --extra mlx --extra hf`, `bench`, markers, `MINION_TEST_MODELS=1 uv run pytest -m "gpu or model"`;
  - Executor-inference row unchanged.
- `.env.example`: executor settings.
- TASK-9 notes.
- PR body attaches the bench output, as CLAUDE.md requires.

## Invariants check
- **(1) Executors never talk to PostgreSQL.** Unchanged. The executor talks to the server REST API, S3 presigned URLs, the HF hub (fetch process) and the loopback Ollama daemon only.
- **(2), (3) Single ownership and owner-only mutation.** Untouched. There are no protocol changes besides params validation.
- **(4) Large payloads bypass the server.** Holds. Weights come from HF or Ollama, not the server.
- **(5) Idempotent execution.** Outputs stay attempt-keyed. Temperature 0 by default. `model_revision` stamping makes cross-revision mixes visible.
- **(6) Yield to the user.** Engine memory limits and wired-limit reset, static duty pacing, `nice`, idle unload, and killable fetch and worker. No sleep prevention. The Ollama caveat is documented.
- **(7) Inference only in the worker subprocess.** All engine imports, including the probe, happen in the worker. Nothing outside `runners/` imports `mlx`, `transformers`, `torch` or `ollama`. huggingface_hub is used only in the fetch subprocess.

## Risks
- **mlx-lm and mlx-embeddings API churn.** `BatchGenerator` in 0.32 has `insert`/`next`/`close` and `Response(uid, token, finish_reason)`. Mitigation: exact pins and a `gpu` test per runner.
- **mlx-embeddings 0.1.0 is young and pulls in `mlx-vlm`.** That brings opencv, fastapi, mlx-audio, datasets and cryptography: a large supply-chain and install footprint inside the worker. Mitigation: pinned, sandboxed and offline. Revisit with a slim BERT path if it hurts.
- **`sandbox_init` is a deprecated API.** The SBPL rules need empirical tuning (Metal compiler service, Python lazy imports, socketpair IPC under `deny network*`). Mitigation: fail-closed, the `macos` test tier and the optional macOS CI job.
- **Ollama is not installed on this Mac.** AC#1 needs `brew install ollama`. Ollama's MLX path applies only to safetensors models, and GGUF fp16 runs llama.cpp, so the bench must record which backend Ollama used.
- **Quantization parity.** The bench uses bf16/fp16 everywhere. The 4-bit MLX numbers are reported separately and never compared head-to-head.
- **Uncertain specifics.**
  - Whether the Ollama digest format from `/api/tags` matches `sha256:` + 64-hex or a bare 64-hex string.
  - MLX `set_memory_limit` semantics (whether it raises or blocks).
  - `ru_maxrss` under-reports unified (Metal) memory, so the engine-reported peak is the primary metric.
- **Torch and transformers make the `hf` extra large (GBs).** It stays optional.
- **Run-time accounting.** Large-model downloads under an acquired lease count against max lease (6 h) and heartbeat continues, which is fine. A pre-fetch outside a lease is future work.

## Open questions (for the orchestrator)
1. **Revision snapshot (D9).** Snapshot `revision` into `minion_task` (migration) rather than resolving it from the registry at acquire? Recommendation: no snapshot in M3; stamp the revision into the output, plus the operator rule.
2. **Prompt-too-long records.** Per-record `error` lines (D10) rather than failing the subtask? Recommendation: per-record, which needs a SPEC FR-6 amendment.
3. **Static duty pacer.** Ship the static pacer in M3 (D14) so real engines respect `gpu_duty_cycle` before the M4 governor? Recommendation: yes, because invariant 6 otherwise regresses the moment real engines land.
4. **Credential-bearing proxies.** Allowed only for the fetch process, never for the worker (D7). Confirm.
5. **macOS CI job.** Add a `macos-14` job for the `macos` (sandbox) tier? Recommendation: yes, at about 10x runner cost, sandbox tests only.
6. **TASK-9 #2.** Keep it in TASK-9, as proposed.

## Proposed sub-tasks (not created; orchestrator decides)
```
backlog task create "M3.1: Contracts and worker foundation (Runner v2, params allow-list, revision pin, engine probe)" -p TASK-4 -l M3 --priority high -d "Plan steps S1-S3 of TASK-4 (D1-D4, D9, D10, D14 idle unload). Delivers TASK-9 #1 (schema) and #3." --ac "ModelEntry requires revision (40-hex mlx/hf, 64-hex digest ollama); unknown task params rejected with 422 INVALID_PARAMS" --ac "Runner v2: StepControl ticks keep long runs alive past step timeout; no ticks -> WorkerCrashed; RunnerError.retryable round-trips" --ac "run --engines auto probes engines in a worker subprocess; none available -> exit 2; fake never implicit" --ac "Output lines carry model and model_revision; idle worker unloaded after MINION_EXECUTOR_IDLE_UNLOAD_S"
backlog task create "M3.2: Model cache, fetch subprocess and worker sandbox" -p TASK-4 -l M3,security --priority high --dep TASK-4.1 -d "Plan steps S4-S5 of TASK-4 (D5-D8). ADR-0009." --ac "Cache: pinned revision, safetensors-only allow-patterns, sha256 verified, LRU eviction under MINION_EXECUTOR_MODEL_CACHE_GB, cancel/SIGTERM kills fetch" --ac "Worker is offline and sandboxed: cannot read HOME secrets, open non-loopback sockets or reach securityd; fail-closed if sandbox_init fails" --ac "HF_TOKEN/proxy/CA reach only the fetch subprocess, never the worker or logs" --ac "ADR-0009 written"
backlog task create "M3.3: MLX, Ollama and HF runners" -p TASK-4 -l M3 --priority high --dep TASK-4.2 -d "Plan steps S6-S8 of TASK-4 (D10, D11, D12, D14)." --ac "MLX completion (BatchGenerator) and embedding runners pass gpu+model tests with tiny models; wired limit within budget" --ac "Ollama runner (loopback only, digest pinned) passes MockTransport unit tests in CI" --ac "HF mps runner: bge-small embeddings match MLX (cosine >= 0.99)" --ac "CI stays green without engine extras (markers auto-skip; mypy strict clean)"
backlog task create "M3.4: minion-executor bench, ADR-0005 confirmation and docs" -p TASK-4 -l M3 --priority high --dep TASK-4.3 -d "Plan steps S9-S10 of TASK-4 (D13)." --ac "bench reports tok/s, records/h, peak memory for mlx vs ollama vs hf on the same model (results in docs/bench/)" --ac "ADR-0005 updated with bench numbers; SPEC/ARCHITECTURE/CLAUDE.md match code"
```
Also: `backlog task edit 9 --notes "Items #1 (revision pin) and #3 (params allow-list) delivered by TASK-4.1; #2 (Range-signed GET, PUT size cap) remains."`
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Carry-over from M1 (TASK-2.3 reviews): (1) Runner protocol needs per-step cancel/pause hook (duty cycling), streaming/progress, memory_in_use() before real engines; (2) worker sandboxing (sandbox-exec profile or separate user) — compromised model code can read Keychain/~/.aws as same user; (3) worker env allowlist must gain proxy/CA (HTTPS_PROXY, SSL_CERT_FILE/REQUESTS_CA_BUNDLE), HF_HUB_*/HF_ENDPOINT/HF_TOKEN, OLLAMA_HOST handling — decide on credential-bearing proxy URLs (TODO(M3) in worker/process.py); (4) default --runner mlx currently exits 2 'unknown runner' — M3 must ship it; (5) separate load/run timeouts sized for large MLX loads; (6) TASK-9 model revision pinning.

Orchestrator decisions on plan open questions (2026-10-15): (1) model revision NOT stored on task in M3 — output lines carry model + model_revision; new revision = new model id; (2) over-long prompt → per-record error line (SPEC FR-6 change), not subtask failure; (3) static GPU duty-cycle throttle via per-step hook ships in M3 (invariant 6); (4) credential-bearing proxy URLs allowed ONLY for the fetch subprocess, never worker/logs; (5) NO macOS CI job for now (cost decision left to user) — sandbox/gpu/model tests run locally during verify. Also: evaluate avoiding mlx-embeddings if its mlx-vlm dependency tree (opencv, fastapi, mlx-audio) cannot be trimmed — own pooling on mlx is acceptable (record in ADR). Ollama not installed on this Mac: needed for AC#1 bench (TASK-4.4) — ask user before installing.
<!-- SECTION:NOTES:END -->
