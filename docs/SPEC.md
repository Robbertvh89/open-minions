# open-minions — Specification

Status: **Draft v0.2** · 2026-10-15 (v0.2: MLX default engine, partial capacity allocation)
Source: Li, Xing, De Silva, Flint, Yuan, Suchter. *Minions: A Dynamic and Distributed Virtual GPU Cloud
Built on Idle Apple Silicon Devices.* arXiv:2610.12970 (2026).

Items marked **[paper]** come directly from the paper. Items marked **[ours]** are design decisions the
paper leaves open; each should get an ADR if contested.

---

## 1. Problem

Centralized GPU clusters are expensive, oversubscribed and slow to expand; batch inference jobs can
queue for 10+ hours **[paper]**. Meanwhile, Apple Silicon Macs on employees' desks have capable GPUs
that sit idle most of the day. open-minions pools those idle GPUs into a *virtual GPU cloud* for
**batch inference**, with no new hardware and zero incremental infra cost.

## 2. Goals / Non-goals

### Goals
- G1. Run batch **embedding** and **LLM completion** jobs on a fleet of volunteered Macs.
- G2. Pull-based, decentralized execution: no central scheduler assigning work **[paper]**.
- G3. Tolerate arbitrary executor churn (sleep, network loss, power-off, user stop) without losing work **[paper]**.
- G4. Let executor owners bias their machine toward specific team/project/task (dynamic task binding) **[paper]**.
- G5. Turn queue time from hours into minutes for typical jobs **[paper, qualitative]**.
- G6. Observable: task progress, fleet size, throughput visible in Grafana **[paper]**.
- G7. **Coexist with the owner's work.** Participants donate only part of their Mac's capacity and keep
  using Teams, Outlook, Slack, browsers, IDEs without noticeable slowdown **[ours]**. See §4.6.
- G8. Maximize throughput per Mac by using the fastest Apple Silicon engine (MLX, batched) **[ours, ADR-0005]**.

### Non-goals (v1)
- Training / fine-tuning (paper lists as future work).
- Sharding a single model across multiple devices — one subtask runs entirely on one Mac.
- Low-latency / online serving. This is a batch system.
- Cross-organization federation (future work; see §10).
- Non-Apple executors (architecture shouldn't preclude them, but not supported).

## 3. Concepts & glossary

| Term | Meaning |
|---|---|
| **Task** | A user-submitted batch job, identified by `(team, project, task_name)`. Has a type, model, input location, output location, parameters. |
| **Subtask** | A chunk of a task's input, executed atomically by one executor. Identified by `(team, project, task_name, subtask_id)`. |
| **Executor** | Agent process on a Mac that pulls and runs subtasks. |
| **Acquirement** | The record proving an executor owns a subtask attempt. Exactly one may exist per subtask at a time. |
| **Attempt** | One execution try of a subtask. Incremented on every reclaim. |
| **Preference** | An executor's optional `(team, project, task_name)` filter for dynamic task binding. |
| **Watchdog** | Process that reclaims subtasks from executors with stale heartbeats. |

## 4. Functional requirements

### 4.1 Task submission (Server)
- FR-1. Users create tasks via REST API and web UI **[paper]**.
- FR-2. Server validates parameters synchronously; rejects with `422` on invalid input **[paper]**.
- FR-3. Server partitions the task into subtasks **asynchronously** and persists them **[paper]**. Task
  status is `SPLITTING` until done, then `PENDING`.
- FR-4. Supported task types **[paper]**:
  - `embedding` — input: JSONL with `id` + `text` field.
  - `completion` — input: JSONL with `id` + `prompt` (or `messages`);
    params (allow-list, unknown keys rejected with `422 INVALID_PARAMS`): `temperature` (0..2,
    default 0), `top_p` ((0,1], default 1), `max_tokens` (1..4096, default 256), `seed` (int or
    null), `system` (string, at most 8000 chars). Embedding params: `normalize` (bool, default true).
- FR-4a. Each task names a `model` from the deployment's **model allow-list** (`models.yaml`). An entry
  defines `model_id`, `type`, `engine` (`mlx` | `ollama` | `hf`; plus `fake`, a test/dev-only engine for the fake runner, never in a production allow-list), engine-specific reference (HF repo for
  MLX/HF, tag for Ollama), a pinned `revision` (40-hex commit SHA for MLX/HF, 64-hex manifest digest
  for Ollama; optional only for `fake`), and `memory_gb` (peak resident estimate incl. KV cache at default batch size).
  Default engine is `mlx` **[ours, ADR-0005]**; the paper used HF transformers + Ollama.
- FR-5. Input format **[ours]**: one or more JSONL objects under an S3 prefix. Splitting is by record
  count (`records_per_subtask`, default 500). Each subtask stores `(input_uri, byte_offset, byte_length)`
  or a pre-split object key — see ARCHITECTURE §5.2.
- FR-6. Output **[ours]**: one JSONL object per subtask at
  `{output_prefix}/{subtask_id:06d}.jsonl`, each line `{id, embedding|completion, model,
  model_revision, ...}`. A record that cannot be processed (e.g. a prompt longer than the model
  context minus `max_tokens`) yields a per-record line `{id, error: "PROMPT_TOO_LONG", model,
  model_revision}` instead of failing the subtask. Operators never change the revision of an
  existing `model_id`; a new revision gets a new id.
  Written to an attempt-scoped key first and promoted on completion (idempotency).
- FR-7. Users can query task status, per-subtask status, progress counts, and the output location **[paper]**.
- FR-8. Users can cancel a task; pending subtasks become `CANCELLED`, running ones are told to stop on
  their next heartbeat **[ours]**. Status: implemented (TASK-3.2).

### 4.2 Execution (Executor)
- FR-10. Executor registers with the server using a pre-issued token and reports host capabilities
  (chip, unified memory, macOS version, available engines) and its configured capacity profile **[ours]**.
- FR-11. Executor loop **[paper]**: poll for eligible subtask → acquire → download input → ensure model
  cached → run inference → upload output → report result.
- FR-12. Models are downloaded on first use and cached locally **[paper]**. Cache size is bounded
  (LRU eviction, configurable, default 50 GB) **[ours]**.
- FR-13. Engines **[ours, ADR-0005]**:
  - `mlx` (default): completions via `mlx-lm` batched generation (`BatchGenerator`), embeddings via a small
    BERT-family encoder written on `mlx` (not the `mlx-embeddings` package, ADR-0005 addendum;
    pooling from the model's sentence-transformers config, CLS by default). Output lines: embedding
    `{id, embedding, tokens}`; completion `{id, completion, finish_reason, prompt_tokens,
    completion_tokens}` (the loop adds `model`, `model_revision`). A malformed record (neither or
    both of `prompt`/`messages`, wrong types, empty content, a chat ending in an assistant turn)
    fails the subtask with non-retryable `BAD_INPUT`; an over-long prompt gives a per-record
    `PROMPT_TOO_LONG` line; embedding input over 512 tokens is truncated. A Metal out-of-memory
    error is a retryable `OOM`. The MLX runner keeps `mx.set_memory_limit` at the memory budget and
    resets the wired limit to `min(budget, recommended)` after `BatchGenerator` raised it. All
    records of a subtask are processed in batches sized by the capacity budget; the pacer runs
    after every generation step. Install with `uv sync --package minion-executor --extra mlx`.
    BERT encoders only (`model_type: bert`): XLM-R is `MODEL_UNSUPPORTED` (its position ids differ),
    as is `mean_sqrt_len` pooling. The prompt limit is the smaller of the executor's
    `max_prompt_tokens` and the model's `max_position_embeddings`.
  - `ollama` (fallback, e.g. GGUF-only models; **opt-in**: `--engines auto` never probes it, use
    `--engines ollama` or a list containing it): local Ollama HTTP API **[paper]**. The runner talks
    to a loopback daemon only, pulls missing models with `/api/pull` (bounded below the worker load
    timeout) and verifies the pinned manifest digest after load and again after every run
    (`MODEL_INTEGRITY`, executor-local: the subtask is released and no output is stamped). The
    daemon runs outside the sandbox and the capacity budget, and `/api/pull` writes to `~/.ollama`
    outside the model-cache cap (residual confused-deputy risk, ADR-0009). Refs need an explicit
    tag and no registry host. Deviations from the other engines (ADR-0009): the daemon cannot return
    unnormalised embeddings, so the server rejects `normalize=false` for ollama models (422
    `INVALID_PARAMS`) and the runner fails it non-retryably (`PARAMS_UNSUPPORTED`); embedding
    `tokens` is `prompt_eval_count` of one `/api/embed` call per text and can under-report when
    Ollama's prompt cache holds a shared prefix; over-long prompts are not detectable (no
    `PROMPT_TOO_LONG` lines; `num_ctx` is the constant `max_prompt_tokens + 4096`, which costs KV cache for that window, ADR-0009). Every request carries a
    bounded `keep_alive` (`MINION_EXECUTOR_IDLE_UNLOAD_S`, at most 300 s, 60 s when disabled) and
    the worker asks the runner to unload (`keep_alive: 0`) before it is killed, on idle unload and on
    a model switch, under a total deadline that always ends in a kill; after a hard kill (stop,
    cancel, 409) the control side sends a best-effort `keep_alive: 0` (short timeout, no env
    proxies). Load phase (show, pull, tags): 5xx and transport errors are releases
    (`ENGINE_UNAVAILABLE`), 4xx a non-retryable failure. Run phase: 4xx is a non-retryable failure,
    except 404 (model removed locally), which is a release; 5xx a retryable failure (`OOM` when
    the daemon reports too little memory); a transport error a release (`ENGINE_UNAVAILABLE`).
    Refs allow at most one `/` (`namespace/name:tag`).
  - `hf` (fallback): HuggingFace transformers on `mps` **[paper]**. Output lines as for `mlx`
    (embedding `{id, embedding, tokens}`; completion `{id, completion, finish_reason,
    prompt_tokens, completion_tokens}`), safetensors only, local files only, remote code off.
    `torch.mps.set_per_process_memory_fraction` caps the allocator at the memory budget; an MPS
    out-of-memory error is a retryable `OOM`. Sampler settings are explicit, so the model's
    `generation_config.json` cannot change them. Install with `--extra hf`.
  - Inference runs in a worker subprocess; the loaded model is kept warm across consecutive subtasks
    of the same model and unloaded when the budget drops or the executor idles > 10 min.
- FR-14. Heartbeat every **5 minutes** while running a subtask **[paper]**. Heartbeat response may carry
  `cancel=true`. Status: implemented (TASK-3.3); interval `--heartbeat-interval` / `MINION_EXECUTOR_HEARTBEAT_INTERVAL_S` (default 300 s, min 1 s).
- FR-15. Executor processes one subtask at a time by default (`concurrency=1`) **[ours]**.
- FR-16. Executor only acquires subtasks whose model `memory_gb` fits its **current** memory budget
  (§4.6) and whose engine it has installed **[ours]**.
- FR-17. Graceful stop: on SIGTERM / user stop, finish-or-release the current subtask within a grace
  period (default 30 s), then exit. Releasing = explicit `release` call so it re-queues immediately
  instead of waiting for the watchdog **[ours]**.
- FR-18. When and how much the executor works is governed by the capacity rules in §4.6 (the paper
  only lets users stop the executor entirely).

**Implementation status (M1, TASK-2.3).** Implemented: FR-10 (register, token from
`$MINION_EXECUTOR_TOKEN` or Keychain item `open-minions-executor`), FR-11 (loop with fake runner
only), FR-15 (one subtask at a time), FR-17 partially (SIGTERM/SIGINT stop, release of the in-flight
subtask, exit 0; **no 30 s grace period**: the worker is killed immediately, which conflicts with the
"finish-or-release within grace" wording). Not yet implemented (as of TASK-2.3; FR-12 and FR-13 are now done, see the M3 updates): FR-16 engine filtering beyond the `engines` list sent on acquire, FR-18/§4.6
governor (M4). ARCHITECTURE §2.2 describes the executor as built.

**M3 update (TASK-4.1 S3).** `run --engines auto|mlx,ollama,hf` (default `auto`, which probes mlx and hf only: Ollama is opt-in, see S-10) probes engines in a worker subprocess and registers/acquires with the available ones; none available exits 2, and `fake` is never implicit (needs `--engines fake` plus `MINION_EXECUTOR_DEV=1`). The loop validates task params (`BAD_PARAMS`, non-retryable), releases subtasks whose engine is not advertised or whose model exceeds the memory budget (`MODEL_NOT_ADMISSIBLE`; always released, bounded server-side by `MINION_RELEASE_LIMIT`). `MODEL_NOT_ADMISSIBLE` and `ENGINE_UNAVAILABLE` (engine missing or not yet implemented on this Mac, also released) are logged executor-side only, because `release` carries no reason body. The loop also honours `RunnerError.retryable`, stamps `model` and `model_revision` on every output line, treats per-record `PROMPT_TOO_LONG` error lines as records (not a subtask failure), and unloads the worker after `MINION_EXECUTOR_IDLE_UNLOAD_S` (600; 0 disables) without work, including after acquire errors such as a server outage (FR-13, invariant 6). The profile's `gpu_duty_cycle` is applied by the worker's static pacer.

**M3 update (TASK-4.2, model cache; FR-12).** MLX/HF models are fetched by the control side in a killable, Seatbelt-sandboxed `spawn` subprocess (network allowed; writes only to its staging dir; no HOME reads, exec, LaunchServices or AppleEvents), pinned to `ModelEntry.revision` and verified (LFS sha256, git blob sha1 for non-LFS files, no symlinks, no `auto_map` in any JSON) before the worker, which is offline and sandboxed (ADR-0009), loads it. Settings: `MINION_EXECUTOR_MODEL_CACHE_DIR` (absolute, `~` expanded), `MINION_EXECUTOR_MODEL_CACHE_GB` (LRU cap, default 50), `MINION_EXECUTOR_MIN_FREE_DISK_GB` (default 10: a download must leave at least max(this, 10% of the disk) free), `MINION_EXECUTOR_DOWNLOAD_TIMEOUT_S`; `MINION_EXECUTOR_FETCH_SANDBOX=0` is dev-only (needs `MINION_EXECUTOR_DEV=1`). A model that is loaded is pinned with a shared `flock`, so no executor process sharing the cache evicts it. Provisioning outcomes (release = `release` call, no retry budget burnt; fail = `fail` with the code):

| Code | Outcome | When |
|---|---|---|
| `MODEL_DOWNLOAD_FAILED` | release | network down, proxy error, fetch process died, download timeout |
| `MODEL_DOWNLOAD_FAILED` | fail, retryable | hub answered with another HTTP error (404, 5xx) |
| `MODEL_DOWNLOAD_FAILED` | release, remembered 1 h | hub 403, or 401 when no `HF_TOKEN` is set (this Mac has no valid token) |
| `MODEL_DOWNLOAD_FAILED` | release, not remembered | hub 401 with `HF_TOKEN` set (bad, expired or revoked token on this Mac; a rotated token works on the next assignment) |
| `MODEL_INTEGRITY` | fail, retryable | sha256/blob-sha1/size mismatch, file set differs from the hub listing, symlink, hub returned another revision |
| `MODEL_UNSUPPORTED` | fail, non-retryable | no safetensors, disallowed file type, `auto_map` / bad JSON (would need remote code) |
| `MODEL_CACHE_FULL` | release, remembered 10 min | model larger than `MODEL_CACHE_GB` |
| `MODEL_CACHE_FULL` | release (not remembered) | the space is held by models other executor processes have loaded |
| `MODEL_CACHE_FULL` | release, remembered 10 min | free-disk reserve not met, or `ENOSPC` while downloading/sealing (staging is deleted first) |
| `MODEL_CACHE_FULL` | internal retry once | the only blocker is this executor's own loaded model: the worker is unloaded and the fetch retried |
| `SANDBOX_FAILED` | release, remembered 1 h | the fetch sandbox could not be applied (fail closed: nothing is downloaded unconfined) |
| `LOCAL_IO_ERROR` | release | the cache directory itself is unreadable or unwritable (`OSError` from the cache) |

A 401 with a token set is released rather than failed because the credential is a property of this Mac, not of the subtask; it is not remembered so that fixing the token takes effect at once. "Remembered" means the executor keeps a bounded in-memory (model, revision) to reason map, so repeated assignments of that model are released immediately without network or disk work; the server's `MINION_RELEASE_LIMIT` still bounds how often one subtask bounces. A full or unusable local disk is a property of this Mac, not of the subtask, which is why it is a release and not a (retry-burning) failure.

Errors from the worker and engines (the full set of codes the executor reports; payload text is never included):

| Code | Outcome | When |
|---|---|---|
| `MODEL_NOT_ADMISSIBLE` | release | the assigned model's engine is not advertised by this executor, or its `memory_gb` exceeds the budget |
| `ENGINE_UNAVAILABLE` | release | the engine cannot run here right now (e.g. the Ollama daemon is unreachable at load) |
| `ENGINE_NOT_ALLOWED` | fail, retryable | the worker was asked to load an engine outside its allowed set (defence in depth; the loop checks first) |
| `SANDBOX_FAILED` | release | the worker sandbox could not be applied (worker exit 78) |
| `BAD_PARAMS` | fail, non-retryable | task params fail the executor's re-validation, or do not match the task type |
| `BAD_INPUT` | fail, non-retryable | the input object is not valid JSONL records |
| `MODEL_NOT_FOUND` | fail, non-retryable | the model directory is missing from the local cache at load |
| `MODEL_UNSUPPORTED` | fail, non-retryable | also: architecture or pooling the runner does not implement (XLM-R, `mean_sqrt_len`) |
| `OOM` | fail, retryable | Metal/MPS (or Ollama) out of memory |
| `ENGINE_STALLED` | fail, retryable | MLX generation made no progress |
| `ENGINE_REJECTED` | fail, non-retryable | Ollama answered 4xx at run time |
| `ENGINE_ERROR` | fail, retryable | Ollama answered 5xx at run time (404 = model gone: release) |
| `PROMPT_TOO_LONG` | per record | written as an output line for that record; the subtask still succeeds (FR-6) |

**M2 update (TASK-3.3).** FR-14 is implemented (heartbeater, 429 non-fatal, cancel and 409 interrupt the worker). FR-17 remains partial.

### 4.3 Acquisition protocol **[paper]**
- FR-20. Ownership is claimed by inserting into `minion_subtask_acquirement`, which has a UNIQUE
  constraint on `(team, project, task_name, subtask_id)`.
- FR-21. Exactly one insert wins; losers get no row (`ON CONFLICT DO NOTHING`) and try the next candidate.
- FR-22. Dynamic task binding: if the executor has a preference, matching subtasks are dequeued first;
  otherwise (or if none match) fall back to the **global FIFO** queue (ordered by task `created_at`,
  then `subtask_id`).

### 4.4 Fault tolerance (Watchdog)
- FR-30. Watchdog monitors heartbeats **only for executors currently running subtasks** **[paper]**.
- FR-31. If no heartbeat for `heartbeat_timeout` (default 15 min = 3 missed beats) **[ours]**, the
  watchdog deletes the acquirement, archives it as `EXPIRED`, and sets the subtask back to
  `PENDING` (or `FAILED` once the budget is spent) **[paper semantics]**. Reclaim does not touch
  `attempt`: the next acquire increments it, so attempt-scoped output keys stay unique.
- FR-31a. Hard lease: an acquirement older than `max_lease` (`MINION_MAX_LEASE_S`, default 6 h) is reclaimed as `EXPIRED` (code `MAX_LEASE_EXCEEDED`, consumes budget) even while it heartbeats, so a looping executor cannot hold a subtask forever **[ours]**.
- FR-32. The retry budget is the number of `FAILED` + `EXPIRED` attempts; once it reaches
  `max_attempts` (default 3) the subtask becomes `FAILED`; task becomes `FAILED` or `PARTIAL`
  depending on policy. A voluntary `release` does not consume the budget **[ours]**, but a
  subtask already released `MINION_RELEASE_LIMIT` times (default 10) records further releases as
  a retryable `FAILED` attempt (`RELEASE_LIMIT`), so a misclassified error cannot livelock the
  fleet.
- FR-33. Executor-reported errors: retryable (OOM, download failure) → requeue; non-retryable (bad
  input) → `FAILED` immediately **[ours]**.
- FR-34. A late result from a reclaimed owner is rejected (`409 STALE_ACQUIREMENT`) **[ours]**.

**Implementation status (M2, TASK-3.1–3.3).** Implemented: FR-30, FR-31, FR-31a, FR-32, FR-34, and FR-8 cancel. Reclaim uses `SKIP LOCKED` + re-check, not `DELETE ... RETURNING`; deviations are recorded in ADR-0008. Reclaim codes: `HEARTBEAT_TIMEOUT`, `MAX_LEASE_EXCEEDED` (both `EXPIRED`, retryable). Settings: `MINION_HEARTBEAT_TIMEOUT_S` (900), `MINION_MAX_LEASE_S` (21600), `MINION_WATCHDOG_INTERVAL_S` (60), `MINION_WATCHDOG_BATCH_SIZE` (500), `MINION_WATCHDOG_MAX_BACKOFF_S` (300), `MINION_DB_COMMAND_TIMEOUT_S` (30), `MINION_WATCHDOG_DB_POOL_SIZE` (2), `MINION_CANCEL_SWEEP_WINDOW_S` (3600), `MINION_RELEASE_LIMIT` (10), `MINION_HEARTBEAT_MIN_INTERVAL_S` (1), `MINION_EXECUTOR_HEARTBEAT_INTERVAL_S` (300, executor). Keep `MINION_HEARTBEAT_TIMEOUT_S` at least 2–3× the heartbeat interval.

### 4.5 Observability
- FR-40. Grafana dashboards over PostgreSQL: tasks by status, subtask throughput, active executors,
  reclaims/hour, per-executor completions **[paper]**.
- FR-41. Server exposes `/healthz` and `/metrics` (Prometheus) **[ours]**.
- FR-42. Fleet view shows each executor's active profile, budget and paused reason **[ours]**.

### 4.6 Partial capacity allocation **[ours]**

Participants must be able to donate *part* of their Mac so it stays responsive for normal work.
macOS has no GPU partitioning or GPU quota API, so the executor enforces the budget itself through a
**capacity governor** (ARCHITECTURE §3.6).

- FR-50. **Budget dimensions.** A capacity budget consists of:
  | Dimension | Unit | Enforcement |
  |---|---|---|
  | `memory_gb` | GB of unified memory | Admission (only models with `memory_gb` ≤ budget are acquired) + MLX allocator limits (`mx.set_memory_limit`, `mx.set_cache_limit`) in the worker |
  | `gpu_duty_cycle` | 10–100 % | Governor pauses between generation steps / embedding batches so GPU busy-time ≈ target share |
  | `max_batch_size` | records | Caps batch size → caps KV-cache growth and per-step GPU burst length |
  | `cpu_priority` | `background` \| `low` \| `normal` | Worker runs with background QoS (`taskpolicy -b`) / `nice`; tokenizer & I/O threads capped |
- FR-51. **Profiles.** Built-in profiles; all values overridable in `~/.config/minion/executor.toml`:
  | Profile | memory | GPU duty | max batch | CPU | Intended use |
  |---|---|---|---|---|---|
  | `light` | 25 % of RAM | 30 % | 4 | background | Owner actively working, video calls |
  | `balanced` | 40 % of RAM | 50 % | 8 | low | Normal office day |
  | `full` | 70 % of RAM | 100 % | 32 | normal | Owner away, overnight |
  | `custom` | user-defined | | | | |
  Memory share is capped at 75 % of RAM regardless of config (headroom for macOS + apps).
  **Status (TASK-5.1):** the built-in profiles, `~/.config/minion/executor.toml` overrides
  (`MINION_EXECUTOR_CONFIG`, refused when not owned by the user or group/world-writable; unknown keys
  and bad values exit 2) and the `custom` profile (`[profiles.custom]`, all dimensions required) are
  implemented, with the 75 % cap on every profile. `cpu_priority` is part of the budget on the wire
  (optional for older clients) and derives the worker/fetch `nice` (15 / 10 / 0); background QoS
  (`taskpolicy -b`) lands in M4.3. FR-52..FR-55 enforcement is M4.2..M4.5; `POST /executor/presence`
  exists (FR-55); FR-56 is partially met (`run --profile` only). Proposed (ADR-0006 D10): there is no
  implicit `balanced` default; `run` without `--profile` is adaptive (FR-52), and `--profile
  <name>` pins a profile.
- FR-52. **Adaptive switching.** The owner configures an `active_profile` (used while the user is at the
  machine) and an `idle_profile` (used when HID idle time ≥ `idle_after_min`, default 10 min, and on AC
  power). Defaults: `active_profile = light`, `idle_profile = full`. Optional time schedules override
  both (e.g. `full` 19:00–07:00, `light` otherwise).
- FR-53. **Automatic pause** (no new acquires; running subtask released after current batch if the
  condition persists > 60 s):
  - memory pressure `warning` or higher (pause and shrink cache at `warning`; release at `critical`);
  - thermal state `serious` or `critical`;
  - on battery (default) or battery < 30 % (if `allow_on_battery = true`); Low Power Mode on;
  - owner-triggered `minion-executor pause [--for 1h]`;
  - optional: listed foreground apps active (e.g. screen sharing, `zoom.us`) → drop to `light` or pause.
- FR-54. **Budget changes mid-subtask.** Duty cycle, batch size and CPU priority apply at the next
  generation step / batch. A memory budget below the loaded model's `memory_gb` causes the executor to
  finish the current batch, `release` the subtask (immediate requeue, not a failure), and unload the model.
- FR-55. **Reporting.** The executor sends its current profile, budget and pause reason with every
  `acquire` and `heartbeat`; the server uses `memory_gb` for admission filtering and records it for
  Grafana. A paused executor does not poll (it only sends a low-frequency presence ping).
- FR-56. **Owner control.** CLI (and later a menu-bar app, M6): `minion-executor profile <name>`,
  `pause`, `resume`, `status` (shows budget, current task, GPU share, today's contribution).

## 5. Subtask & task state machines

```
Subtask:  PENDING ──acquire──▶ RUNNING ──complete──▶ SUCCEEDED
             ▲                   │  │
             │   reclaim/release │  └──error(non-retryable) / attempts exhausted──▶ FAILED
             └───────────────────┘
          PENDING/RUNNING ──cancel──▶ CANCELLED

Task:     SPLITTING ─▶ PENDING ─▶ RUNNING ─▶ SUCCEEDED | FAILED | PARTIAL | CANCELLED
```
Task status is derived from subtask counts and updated by the server on each subtask transition.

## 6. API (v1)

All endpoints under `/api/v1`. JSON. Users authenticate with user API keys; executors with executor
tokens (`Authorization: Bearer ...`). User keys come from a config file, executor tokens from the
database (§8, ADR-0007). Request/response schemas live in `minion-common` and are shared by server and
client. The schemas and typed client exist (TASK-2.1); the endpoints themselves land in TASK-2.2.

**Error body.** Every error is `{"detail": {"code": "<CODE>", "message": "<text>"}}`. Messages are
diagnostic only and never echo payload content. Codes: `UNAUTHORIZED` (401: missing, invalid, expired
or revoked credential) and `STALE_ACQUIREMENT` (409, below). Other 4xx responses (403 team not in the
key's teams, 422 invalid input or URI, 404) carry the same shape with their own code. Also:
`TASK_EXISTS` and `OUTPUT_URI_IN_USE` (409, task create: the name, respectively the normalized
output prefix, belongs to another task), `PAYLOAD_TOO_LARGE` (413: request body above
`MINION_MAX_REQUEST_BODY_BYTES`, default 1 MiB, enforced before authentication), and
`OUTPUT_CHANGED` (422, complete: the attempt object changed during verification), and task-create
422s `UNKNOWN_MODEL`, `MODEL_TYPE_MISMATCH`, `EMPTY_INPUT` (no object under the input prefix),
`INVALID_URI`, plus `RECORD_COUNT_MISMATCH` and `OUTPUT_MISSING` on complete. A task whose
splitting fails becomes `FAILED` with `error` set to a short code-prefixed message, for example
`LINE_TOO_LONG` (a JSONL line above `MINION_SPLIT_MAX_LINE_BYTES`, default 16 MiB) or
`INPUT_CHANGED` (the input differs from rows written by an earlier split attempt, or an object
changed while being read), or `SPLIT_ATTEMPTS_EXHAUSTED` (claimed `MINION_SPLIT_MAX_ATTEMPTS` times
without finishing).

**Implementation status.** TASK-2.2 implements `POST /tasks`, `GET /tasks/{team}/{project}/{task_name}`,
and all of §6.2 (including `presence`, TASK-5.1); TASK-3.1 adds `heartbeat` and the watchdog reclaim; TASK-3.2 adds `cancel` and the `minion-watchdog` process (reclaim, `MINION_MAX_LEASE_S` hard cap, cancel sweep). The other rows are not yet implemented.

**Heartbeat rate limit.** Beats for one acquirement closer together than `MINION_HEARTBEAT_MIN_INTERVAL_S` (default 1 s, `0` disables) get `429 TOO_MANY_HEARTBEATS` with `Retry-After`; the beat is not recorded. Executors treat 429 as non-fatal. Per-IP limiting stays a reverse-proxy concern.

### 6.1 User-facing
| Method | Path | Purpose |
|---|---|---|
| `POST` | `/tasks` | Create task. `input_uri` and `output_uri` are prefixes: a missing trailing `/` is added, and `output_uri` must be unique across tasks. Body: `team, project, task_name, type, model, input_uri, output_uri, params, records_per_subtask (1..100 000, default 500), priority? (-1000..1000, default 0), allowed_pool?` → `202` + task |
| `GET` | `/tasks?team=&project=&status=` | List tasks |
| `GET` | `/tasks/{team}/{project}/{task_name}` | Task detail + progress counts (`TaskOut`, below) |
| `GET` | `/tasks/{team}/{project}/{task_name}/subtasks` | Paginated subtasks |
| `POST` | `/tasks/{team}/{project}/{task_name}/cancel` | Cancel (key's team must own the task: `403` otherwise, `404` unknown). Lazy two-phase: task → `CANCELLED`, then `PENDING` subtasks → `CANCELLED`; `RUNNING` ones keep their owner, who learns via `heartbeat.cancel=true` and releases (their `release`/`fail`/reclaim end `CANCELLED`; a `complete` is still accepted and the task stays `CANCELLED`). Idempotent: re-cancel → `200` + task; an already `SUCCEEDED/FAILED/PARTIAL` task → `409 TASK_FINISHED`. The watchdog sweeps stragglers (`PENDING` under a task cancelled within `MINION_CANCEL_SWEEP_WINDOW_S`, default 1 h; older stragglers are healed only by re-cancel). Acquire also refuses subtasks of a non-`PENDING/RUNNING` task |
| `GET` | `/executors` | Fleet view: online, busy, last heartbeat |

### 6.2 Executor-facing **[paper: acquire, state update, heartbeat, publish]**
| Method | Path | Purpose |
|---|---|---|
| `POST` | `/executor/register` | Report host capabilities + engines + capacity profile; returns `executor_id` |
| `PUT` | `/executor/preference` | Set/clear `(team?, project?, task_name?)` preference |
| `POST` | `/executor/acquire` | Body: `{engines, budget: {profile, memory_gb, gpu_duty_cycle, max_batch_size, cpu_priority? (background|low|normal)}}`. Server filters candidates by `model.memory_gb ≤ budget.memory_gb`, engine and `allowed_pool`. `200` + assignment (presigned input GET URL, presigned output PUT URL, model entry, params, `acquirement_id`, `attempt`) or `204` = no eligible work (empty body) |
| `POST` | `/executor/subtasks/{acquirement_id}/heartbeat` | Liveness + progress + current budget/pause state. Body (all optional, unknown fields rejected): `progress? {records_done, records_total}` (ints 0..2^31-1, `done ≤ total`; `total` above the subtask's `record_count` → `422 INVALID_PROGRESS`), `budget?` (`memory_gb` 0..1024 finite, `gpu_duty_cycle` 0..1, `max_batch_size` 1..4096, `cpu_priority?` `background|low|normal`), `paused_reason?` (`[a-z_]{1,64}`). Response `200 {cancel: bool, output_url, expires_at}` (`output_url` is a refreshed presigned PUT for the same attempt key); stale/foreign `acquirement_id` → `409 STALE_ACQUIREMENT` |
| `POST` | `/executor/presence` | Low-frequency ping while idle/paused: `{budget, paused_reason?}` (`budget.cpu_priority` optional; `paused_reason` `[a-z_]{1,64}`). Response `204`; updates the caller's own executor row only (profile, budget, paused_reason, `last_seen_at`); revoked/invalid token → `401`; bad body → `422`. `acquire` clears `paused_reason` |
| `POST` | `/executor/subtasks/{acquirement_id}/complete` | Report success + output metadata (record count, checksum) |
| `POST` | `/executor/subtasks/{acquirement_id}/fail` | Report error `{retryable, code, message}` |
| `POST` | `/executor/subtasks/{acquirement_id}/release` | Voluntarily give back (graceful stop) |

Errors: `409 STALE_ACQUIREMENT` if `acquirement_id` is no longer current (or belongs to another
executor; the response is identical for both, so ids cannot be probed); `401` auth.
`complete` is idempotent for the same owner: a retry after a lost response returns `204` again. A
retried `fail` or `release` after a lost response returns `409` (the acquirement is already gone),
so executors must treat `409` on those two as "already handled".
Executors should back off with jitter on `204` (default 10–30 s).

**Identifiers.** `acquirement_id` and `executor_id` are UUIDs. `subtask_id` is a non-negative integer,
sequential per task and deterministic from the input. `attempt` is an integer ≥ 1.

**`TaskOut`** (response of `POST /tasks` and `GET /tasks/...`):
`team, project, task_name, type, model, input_uri, output_uri, params, records_per_subtask, priority,
allowed_pool, status` (`SPLITTING | PENDING | RUNNING | SUCCEEDED | FAILED | PARTIAL | CANCELLED`),
`created_by` (the API key's name), `created_at`, `finished_at` (null until terminal), `error` (null or a
short diagnostic), `counts: {total, pending, running, succeeded, failed, cancelled}`.

## 7. Data model (PostgreSQL — 7 tables)

The paper states 7 tables and names `minion_subtask_acquirement` and a heartbeat table. Proposed set **[ours]**:

1. `minion_task` — `(team, project, task_name)` PK/unique; type, model_id (→ allow-list; engine and memory_gb snapshotted at create), input_uri, output_uri, params jsonb, status, priority, counts, created_by, timestamps. `output_uri` is UNIQUE; `split_attempts` and `next_split_at` implement the splitter lease and retry backoff.
2. `minion_subtask` — `(team, project, task_name, subtask_id)` unique; input slice, status, attempt, max_attempts, error, output_uri, timestamps.
3. `minion_subtask_acquirement` — **UNIQUE `(team, project, task_name, subtask_id)`** **[paper]**; `acquirement_id` uuid, executor_id, attempt, acquired_at.
4. `minion_executor_heartbeat` — acquirement_id (the executor is reached by joining `minion_subtask_acquirement`), last_seen_at, progress jsonb (one row per active acquirement, upserted).
5. `minion_executor` — executor_id, token_hash, hostname, owner, pool, chip, total_memory_gb, os_version, engines, status, **profile, budget jsonb, paused_reason**, registered_at, last_seen_at, token_expires_at (NULL = no expiry; auth rejects tokens past it).
6. `minion_executor_preference` — executor_id, team, project, task_name (nullable for wildcards).
7. `minion_subtask_attempt` — history: one row per finished attempt (executor, outcome `SUCCEEDED|FAILED|EXPIRED|RELEASED|CANCELLED`, duration, error). Feeds Grafana.

The model allow-list is configuration (`models.yaml`, loaded by the server at start), not a table, so
the paper's 7-table count holds.

Full DDL lives in Alembic migrations; ARCHITECTURE §5 explains indexes and the acquire query.

## 8. Security & privacy

- S-1. Executors run on personal work laptops: data confidentiality is the main risk **[paper]**.
  Mitigation: deploy per team/org with dedicated DB and isolated executor pools **[paper]**; tasks may be
  restricted to an executor pool via `allowed_pool` **[ours]**.
- S-2. Executors never hold DB or S3 credentials; only per-subtask presigned URLs (TTL ≤ 1 h, refreshed
  via heartbeat) **[ours]**.
- S-3. Executor tokens: random 256-bit (`token_urlsafe(32)`), stored as SHA-256 hash only, revocable,
  scoped to a pool, optional expiry (`token_expires_at`). Tokens are pre-issued by the operator with
  `minion-server-admin issue-executor-token --owner <name> [--pool default] [--ttl-days N]` (N = 1..365,
  default `MINION_EXECUTOR_TOKEN_TTL_DAYS`, 90); the token is printed once and the executor row starts as
  `(unregistered)`. Revoke with `minion-server-admin revoke-executor-token --executor-id <uuid>`. Auth
  rejects a revoked or expired token with `401` on its next request. Tokens are never logged.
- S-4. Executor deletes local input/output files after upload; model cache is retained.
- S-5. No payload content in logs or metrics.
- S-6. TLS required between executor and server outside local dev.
- S-7. Model ids are allow-listed per deployment (executors must not pull arbitrary remote code;
  `trust_remote_code=False`).
- S-8. User API keys: `minion-server-admin generate-api-key --name <name> --teams a,b` prints the key
  once and a YAML entry holding only its SHA-256 hash. Entries live in the file named by
  `MINION_API_KEYS_FILE`; lookup is by SHA-256 with `hmac.compare_digest`. The file is read at server
  startup, so revoking a user key requires removing it and restarting the server. No DB table (ADR-0007).
  `created_by` on a task records the key name; there is no foreign key yet (deferred to M5).
- S-9. Storage configuration: S3 access and secret keys are required server settings and are never sent
  to executors. `MINION_S3_PUBLIC_ENDPOINT_URL` (the host that presigned URLs point at) must be `https`
  unless it is a loopback host; the server refuses to start otherwise. Presigned URLs use the configured
  TTL (`MINION_PRESIGN_TTL_S`, default 900 s, max 3600 s).
- S-10. Inference worker confinement **[ours]** (ADR-0009). Models are fetched by a separate killable
  subprocess at a pinned revision, safetensors-only, sha256-verified; `HF_TOKEN`, proxy and CA settings
  reach only that subprocess. The worker is offline (`HF_HUB_OFFLINE=1`) and runs inside a macOS
  Seatbelt sandbox applied before any engine import: no network (except the loopback Ollama port,
  granted to Ollama loads only; Ollama is opt-in via `--engines ollama` because its daemon runs
  outside this sandbox and `/api/pull` writes to `~/.ollama` outside the cache cap), no process exec, no LaunchServices/AppleEvents/distributed
  notifications/preferences/foreign signals, Mach lookups deny-default with a six-service Metal and
  logging allow-list (no Keychain, tccd, pasteboard, WindowServer, mds, nsurlsessiond), writes denied
  except the worker's tmp dir, a few `/dev` nodes and the Python Metal cache, reads denied for HOME,
  `/Users`, `/Volumes`, `/private/tmp`, `/private/var/folders`, `/Library` and the Homebrew data
  dirs (`/opt/homebrew/{var,etc}`, `/usr/local/{var,etc}`) except the current model
  dir, the Python prefixes/site-packages/editable source roots and the tmp dir. Other processes' launch
  arguments/environment (`KERN_PROCARGS2`, `process-info`) are unreadable. A self-test after
  applying the profile aborts the worker (exit 78, mapped to `SANDBOX_FAILED`, released) if HOME is
  listable, an outside write succeeds, another process's procargs are readable or (worker) an
  outbound TCP connect to `1.1.1.1:443` is not refused with EPERM/EACCES. The model-fetch child has
  the same profile with network re-opened only for DNS (mDNSResponder socket), outbound TCP to
  `*:443` (or the `HF_ENDPOINT` port) and the configured proxy port; loopback (unless the proxy is
  local) and every other unix socket (docker.sock, SSH agent) are denied; CA bundles are granted as
  single files, an `SSL_CERT_DIR` under HOME is refused, and its staging dir is size-capped to the
  hub's listing (`MODEL_INTEGRITY`). If the
  sandbox cannot be applied the worker fails (`SANDBOX_FAILED`) and `run` exits 2; off macOS only the
  fake engine runs. `MINION_EXECUTOR_SANDBOX=0` requires `MINION_EXECUTOR_DEV=1`. Residual: the profile
  is allow-default outside the denied areas, so non-denied system paths (`/etc`, `/opt`, ...) stay
  readable (ADR-0009).

## 9. Non-functional requirements

| ID | Requirement |
|---|---|
| NF-1 | Acquire endpoint p99 < 200 ms with 500 executors and 1M pending subtasks. |
| NF-2 | Zero double-completion: a subtask's final output is written by exactly one attempt. |
| NF-3 | No lost subtasks: every subtask reaches a terminal state given ≥1 live executor. |
| NF-4 | Executor idle CPU < 1 %; no work while user has stopped it. |
| NF-5 | Server and watchdog are stateless and horizontally scalable (all state in Postgres). |
| NF-6 | Executor runs on macOS 14+ on M1 or newer. |
| NF-7 | Foreground responsiveness under `light`/`balanced`: no audible/visible degradation in a Teams/Zoom video call; UI frame drops and app-launch time within 10 % of baseline (measured in M5 usability test). |
| NF-8 | Budget adherence: measured GPU busy share within ±10 pp of `gpu_duty_cycle`; worker RSS never exceeds `memory_gb` + 10 %. |
| NF-9 | Reaction time: governor reacts to memory-pressure / thermal / user-return events within 5 s (pause new steps). |

## 10. Future work (out of scope for v1) **[paper]**
- Distributed training on the fleet.
- Secure federation of idle Macs across organizations or globally.

## 11. Open questions
- Q1. Server-side splitting: pre-split into separate S3 objects, or byte ranges on the original? (ARCH §5.2 proposes byte ranges.)
- Q2. Priority/fairness across teams beyond FIFO + preference?
- Q3. Should executors batch-acquire (N subtasks) to cut round-trips for very small subtasks?
- Q4. Auth for the web UI (SSO/OIDC vs API keys only)?
- Q5. Is step-level duty cycling enough to keep video calls smooth, or do we also need to throttle on
  measured *total* GPU utilization (ioreg `Device Utilization %`)? **Open.** The M3 benchmark ran at
  duty cycle 1.0 with no foreground load, so it does not answer this; decide with the M4 governor
  measurements (NF-7, NF-8).
- Q6. Should `memory_gb` estimates in `models.yaml` be measured automatically by `minion-executor bench`
  and fed back to the server? **Partly answered by M3:** `bench` measures worker RSS peak and
  per-engine peak; `memory_gb` is still hand-set in `deploy/bench-models.yaml`, and feeding measured
  values to the server is **not implemented** (open). The M3 data shows the hand-set 1.5 GB for
  Qwen2.5-0.5B is below the HF worker RSS peak (2.26 GB) and above the MLX peak (1.42 GB); not reconciled.
