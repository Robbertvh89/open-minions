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
- FR-1. Users create tasks via REST API, web UI and the `minion` submitter CLI (`minion submit`, which uploads local JSONL to S3 with the submitter's own credentials, then creates the task via the REST API) **[paper]**.
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
- FR-9. A team may hold at most `MINION_MAX_LIVE_TASKS_PER_TEAM` (default 500, 1..10000) live tasks
  (`SPLITTING`, `PENDING`, `RUNNING`). Task creation above it answers `429 TOO_MANY_LIVE_TASKS` (no
  `Retry-After`; slots free as tasks finish or are cancelled), for REST and UI alike. A team can override
  the limit with `teams.<slug>.max_live_tasks` in the API keys file (S-1, live-reloaded). The limit is
  checked in the create transaction under a per-team advisory lock, so it is exact across workers and
  replicas (NF-5); the check runs after validation, so a team at its limit gets 429 even for a duplicate
  name. It bounds the acquire candidate walk over tasks no executor can run (NF-1; ADR-0020) **[ours]**.

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
    after every generation step. Install with `uv sync --all-packages --extra mlx`.
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
  instead of waiting for the watchdog **[ours]**. Implemented (M4.2, TASK-5.2): the grace is
  `MINION_EXECUTOR_STOP_GRACE_S` (default 30, 0..300; 0 = stop at once); a second signal stops
  immediately. Exit follows within grace + 10 s (one shared shutdown budget for complete, release, kill and close, plus small per-call floors). launchd's default `ExitTimeOut` is 20 s, so the plist must set it to at least grace + 15 s. Implemented (M5, TASK-6.8, ADR-0011): `minion-executor service install` renders a per-user LaunchAgent with `ExitTimeOut = ceil(grace) + 15`, `service uninstall` boots it out (SIGTERM, graceful) and `service status` warns when the loaded timeout is too short; `run` refuses to start if `MINION_EXECUTOR_LAUNCHD_EXIT_TIMEOUT_S` < grace + 15 (under launchd permanent failures exit 0 with a `permanent_failure` log line, see ADR-0011).
- FR-18. When and how much the executor works is governed by the capacity rules in §4.6 (the paper
  only lets users stop the executor entirely).

**Implementation status (M1, TASK-2.3).** Implemented: FR-10 (register, token from
`$MINION_EXECUTOR_TOKEN` or Keychain item `open-minions-executor`), FR-11 (loop with fake runner
only), FR-15 (one subtask at a time), FR-17 partially (SIGTERM/SIGINT stop, release of the in-flight
subtask, exit 0; the 30 s grace period followed in M4.2, see FR-17). Not yet implemented (as of TASK-2.3; FR-12 and FR-13 are now done, see the M3 updates): FR-16 engine filtering beyond the `engines` list sent on acquire, FR-18/§4.6
governor (M4). ARCHITECTURE §2.2 describes the executor as built.

**M3 update (TASK-4.1 S3).** `run --engines auto|mlx,ollama,hf` (default `auto`, which probes mlx and hf only: Ollama is opt-in, see S-10) probes engines in a worker subprocess and registers/acquires with the available ones; none available exits 2, and `fake` is never implicit (needs `--engines fake` plus `MINION_EXECUTOR_DEV=1`). The loop validates task params (`BAD_PARAMS`, non-retryable), releases subtasks whose engine is not advertised or whose model exceeds the memory budget (`MODEL_NOT_ADMISSIBLE`; always released, bounded server-side by `MINION_RELEASE_LIMIT`). `MODEL_NOT_ADMISSIBLE` and `ENGINE_UNAVAILABLE` (engine missing or not yet implemented on this Mac, also released) are logged executor-side and, since TASK-6.5, also sent as the release `code` (see FR-40). The loop also honours `RunnerError.retryable`, stamps `model` and `model_revision` on every output line, treats per-record `PROMPT_TOO_LONG` error lines as records (not a subtask failure), and unloads the worker after `MINION_EXECUTOR_IDLE_UNLOAD_S` (600; 0 disables) without work, including after acquire errors such as a server outage (FR-13, invariant 6). The profile's `gpu_duty_cycle` is applied by the worker's static pacer.

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

**M2 update (TASK-3.3).** FR-14 is implemented (heartbeater, 429 non-fatal, cancel and 409 interrupt the worker). FR-17 remained partial until M4.2 (TASK-5.2, grace period).

### 4.3 Acquisition protocol **[paper]**
- FR-20. Ownership is claimed by inserting into `minion_subtask_acquirement`, which has a UNIQUE
  constraint on `(team, project, task_name, subtask_id)`.
- FR-21. Exactly one insert wins; losers get no row (`ON CONFLICT DO NOTHING`) and re-read a fresh candidate window (ADR-0013).
- FR-22. Dynamic task binding: if the executor has a preference, matching subtasks are dequeued first;
  otherwise (or if none match) fall back to the **global FIFO** queue (ordered by task `created_at`,
  then `subtask_id`).
  Note (ADR-0013): among the top `MINION_ACQUIRE_WINDOW` (default 16) candidates of the highest priority
  present the pick is random, to avoid head contention; priority is never inverted, and with window 1
  the order is strict.
- FR-23. **[ours]** Per-executor cap on open acquirements: one executor holds at most `MINION_MAX_OPEN_ACQUIREMENTS`
  (default 4, 1..64) acquirements at once. Acquire above it answers `429 TOO_MANY_ACQUIREMENTS` with
  `Retry-After: 30` before any candidate is read (executors back off like for any 429). The cap is an
  admission quota, not an ownership decision (FR-20/FR-21 are unchanged), and it is enforced in the
  database, so it holds across server workers and replicas (ADR-0019). A slot frees on complete,
  fail, release or reclaim.

### 4.4 Fault tolerance (Watchdog)
- FR-30. Watchdog monitors heartbeats **only for executors currently running subtasks** **[paper]**.
- FR-31. If no heartbeat for `heartbeat_timeout` (default 15 min = 3 missed beats) **[ours]**, the
  watchdog deletes the acquirement, archives it as `EXPIRED`, and sets the subtask back to
  `PENDING` (or `FAILED` once the budget is spent) **[paper semantics]**. Reclaim does not touch
  `attempt`: the next acquire increments it, so attempt-scoped output keys stay unique.
  Deviation **[ours]** (ADR-0014): an acquirement whose heartbeat was never refreshed after the
  claim (executor never acknowledged it, e.g. the acquire response was lost) is archived as
  `EXPIRED` with code `NOT_ACKNOWLEDGED` and does not consume the budget, up to
  `MINION_RELEASE_LIMIT` such attempts per subtask; beyond that it expires as `HEARTBEAT_TIMEOUT`
  and counts. Executors therefore send an immediate heartbeat (ack) after a successful acquire.
- FR-31a. Hard lease: an acquirement older than `max_lease` (`MINION_MAX_LEASE_S`, default 6 h) is reclaimed as `EXPIRED` (code `MAX_LEASE_EXCEEDED`, consumes budget) even while it heartbeats, so a looping executor cannot hold a subtask forever **[ours]**.
- FR-32. The retry budget is the number of `FAILED` + `EXPIRED` attempts (not counting
  `NOT_ACKNOWLEDGED` expiries within their cap, FR-31); once it reaches
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

**Implementation status (M5, TASK-6.5).** FR-40: two dashboards provisioned from `deploy/grafana/dashboards/` (`minion-tasks`: tasks/subtasks by status, subtasks and records per minute, per-model throughput, attempt outcomes, reclaims per hour by code, top failure codes; `minion-fleet`: active/busy/paused executors, paused by reason, donated memory by profile, releases by reason, top contributors, per-executor table). Grafana connects as `minion_grafana`, a login member of the NOLOGIN role `minion_readonly`, which can only `SELECT` from the views of schema `minion_ro` (migration 0007): no token hash, task params, input/output keys, acquirement ids or free-text errors, and no privilege on the `public` tables. What is **enforced** on the login is its privileges (`SELECT` on `minion_ro` only), a connection limit (5) and `temp_file_limit` (1 GB); `statement_timeout` 10 s and the read-only transaction are session defaults the role can `SET` away, not a security boundary; memory/CPU exhaustion through user-settable `work_mem`/`statement_timeout` is **not** enforced for this role, so production should point Grafana at a read replica or put a query-timeout proxy in front. Large-object creation and temp tables are revoked. Fleet panels count only `online` executors (`minion_ro.executor.online`, the same rule as `GET /executors`; the view hardcodes the 600/120 s defaults). The fleet dashboard shows per-owner presence, so like `GET /executors` it is **operator-only** (S-11): Grafana OSS has no per-datasource permissions, so any Grafana account in the org can read everything in `minion_ro`; run the production Grafana operator-only or in a separate org. FR-41: `/metrics` (prometheus-client) is **off** unless `MINION_METRICS_ENABLED=true`; it is served on the API port, so set `MINION_METRICS_TOKEN` (at least 32 characters, empty counts as unset; scrape with `Authorization: Bearer`) or restrict the path at the proxy; enabling metrics without a token on a non-loopback host logs a startup warning. Metrics: `minion_http_requests_total{route,method,status}` (route template, never the URL) `minion_acquire_total{outcome=assigned|empty|exhausted|shed|rate_limited|capped}` (`exhausted` = 204 although at least one pass used all MAX_ROUNDS with candidates and lost every race, TASK-31; no dashboard panel reads it) and the histogram `minion_acquire_seconds{outcome}` (server-side acquire duration; outcomes as the counter plus `error`, which is timed but not counted); no ids, team names or payload. `POST .../fail` accepts any 1..64 character `code` for compatibility with old executors; the server upper-cases it and stores `RUNNER_ERROR` if it is still not `^[A-Z0-9_]{1,64}$`. A warning is logged at startup when `MINION_EXECUTOR_ONLINE_*` differ from the 120/600 s the dashboards' `online` column hardcodes. `release` takes an optional body `{"code": "^[A-Z0-9_]{1,64}$"}` (why the executor handed the subtask back), stored in the attempt history (`minion_ro.attempt.error_code`); a release without a body (older executors) still works.
**Implementation status (M5, TASK-6.11).** FR-41 in containers: `compose.yml --profile app` runs `server` with a `/healthz` healthcheck, published on loopback only (`MINION_SERVER_HOST_PORT`), with the rest of the stack as in ARCHITECTURE §6. The `/metrics` requirements above are unchanged.
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
**Status (TASK-15):** the control process enforces `gpu_duty_cycle` without trusting the worker. It meters each worker pid's GPU time itself (IOKit `accumulatedGPUTime`) and judges a 10 s window against duty + 10 pp. An excess bucket in GPU-seconds reaching its 20 GPU-s budget (a burn/rest sawtooth cannot reset it, an honest long unpaced step stays below it), or any GPU use above the margin while paused (after the worker acked the pause) or idle, kills the worker and the subtask is released with `GPU_DUTY_VIOLATION` (the subtask is RELEASED; GPU kills have their own exponential back-off streak that a completed subtask does not reset, and an engine whose kills add up to 6 points within an hour (a duty or clients kill counts 2, a meter kill 1) is no longer acquired, listed as `gpu_quarantined` in `status.json`, and surviving an executor restart via `gpu_quarantine.json` in the state dir; when no engine is left the executor sends presence with `paused_reason` `gpu_violation`); repeated meter errors, a read that overruns the sample interval, or a sampler that stalls or dies (the executor then exits 1) release with `GPU_METER_UNAVAILABLE`. `status.json` reports the last kill as `gpu_violation`. Off darwin, or when no metered engine (mlx, hf) is advertised, the guard is disabled (one log line); on darwin a meter that exists but fails its startup read stops `run` (exit 2). The kill rung enforces the budget without worker cooperation; the throttle rung (SIGSTOP gating) is TASK-15.1 and not implemented. HF/MPS runs in the worker pid and is metered like MLX (one bench repeat on this Mac: 1 GPU client, honest windows well below the limits); Ollama runs in a daemon outside the worker pid and is not metered (ADR-0006 D17).
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
  (`taskpolicy -b`) is applied by the worker since M4.3 (live, also inside the sandbox). FR-52..FR-55 enforcement is M4.2..M4.5; `POST /executor/presence`
  exists (FR-55); FR-56 is partially met (`run --profile` only). Proposed (ADR-0006 D10): there is no
  implicit `balanced` default; `run` without `--profile` is adaptive (FR-52), and `--profile
  <name>` pins a profile.
- FR-52. **Adaptive switching.** The owner configures an `active_profile` (used while the user is at the
  machine) and an `idle_profile` (used when HID idle time ≥ `idle_after_min`, default 10 min, and on AC
  power). Defaults: `active_profile = light`, `idle_profile = full`. Optional time schedules override
  both (e.g. `full` 19:00–07:00, `light` otherwise).
- FR-53. **Automatic pause** (no new acquires; running subtask released after current batch if the
  condition persists > 60 s). **Status (TASK-5.5):** implemented in the executor loop: no acquire
  while paused, the in-flight subtask keeps its attempt if the pause ends within 60 s and is
  interrupted and released after 60 s (or at once on memory pressure `critical`):
  - memory pressure `warning` or higher (pause and shrink cache at `warning`; release at `critical`);
  - thermal state `serious` or `critical`;
  - on battery (default) or battery < 30 % or battery percentage unknown (if `allow_on_battery = true`; reason `low_battery`); Low Power Mode on;
  - owner-triggered `minion-executor pause [--for 1h]`;
  - an unusable owner `control.json` (fails closed; `paused_reason` `control_invalid`, listed right
    after `user_paused` in the priority order);
  - optional, call-aware (TASK-6.10): a process is using audio input or a camera is running
    (CoreAudio process objects with `IsRunningInput`, macOS 14+; fallback the default input device's
    "running somewhere" flag, which has false positives on combined headset/USB devices and
    always-open microphone apps; CoreMediaIO for cameras; no device is opened, no permission prompt) →
    `on_call = "auto"` (default: every budget dimension capped at `light` at once and reported as plain
    `light`, back up after the usual 60 s hold; only while the profile is adaptive, so a profile pinned
    with `run --profile` or `minion-executor profile` is not capped and returns at once),
    `"light"` (the same cap for pinned and owner profiles too, which also return at once after the
    call), `"pause"` (reason `in_call`, lowest in the priority order) or `"ignore"`. An unreadable
    flag counts as not in a call; the probe runs apart from the safety read and can never cause
    `signals_unavailable`. Listed foreground apps (screen sharing) are not detected.
- FR-54. **Budget changes mid-subtask.** Duty cycle, batch size and CPU priority apply at the next
  generation step / batch. A memory budget below the loaded model's `memory_gb` causes the executor to
  finish the current batch, `release` the subtask (immediate requeue, not a failure), and unload the model.
  **Status (TASK-5.3):** the worker side is implemented: a running batch takes a new duty cycle or a
  pause at its next checkpoint (also mid pacer-sleep, acked per generation), and each batch can carry a
  new budget (memory limits, batch cap, CPU priority).
  **Status (TASK-5.5):** the governor is wired in: duty, pause and trim reach the worker at once,
  batch size, memory limits and CPU priority apply to the next batch, and a memory budget below the
  loaded model finishes the batch (bounded by 60 s), releases (`BUDGET_DOWNGRADE`, executor-side log
  code), unloads and unpins the cache; an idle warm worker above the budget is unloaded at once. A
  worker footprint above 1.1x the memory budget on two consecutive samples fails the subtask as a
  retryable `OOM`. The guard measures against the budget the running batch started with, so a drop
  that still fits the model (full to light) does not trip it before the next batch boundary; if the
  batch outlives `downgrade_batch_s` (60 s) after the drop with the footprint over 1.1x the live
  budget, the subtask is released (owner's policy), not failed. OOM fails only above 1.1x the budget
  the batch started with. Once
  inference is over, upload and complete are never cut short by the governor.
- FR-55. **Reporting.** The executor sends its current profile, budget and pause reason with every
  `acquire` and `heartbeat`; the server uses `memory_gb` for admission filtering and records it for
  Grafana. A paused executor does not poll (it only sends a low-frequency presence ping).
  **Status (TASK-5.5):** the loop sends the governor's live budget (the per-dimension minimum while a
  profile hold is running) and `paused_reason`; presence goes out on entering a pause, on a reason or
  budget change and every `MINION_EXECUTOR_PRESENCE_INTERVAL_S` (60).
- FR-56. **Owner control.** CLI and a menu-bar app (`minion-executor menubar`, TASK-7.5; local files only, no server or token; launch at login via `minion-executor menubar install`, a separate Aqua-session LaunchAgent, TASK-7.6):
  `minion-executor profile <name>`,
  `pause`, `resume`, `status` (shows budget, current task, GPU share, today's contribution).
  **Status (TASK-5.4):** `pause [--for 1h]`, `resume` and `profile <name|auto>` write the owner's
  `control.json` (0600, atomic, strict schema; ADR-0006 D11). **Status (TASK-5.5):** the running
  executor's governor reads it every poll, and `minion-executor status` prints `status.json` (profile
  and source, budget, `held below nominal`, paused reason, current subtask id and progress, GPU share,
  a signals summary, subtasks/records completed today by this process); it exits 1 when no executor
  is running (no file, `stopped`, not updated for 30 s, or the pid is gone). No payload, URL or token
  is ever written to it. **One executor per state directory:** the executor takes an exclusive
  `flock` on `executor.lock` there for its whole lifetime; a second one exits 2 (the lock dies with
  the process). `status` accepts enum values it does not know (a newer executor's file) and rejects
  an unknown `version` with a clear message (exit 2).

## 5. Subtask & task state machines

```
Subtask:  PENDING ──acquire──▶ RUNNING ──complete──▶ SUCCEEDED
             ▲                   │  │
             │   reclaim/release │  └──error(non-retryable) / attempts exhausted──▶ FAILED
             └───────────────────┘
          PENDING/RUNNING ──cancel──▶ CANCELLED

Task:     SPLITTING ─▶ PENDING ─▶ RUNNING ─▶ SUCCEEDED | FAILED | PARTIAL | CANCELLED
```
Task status is derived from subtask counts. The server sets the terminal status just after the last subtask transition commits (a post-commit settle, ADR-0015); the watchdog settles any task whose settle was lost, so a finished task may show `RUNNING` for up to one watchdog interval. A concurrent cancel wins: a `CANCELLED` task is never moved to another status.

## 6. API (v1)

Compatibility: response models the executor parses (assignment, register, heartbeat) ignore unknown top-level fields for forward compatibility (the nested `model` entry stays strict until TASK-12), while request models are strict (`extra="forbid"`); roll out the server before executors.

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
`INVALID_URI`, plus `RECORD_COUNT_MISMATCH`, `OUTPUT_MISSING`, `OUTPUT_SIZE_MISMATCH` and `OUTPUT_TOO_LARGE` (attempt object above `MINION_MAX_OUTPUT_BYTES`) on complete. A task whose
splitting fails becomes `FAILED` with `error` set to a short code-prefixed message, for example
`LINE_TOO_LONG` (a JSONL line above `MINION_SPLIT_MAX_LINE_BYTES`, default 16 MiB) or
`INPUT_CHANGED` (the input differs from rows written by an earlier split attempt, or an object
changed while being read), or `SPLIT_ATTEMPTS_EXHAUSTED` (`MINION_SPLIT_MAX_ATTEMPTS` real split failures, or
10x that many claims by holders that never recorded one; restarts and cancels do not count).

**Implementation status.** TASK-2.2 implements `POST /tasks`, `GET /tasks/{team}/{project}/{task_name}`,
and all of §6.2 (including `presence`, TASK-5.1); TASK-3.1 adds `heartbeat` and the watchdog reclaim; TASK-3.2 adds `cancel` and the `minion-watchdog` process (reclaim, `MINION_MAX_LEASE_S` hard cap, cancel sweep). TASK-6.4 adds the task list, subtasks and fleet read endpoints; all §6.1 rows are implemented. TASK-6.6 adds the server-rendered web UI under `/ui` (not part of the REST API; ADR-0010): `GET /ui/login`, `POST /ui/login`, `POST /ui/logout`, `GET /ui/tasks`, `GET /ui/tasks/{team}/{project}/{task_name}` (with `?partial=1` for the 5 s htmx refresh) and `GET /ui/static/{htmx.min.js,ui.css}`. TASK-6.7 adds `GET /ui/tasks/new` + `POST /ui/tasks` (create form; the same service function as `POST /tasks`, so the same row, pool policy, params parsing, model allow-list and error codes, shown as `CODE: message` and with the form refilled; form-level input errors are `INVALID_REQUEST`; success redirects to the task), `GET`/`POST /ui/tasks/{team}/{project}/{task_name}/cancel` (a server-rendered confirm page, then the FR-8 cancel: idempotent, `409 TASK_FINISHED`) and `GET /ui/fleet` (operator only, `403 OPERATOR_REQUIRED` otherwise; paged executor table with online/busy/profile/budget/paused reason, plus online executors and budget memory by profile and paused executors by reason over the whole fleet, up to 10 000 executors). `ExecutorOut` gains the optional `budget` field. Every `/ui` route is `404` unless `MINION_UI_SESSION_SECRET` is set; `POST`s answer `403` on a bad CSRF token or foreign `Origin`.

**Executor endpoint rate limits (NF-5).** One executor calling `register`, `acquire`, `presence` or `PUT /preference` closer together than `MINION_REGISTER_MIN_INTERVAL_S` (5), `MINION_ACQUIRE_MIN_INTERVAL_S` (0.2), `MINION_PRESENCE_MIN_INTERVAL_S` (5) or `MINION_PREFERENCE_MIN_INTERVAL_S` (1; `0` disables each) gets `429 TOO_MANY_REQUESTS` with `Retry-After` (whole seconds, >= 1), checked after authentication and before any DB write or acquisition; a rejected call changes nothing. In-memory, per replica, bounded LRU (`MINION_RATE_LIMIT_MAX_ENTRIES`, 40 000); per-IP limits stay at the reverse proxy. A healthy executor never trips them: idle acquires are >= 5 s apart (back-off 10 s base, jitter 0.5..1), presence goes out on a pause, a changed reason/budget or every `--presence-interval` (60 s), the executor keeps at least `MINION_EXECUTOR_ACQUIRE_PACE_S` (0.4, 0 or >= 0.2; the 0.2 s margin over the server interval absorbs network jitter) between acquire calls by sleeping only the remainder, so instant subtasks never trip the limit; a fast release/flap can still see a 429 (handled as back-off). The executor waits at least `Retry-After` (capped at 300 s), also across a pause. `minion-executor prefer --server URL (--team T [--project P] [--task-name N] | --clear)` calls `PUT /executor/preference` (G4/FR-22; exit 2 on bad input, missing token or an insecure URL).

**Live-task cap (FR-9, TASK-30).** `POST /tasks` (and `POST /ui/tasks`) above the team's live-task limit answers `429 TOO_MANY_LIVE_TASKS` with no `Retry-After` and inserts nothing; the message never echoes payload. Creates of one team are serialised by a per-team advisory lock with a 2 s wait; past it create answers `503 SERVER_BUSY` + `Retry-After`. `minion submit` exits 2 with the code in the message.

**Heartbeat rate limit.** Beats for one acquirement closer together than `MINION_HEARTBEAT_MIN_INTERVAL_S` (default 1 s, `0` disables) get `429 TOO_MANY_HEARTBEATS` with `Retry-After`; the beat is not recorded. The first beat after a claim (the ack) is exempt from the limit (ADR-0014). Executors treat 429 as non-fatal. Per-IP limiting stays a reverse-proxy concern.

**Overload shedding (TASK-7.4.4).** When the server is saturated it fails fast and safely instead of timing out: (a) a request that waits more than `MINION_DB_POOL_TIMEOUT_S` (5, 0.5..30) for a DB connection, on any endpoint, answers `503 SERVER_BUSY` with a jittered `Retry-After` (never 500); (b) at most `MINION_ACQUIRE_CONCURRENCY` (default pool + overflow - 4, min 1) acquires run per process, the rest answer `503 SERVER_BUSY` + `Retry-After` before any candidate work, which keeps connections free for heartbeat / complete / release; (c) an acquire that has used `MINION_ACQUIRE_DEADLINE_S` (10, 1..20, always below the executor's 30 s HTTP timeout) by the time it holds its connection answers `503 ACQUIRE_DEADLINE` + `Retry-After` before the claim INSERT (the deadline clock starts at request arrival; the claim statement itself is cancelled by `statement_timeout` at the deadline, same 503), so no acquirement is committed for a client that has already given up; `MINION_DB_POOL_TIMEOUT_S` + `MINION_ACQUIRE_DEADLINE_S` must be <= 25 s; one executor may have at most 2 acquires in flight (429); (d) a token never seen by this process is looked up on a separate auth pool (`MINION_DB_AUTH_POOL_SIZE`, 2, no overflow); waiting more than `MINION_AUTH_LOOKUP_TIMEOUT_S` (1.0, 0.1..5) for it answers `503 SERVER_BUSY` + `Retry-After` (counted in `minion_executor_auth_total{outcome=busy}`, not logged per request), so a flood of garbage bearer tokens cannot take connections owner operations need (ADR-0018); (e) an executor holding `MINION_MAX_OPEN_ACQUIREMENTS` acquirements answers `429 TOO_MANY_ACQUIREMENTS` + `Retry-After: 30` (FR-23, counted as `minion_acquire_total{outcome="capped"}`), and a missing, revoked or rotated-away executor row is also re-checked in every claim round and answers `401` (ADR-0019). Executors treat a 503 with `Retry-After` like a 429: acquire waits at least that long; `complete` waits it between retries without using up its retry count, bounded by `MINION_EXECUTOR_COMPLETE_SHED_BUDGET_S` (120, >0..300, heartbeats continue meanwhile); past the budget the executor releases the subtask with code `SERVER_BUSY` (a shed 503 committed nothing; RELEASED is not budget-counted, `MINION_RELEASE_LIMIT` bounds a livelock, the re-run is idempotent), falling back to the watchdog only if the release fails (no COMPLETE_FAILED).

### 6.1 User-facing
| Method | Path | Purpose |
|---|---|---|
| `POST` | `/tasks` | Create task. `input_uri` and `output_uri` are prefixes: a missing trailing `/` is added, `output_uri` must be unique across tasks, and an `output_uri` containing `"` is rejected with `422 INVALID_URI` (it becomes part of the presigned POST form, ADR-0017). Body: `team, project, task_name, type, model, input_uri, output_uri, params, records_per_subtask (1..100 000, default 500), priority? (-1000..1000, default 0), allowed_pool?` → `202` + task |
| `GET` | `/tasks?team=&project=&status=&cursor=&limit=` | Tasks of the caller's teams, newest first (`TaskPage {items: [TaskOut], next_cursor}`). `team` outside the caller's teams → `403`; `limit` 1..100 (default 50). Keyset pagination on `(created_at, team, project, task_name)` DESC: stable under concurrent inserts. `cursor` is opaque and strictly validated (tampered/foreign → `422 INVALID_CURSOR`); it carries a sort position only, the team filter is always re-applied. Counts for the page come from one grouped query |
| `GET` | `/tasks/{team}/{project}/{task_name}` | Task detail + progress counts (`TaskOut`, below) |
| `GET` | `/tasks/{team}/{project}/{task_name}/subtasks?status=&cursor=&limit=` | Subtasks by `subtask_id` (`SubtaskPage`; `limit` 1..100, default 100); same `403`/`404` rules as task detail. `SubtaskOut`: `subtask_id, status, attempt, max_attempts, record_count, error_code, updated_at` only: no input/output keys, presigned URLs or executor identity (S-11) |
| `POST` | `/tasks/{team}/{project}/{task_name}/cancel` | Cancel (key's team must own the task: `403` otherwise, `404` unknown). Lazy two-phase: task → `CANCELLED`, then `PENDING` subtasks → `CANCELLED`; `RUNNING` ones keep their owner, who learns via `heartbeat.cancel=true` and releases (their `release`/`fail`/reclaim end `CANCELLED`; a `complete` is still accepted and the task stays `CANCELLED`). Idempotent: re-cancel → `200` + task; an already `SUCCEEDED/FAILED/PARTIAL` task → `409 TASK_FINISHED`. The watchdog sweeps stragglers (`PENDING` under a task cancelled within `MINION_CANCEL_SWEEP_WINDOW_S`, default 1 h; older stragglers are healed only by re-cancel). Acquire also refuses subtasks of a non-`PENDING/RUNNING` task |
| `GET` | `/executors` | Fleet view, **operator keys only** (`403 OPERATOR_REQUIRED`; it reveals when owners are active, S-11). `ExecutorOut` (never the token hash): `executor_id, owner, pool, hostname, chip, total_memory_gb, os_version, engines, status, profile, budget (nullable; an unreadable stored value is returned as null), paused_reason, registered_at, last_seen_at, last_heartbeat_at, online, busy`. Keyset-paginated on `(registered_at, executor_id)` (cursor carries both, strictly validated; `ExecutorPage {items, next_cursor}`; `?status=&pool=&cursor=&limit=`, `limit` 1..500, default 200). `busy` = owns an acquirement with a heartbeat row; `online` = `ACTIVE` and heard from within `MINION_EXECUTOR_ONLINE_BUSY_S` (600) when busy, else `MINION_EXECUTOR_ONLINE_IDLE_S` (120) (ARCHITECTURE §7) |

### 6.2 Executor-facing **[paper: acquire, state update, heartbeat, publish]**
| Method | Path | Purpose |
|---|---|---|
| `POST` | `/executor/register` | Report host capabilities + engines + capacity profile; returns `executor_id` |
| `PUT` | `/executor/preference` | Set/clear `(team?, project?, task_name?)` preference |
| `POST` | `/executor/acquire` | Body: `{engines, budget: {profile, memory_gb, gpu_duty_cycle, max_batch_size, cpu_priority? (background|low|normal)}}`. Server filters candidates by `model.memory_gb ≤ budget.memory_gb`, engine and `allowed_pool`. Body also carries `output_upload: "post"` (capability marker): without it the server answers `426 EXECUTOR_OUTDATED` before claiming anything (ADR-0017). `200` + assignment (presigned input GET URL, presigned output POST `output_post {url, fields}`, `max_output_bytes`, model entry, params, `acquirement_id`, `attempt`) or `204` = no eligible work (empty body) |
| `POST` | `/executor/subtasks/{acquirement_id}/heartbeat` | Liveness + progress + current budget/pause state. Body (all optional, unknown fields rejected): `progress? {records_done, records_total}` (ints 0..2^31-1, `done ≤ total`; `total` above the subtask's `record_count` → `422 INVALID_PROGRESS`), `budget?` (`memory_gb` 0..1024 finite, `gpu_duty_cycle` 0..1, `max_batch_size` 1..4096, `cpu_priority?` `background|low|normal`), `paused_reason?` (`[a-z_]{1,64}`). Response `200 {cancel: bool, output_post, expires_at}` (`output_post` is a refreshed presigned POST for the same attempt key); stale/foreign `acquirement_id` → `409 STALE_ACQUIREMENT` |
| `POST` | `/executor/presence` | Low-frequency ping while idle/paused: `{budget, paused_reason?}` (`budget.cpu_priority` optional; `paused_reason` `[a-z_]{1,64}`). Response `204`; updates the caller's own executor row only (profile, budget, paused_reason, `last_seen_at`); revoked/invalid token → `401`; bad body → `422`. `acquire` clears `paused_reason` |
| `POST` | `/executor/subtasks/{acquirement_id}/complete` | Report success + output metadata (record count, checksum) |
| `POST` | `/executor/subtasks/{acquirement_id}/fail` | Report error `{retryable, code, message}` |
| `POST` | `/executor/subtasks/{acquirement_id}/release` | Voluntarily give back (graceful stop). Optional body `ReleaseRequest {code}` (`^[A-Z0-9_]{1,64}$`, e.g. `BUDGET_DOWNGRADE`, `PAUSE_RELEASE`, `STOP`): reason for dashboards, stored in the attempt history |

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
short diagnostic), `counts: {total, pending, running, succeeded, failed, cancelled}`; `pending` is derived as `max(0, total - running - succeeded - failed - cancelled)` and reads 0 while the task is SPLITTING or its split never finished.

## 7. Data model (PostgreSQL — 8 tables)

The paper states 7 tables (we add an eighth, `minion_task_counter`, ADR-0015) and names `minion_subtask_acquirement` and a heartbeat table. Proposed set **[ours]**:

1. `minion_task` — `(team, project, task_name)` PK/unique; type, model_id (→ allow-list; engine and memory_gb snapshotted at create), input_uri, output_uri, params jsonb, status, priority, total_subtasks, created_by, timestamps. `output_uri` is UNIQUE; `split_attempts` and `next_split_at` implement the splitter lease and retry backoff.
2. `minion_subtask` — `(team, project, task_name, subtask_id)` unique; input slice, status, attempt, max_attempts, error, output_uri, timestamps.
3. `minion_subtask_acquirement` — **UNIQUE `(team, project, task_name, subtask_id)`** **[paper]**; `acquirement_id` uuid, executor_id, attempt, acquired_at.
4. `minion_executor_heartbeat` — acquirement_id (the executor is reached by joining `minion_subtask_acquirement`), last_seen_at, progress jsonb (one row per active acquirement, upserted).
5. `minion_executor` — executor_id, token_hash, hostname, owner, pool, chip, total_memory_gb, os_version, engines, status, **profile, budget jsonb, paused_reason**, registered_at, last_seen_at, token_expires_at (NULL = no expiry; auth rejects tokens past it).
6. `minion_executor_preference` — executor_id, team, project, task_name (nullable for wildcards).
7. `minion_subtask_attempt` — history: one row per finished attempt (executor, outcome `SUCCEEDED|FAILED|EXPIRED|RELEASED|CANCELLED`, duration, error). Feeds Grafana.
8. `minion_task_counter` **[ours]** — `(team, project, task_name, shard)` PK, `succeeded`, `failed`, `cancelled`; sharded by `subtask_id % 16` so concurrent completions of one task do not serialize on one `minion_task` row; counts are the sum over shards ([ADR-0015](adr/0015-sharded-task-counters.md)). `minion_ro.task` exposes the sums as `succeeded_subtasks` / `failed_subtasks`.

The model allow-list is configuration (`models.yaml`, loaded by the server at start), not a table.

Full DDL lives in Alembic migrations; ARCHITECTURE §5 explains indexes and the acquire query.

## 8. Security & privacy

- S-1. Executors run on personal work laptops: data confidentiality is the main risk **[paper]**.
  Mitigation: deploy per team/org with dedicated DB and isolated executor pools **[paper]**; tasks may be
  restricted to an executor pool via `allowed_pool` **[ours]**. Per-team policy in the API keys file
  (`teams: {slug: {default_pool, allowed_pools, max_live_tasks}}`): a task without `allowed_pool` gets the team default,
  a pool outside `allowed_pools` is `403 POOL_NOT_ALLOWED`, and a team with `allowed_pools` but no default
  needs an explicit pool (`422 POOL_REQUIRED`). Teams without a policy are unrestricted. `max_live_tasks`
  (1..10000) overrides the global live-task cap for that team (FR-9) and does not affect pool policy.
- S-2. Executors never hold DB or S3 credentials; only per-subtask presigned URLs (TTL ≤ 1 h, refreshed
  via heartbeat) **[ours]**. The input GET URL signs the subtask's `Range: bytes=byte_start-(byte_end-1)` header, so
  on backends that enforce SigV4 signed headers (AWS S3, MinIO) it can read only that slice (403 to a
  missing or different Range); verify every backend before production with `minion-server-admin check-storage` (a required deploy step; exit 1 if the signed Range is not enforced; the dev SeaweedFS passes, see `test_storage_check.py`). The boundary is per URL, not per executor: an executor
  holds URLs only for subtasks it owns, and `allowed_pool` is the control for confidential tasks. The output is a presigned POST scoped to
  one attempt key whose signed policy carries `content-length-range 0..MINION_MAX_OUTPUT_BYTES`, so S3
  itself refuses a larger upload (ADR-0017). The HEAD check at complete stays as defence in depth: an attempt object above `MINION_MAX_OUTPUT_BYTES` (default 4 GiB) is never promoted: `422 OUTPUT_TOO_LARGE`, after which the
  server best-effort deletes the attempt object. The executor reports it as a non-retryable
  `OUTPUT_TOO_LARGE` failure (a re-run gives the same output). The assignment carries the server cap
  (`max_output_bytes`) and the executor refuses to upload a larger file (`min` of it and
  `MINION_EXECUTOR_MAX_OUTPUT_BYTES`; both `<= 5 GiB`; fail `OUTPUT_TOO_LARGE`, not retryable).
  The guarantee starts one presign TTL after the cutover (older PUT URLs live up to 1 h). Run S3 behind a body-limit proxy (`max_output_bytes` + 64 KiB): some stores (SeaweedFS) spool an oversize POST before checking the range. Upgrade the server before the executors.
  Attempts times the cap can still accumulate under `_attempts/`: operators should keep versioning off, set a bucket quota and expire `_attempts/` objects with an S3 lifecycle rule (ARCHITECTURE §3.3).
- S-3. Executor tokens (a token cannot hoard work: at most `MINION_MAX_OPEN_ACQUIREMENTS` open acquirements, FR-23; revoked or rotated-away tokens are also re-checked in every claim round, ADR-0019): random 256-bit (`token_urlsafe(32)`), stored as SHA-256 hash only, revocable,
  scoped to a pool, optional expiry (`token_expires_at`). Tokens are pre-issued by the operator with
  `minion-server-admin issue-executor-token --owner <name> [--pool default] [--ttl-days N]` (N = 1..365,
  default `MINION_EXECUTOR_TOKEN_TTL_DAYS`, 90); the token is printed once and the executor row starts as
  `(unregistered)`. Revoke with `minion-server-admin revoke-executor-token --executor-id <uuid>`. Auth
  rejects an expired token with `401` on its next request. A revoked or rotated-away token is refused at once by acquire, register, presence and heartbeat (their UPDATE matches the live row and token hash), and by complete/fail/release within
  `MINION_EXECUTOR_AUTH_CACHE_TTL_S` (10 s default, 0..60; 0 = immediately; per server worker, ADR-0018).
  Tokens and their hashes are never logged.
- S-4. Executor deletes local input/output files after upload; model cache is retained.
- S-5. No payload content in logs or metrics.
- S-6. TLS required between executor and server outside local dev.
- S-7. Model ids are allow-listed per deployment (executors must not pull arbitrary remote code;
  `trust_remote_code=False`).
- S-8. User API keys: `minion-server-admin generate-api-key --name <name> --teams a,b [--operator]
  [--expires-days N]` prints the key once and a YAML entry holding only its SHA-256 hash. A key has at most 64 teams (more fails the file load). Entries live in
  the file named by `MINION_API_KEYS_FILE`; lookup is by SHA-256 with `hmac.compare_digest`. The file is
  re-read when it changes (checked every `MINION_API_KEYS_RELOAD_S`, default 5 s), so removing a key
  revokes it within that interval without a restart. A file that becomes invalid fails closed (`401` for
  all keys, error logged) until fixed. Keys may carry `expires_at` (expired = `401`) and `operator: true`
  (`require_operator`, `403 OPERATOR_REQUIRED` otherwise). No DB table (ADR-0007). Replace the file
  atomically (temp + rename); it must be on local storage, owned by root or the server user, and not
  group/world-writable; every directory on its resolved path (symlinks followed, including the directory
  holding a symlink) must be owned by root or the server user and not group/world-writable (sticky
  root-owned exempt), else the server refuses it (fails closed). Duplicate YAML keys are rejected.
  `created_by` on a task records the key name; there is no foreign key (won't-do, ADR-0007 amendment).
  Executor tokens: `list-executors` (never prints hashes) and `rotate-executor-token --executor-id`
  (same id, new token shown once, old token `401` at once for acquire/register/presence/heartbeat and within `MINION_EXECUTOR_AUTH_CACHE_TTL_S` for complete/fail/release, ADR-0018).
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
- S-11. Presence leaks activity **[ours]**. Budgets and profile reach the server on every
  acquire/heartbeat/presence; with adaptive switching (FR-52) a profile change from `light` to `full`
  reveals that the owner went idle (away from the keyboard), and back. Owners who do not want that pin a
  profile (`run --profile` or `minion-executor profile <name>`). Keyboard idle time itself is never sent.
  Likewise a drop to `light` that coincides with a meeting reveals that the owner is probably in a call
  (under the default `on_call = "auto"` it looks like any other drop to light; pinning a profile hides
  it; this cover holds only while light is otherwise reachable, which is the default: if the owner sets
  `active_profile` and `idle_profile` both above light, light appears only in calls, so pin a profile or
  use `ignore`). `on_call = "light"` on a pinned executor reveals calls the same way. `on_call = "pause"` reveals it outright (`paused_reason = in_call`); `"light"` caps pinned
  profiles too. Set `on_call = "ignore"` in `executor.toml` to avoid it. The microphone/camera state
  itself is never sent (the local status file only).
- S-12. Read-endpoint cost **[ours]**. `GET /tasks` and the task detail read the per-status counts from the counter shards and the task's acquirement rows (bounded by executors x `MINION_MAX_OPEN_ACQUIREMENTS`), never from `minion_subtask`; `pending` is derived as `max(0, total - running - succeeded - failed - cancelled)`, so it reads 0 while a task is SPLITTING (total is not set yet). `GET /executors` joins heartbeats; both are paginated and capped but not free. Deployments must rate-limit the read endpoints (`/api/v1/tasks`, `/api/v1/executors`) at the reverse proxy (per key / per IP); the server has no built-in limiter.
- S-13. Web UI **[ours]** (ADR-0010). Off (`404`) unless `MINION_UI_SESSION_SECRET` (>= 32 bytes) is set. Login with
  a user API key (checked like an API request; wrong, expired and removed keys all get one `401` page; the
  key is never logged or echoed) sets `minion_session`, a stateless cookie HMAC-SHA256-signed over the key
  hash, issue and expiry time (`MINION_UI_SESSION_TTL_S`, default 8 h): `HttpOnly`, `SameSite=Strict`,
  `Path=/ui`, `Secure` unless the request host is loopback. Each request re-resolves the key in the live
  keys file (the cookie carries an HMAC key id, never the key hash; validity is age-based: `min(expiry, issued + TTL)`, TTL max 7 days), so a removed or expired key ends the session within `MINION_API_KEYS_RELOAD_S`; team scoping is
  the REST API's (`403` for another team's task). Every `POST` (login included) needs a matching `Origin`
  (else `Referer`) and a per-session HMAC CSRF token, else `403`. All `/ui` responses carry `Content-Security-Policy:
  default-src 'self'; script-src 'self'; frame-ancestors 'none'; form-action 'self'; base-uri 'none'`,
  `X-Content-Type-Options: nosniff`, `Referrer-Policy: same-origin` (with `no-referrer` browsers send `Origin: null` on same-origin form POSTs, which are rejected), `Strict-Transport-Security` when the server sees https, `Cache-Control: no-store`. Templates are
  autoescaped: executor-supplied strings (owner, hostname, task `error`) are rendered inert. Rate-limit
  `POST /ui/login` at the reverse proxy; the proxy must forward `Host` including the port (nginx `$http_host`, not `$host`), and a non-loopback UI requires TLS (cookies are `Secure`). Optional `MINION_UI_ALLOWED_HOSTS` (comma list of `host[:port]`) makes other `Host` values `400`; entries are bare `host[:port]` (a scheme, `/`, `@`, whitespace or an empty item fails at startup). Production should set it (DNS rebinding). Generate the secret with `minion-server-admin generate-ui-secret` (hand-picked phrases are weak; only single-character secrets are rejected). OIDC/SSO: put a proxy in front.
- S-14. Submitter S3 credentials **[ours]**. The `minion` CLI (`minion-cli`, TASK-25) uploads inputs
  itself, so it holds S3 credentials (`MINION_CLI_S3_ACCESS_KEY`/`MINION_CLI_S3_SECRET_KEY`, or the boto3
  default chain). These must never be the server's credentials. The policy must grant: `s3:PutObject`,
  `s3:AbortMultipartUpload` and `s3:GetObject` on `inputs/{team}/*` (inputs above 8 MiB upload as
  multipart; configure a bucket lifecycle rule that aborts incomplete multipart uploads after a few
  days so interrupted uploads do not accumulate); `s3:GetObject` on `outputs/{team}/*`; `s3:ListBucket`
  conditioned on `s3:prefix` being `inputs/{team}/*` or `outputs/{team}/*`. It must **not** grant
  `s3:DeleteObject` on either prefix and must not grant `s3:PutObject` on `outputs/` (results are
  written only through presigned URLs). Write access to `inputs/{team}/` is team-wide, so submitters
  of one team trust each other (intra-team trust); enable bucket versioning or Object Lock on
  `inputs/` to make an overwrite of another submitter's input recoverable. Executors never hold these
  credentials (S-2); they keep using presigned URLs. The CLI's API key comes from `MINION_API_KEY` or
  the Keychain item `open-minions-api-key` and is never printed. Only `https` server and S3 endpoints
  (or `http` to loopback) are accepted (as for the executor), including an endpoint taken from the boto3 environment/config.
  `minion fetch` claims its output directory with a `manifest.json` naming the task (team, project,
  name, output URI, creation time) and refuses another task's directory; a part is reused only if its
  content matches the remote object (md5 of a plain ETag, else ETag + sha256 recorded in the manifest).
  Input resume likewise compares content (`x-amz-meta-sha256` set at upload, else a plain-md5 ETag).

## 9. Non-functional requirements

| ID | Requirement |
|---|---|
| NF-1 | Acquire endpoint p99 < 200 ms with 500 executors and 1M pending subtasks, within a tested scope ([ADR-0016](adr/0016-nf1-workload-scope.md)): 0.9 to 1M PENDING, production heartbeat 300 s / watchdog timeout 900 s, first-acquire jitter 15 s, 75 s subtasks, aggregate acquire rate up to the measured passing rate (about 6.6/s in the realistic run, 23.5/s at 50 sims); above that rate the server sheds load fail-fast (503 + `Retry-After`). **Met** within that scope (M6.4, [report](bench/m6.4-scale-apple-m3-pro.md)): server p99 <= 50 ms at 500 executors and 894,924 PENDING; server p99 <= 100 ms at 50 sims (23.5/s). **Not met** for the 2 s-work stress mix at 1,000,000 PENDING (about 169 acquires/s, about 25x the realistic rate): server p99 <= 500 ms (interpolated about 435 ms), 11 % of acquire calls shed, 0 5xx. Server CPU looks like the limit, so capacity should grow with `MINION_SERVER_WORKERS` / server hosts; this is an expectation, not measured (the Mac's 12 cores were shared with the load generator), see TASK-36. |
| NF-2 | Zero double-completion: a subtask's final output is written by exactly one attempt. Verified by `minion-scale verify` after a 100k-subtask, 500-executor chaos drain (vanish/release/fail/late faults; 661 s): 0 violations, 100,000 final objects for 100,000 SUCCEEDED subtasks ([M6.4 report](bench/m6.4-scale-apple-m3-pro.md)). The 1M drain was not run. |
| NF-3 | No lost subtasks: every subtask reaches a terminal state given ≥1 live executor. Verified in the same 100k chaos drain: 0 non-terminal subtasks, 0 orphan acquirements, task counters match, all tasks terminal ([M6.4 report](bench/m6.4-scale-apple-m3-pro.md)). The 1M drain was not run. |
| NF-4 | Executor idle CPU < 1 %; no work while user has stopped it. Measured for the idle and the paused executor: `bench --cpu-check` covers the control process only (governor loop, no server round trips); the whole `run` (control plus worker process tree) is measured with `ps` (docs/bench/governor-measurements.md). |
| NF-5 | Server and watchdog are stateless and horizontally scalable (all state in Postgres). The one exception is soft, per-replica state: the executor-endpoint rate limiter (TASK-6.2, ADR-0012) keeps its counters in memory, so with N replicas (per server worker process, ADR-0012 note) an executor may be admitted up to N times per interval. Likewise the executor token cache (ADR-0018, `MINION_EXECUTOR_AUTH_CACHE_TTL_S`) is per-process soft state that only bounds how long a revoked token keeps working. Counters are never coordination state; correctness stays in the database. |
| NF-6 | Executor runs on macOS 14+ on M1 or newer. |
| NF-7 | Foreground responsiveness under `light`/`balanced`: no audible/visible degradation in a Teams/Zoom video call; UI frame drops and app-launch time within 10 % of baseline (measured in M5 usability test). Synthetic proxy on a Mac: `bench --foreground-check` (ADR-0006 Gap 2: GPU scheduling latency and app-launch time, idle vs loaded worker); the human call study is [docs/usability/nf7-video-call-protocol.md](usability/nf7-video-call-protocol.md) and is not verifiable in CI. |
| NF-8 | Budget adherence, measured by `bench --budget-check` (ADR-0006 Gap 1): (a) hard bound: the worker process's GPU busy share (its `accumulatedGPUTime` delta from `ioreg -c AGXDeviceUserClient` over wall time; device-wide utilization is not a valid per-process share on a Mac in use) ≤ `gpu_duty_cycle` + 10 pp; (b) accuracy: busy share within ±10 pp of `gpu_duty_cycle` × the unpaced share of the same workload (a CPU-bound workload cannot reach `gpu_duty_cycle`; the pacer only adds idle time); (c) worker footprint (`phys_footprint`) ≤ `memory_gb` × 1.10. |
| NF-9 | Reaction time: governor reacts to memory-pressure / thermal / power / user-return events within 5 s (`MINION_EXECUTOR_SIGNAL_POLL_S` is capped at 4 s so poll plus one engine step fits; pause new steps, or release on memory `critical`). Release is measured to worker exit. CI: 20 trials with the fake engine (`test_governor_reaction.py`); Mac: docs/bench/governor-measurements.md. |

## 10. Future work (out of scope for v1) **[paper]**
- Distributed training on the fleet.
- Secure federation of idle Macs across organizations or globally.

## 11. Open questions
- Q1. Server-side splitting: pre-split into separate S3 objects, or byte ranges on the original? (ARCH §5.2 proposes byte ranges.)
- Q2. Priority/fairness across teams beyond FIFO + preference?
- Q3. Should executors batch-acquire (N subtasks) to cut round-trips for very small subtasks?
- Q4. Auth for the web UI (SSO/OIDC vs API keys only)? **Answered (ADR-0010):** API-key login with a stateless HMAC session cookie; OIDC/SSO is left to a reverse proxy.
- Q5. Is step-level duty cycling enough to keep video calls smooth, or do we also need to throttle on
  measured *total* GPU utilization (ioreg `Device Utilization %`)? **Open.** The M3 benchmark ran at
  duty cycle 1.0 with no foreground load, so it does not answer this; decide with the M4 governor
  measurements (NF-7, NF-8). **Still open after the TASK-6.9 NF-7 proxy run**
  ([docs/bench/m5-foreground-apple-m3-pro.md](bench/m5-foreground-apple-m3-pro.md)): within its NF-8
  duty, minilm-mlx fails the probe's miss-rate criterion at light (+1.1 pp) and balanced (+1.6 pp)
  while qwen2.5-0.5b-mlx passes, so duty share alone does not bound foreground tail latency for short,
  frequent steps; GPU frequency scaling confounds the latency ratios. Follow-up TASK-24.
- Q6. Should `memory_gb` estimates in `models.yaml` be measured automatically by `minion-executor bench`
  and fed back to the server? **Partly answered by M3:** `bench` measures worker RSS peak and
  per-engine peak; `memory_gb` is still hand-set in `deploy/bench-models.yaml`, and feeding measured
  values to the server is **not implemented** (open). The M3 data shows the hand-set 1.5 GB for
  Qwen2.5-0.5B is below the HF worker RSS peak (2.26 GB) and above the MLX peak (1.42 GB); not reconciled.
