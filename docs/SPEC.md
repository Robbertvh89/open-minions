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
    params: `temperature`, `max_tokens`, `system`, etc.
- FR-4a. Each task names a `model` from the deployment's **model allow-list** (`models.yaml`). An entry
  defines `model_id`, `type`, `engine` (`mlx` | `ollama` | `hf`; plus `fake`, a test/dev-only engine for the fake runner, never in a production allow-list), engine-specific reference (HF repo for
  MLX/HF, tag for Ollama), and `memory_gb` (peak resident estimate incl. KV cache at default batch size).
  Default engine is `mlx` **[ours, ADR-0005]**; the paper used HF transformers + Ollama.
- FR-5. Input format **[ours]**: one or more JSONL objects under an S3 prefix. Splitting is by record
  count (`records_per_subtask`, default 500). Each subtask stores `(input_uri, byte_offset, byte_length)`
  or a pre-split object key — see ARCHITECTURE §5.2.
- FR-6. Output **[ours]**: one JSONL object per subtask at
  `{output_prefix}/{subtask_id:06d}.jsonl`, each line `{id, embedding|completion, model, ...}`.
  Written to an attempt-scoped key first and promoted on completion (idempotency).
- FR-7. Users can query task status, per-subtask status, progress counts, and the output location **[paper]**.
- FR-8. Users can cancel a task; pending subtasks become `CANCELLED`, running ones are told to stop on
  their next heartbeat **[ours]**.

### 4.2 Execution (Executor)
- FR-10. Executor registers with the server using a pre-issued token and reports host capabilities
  (chip, unified memory, macOS version, available engines) and its configured capacity profile **[ours]**.
- FR-11. Executor loop **[paper]**: poll for eligible subtask → acquire → download input → ensure model
  cached → run inference → upload output → report result.
- FR-12. Models are downloaded on first use and cached locally **[paper]**. Cache size is bounded
  (LRU eviction, configurable, default 50 GB) **[ours]**.
- FR-13. Engines **[ours, ADR-0005]**:
  - `mlx` (default): completions via `mlx-lm` batched generation (`BatchGenerator`), embeddings via
    `mlx-embeddings`. All records of a subtask are processed in batches sized by the capacity budget.
  - `ollama` (fallback, e.g. GGUF-only models): local Ollama HTTP API **[paper]**.
  - `hf` (fallback): HuggingFace transformers on `mps` **[paper]**.
  - Inference runs in a worker subprocess; the loaded model is kept warm across consecutive subtasks
    of the same model and unloaded when the budget drops or the executor idles > 10 min.
- FR-14. Heartbeat every **5 minutes** while running a subtask **[paper]**. Heartbeat response may carry
  `cancel=true`.
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
"finish-or-release within grace" wording). Not yet implemented: FR-12 model cache, FR-13 MLX/Ollama/HF
runners, FR-14 heartbeat, FR-16 engine filtering beyond the `engines` list sent on acquire, FR-18/§4.6
governor (M4). ARCHITECTURE §2.2 describes the executor as built.

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
  watchdog deletes the acquirement, archives it as `EXPIRED`, increments `attempt`, and sets the
  subtask back to `PENDING` **[paper semantics]**.
- FR-32. The retry budget is the number of `FAILED` + `EXPIRED` attempts; once it reaches
  `max_attempts` (default 3) the subtask becomes `FAILED`; task becomes `FAILED` or `PARTIAL`
  depending on policy. A voluntary `release` does not consume the budget **[ours]**.
- FR-33. Executor-reported errors: retryable (OOM, download failure) → requeue; non-retryable (bad
  input) → `FAILED` immediately **[ours]**.
- FR-34. A late result from a reclaimed owner is rejected (`409 STALE_ACQUIREMENT`) **[ours]**.

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
  | `balanced` (default) | 40 % of RAM | 50 % | 8 | low | Normal office day |
  | `full` | 70 % of RAM | 100 % | 32 | normal | Owner away, overnight |
  | `custom` | user-defined | | | | |
  Memory share is capped at 75 % of RAM regardless of config (headroom for macOS + apps).
  **Status (TASK-2.3):** the three built-in profiles are implemented as static values
  (`capacity/profiles.py`), with the 75 % cap. `~/.config/minion/executor.toml` overrides and the
  `custom` profile are **not implemented**. `cpu_priority` is applied only as a `nice` value
  (15 / 10 / 0) in the worker; `taskpolicy -b` is not used. FR-52, FR-53, FR-54 and FR-55 are M4;
  FR-56 is partially met (`run --profile` only; no `profile`, `pause`, `resume`, `status`).
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
and all of §6.2 except `heartbeat` and `presence`. The other rows are not yet implemented.

### 6.1 User-facing
| Method | Path | Purpose |
|---|---|---|
| `POST` | `/tasks` | Create task. `input_uri` and `output_uri` are prefixes: a missing trailing `/` is added, and `output_uri` must be unique across tasks. Body: `team, project, task_name, type, model, input_uri, output_uri, params, records_per_subtask (1..100 000, default 500), priority? (-1000..1000, default 0), allowed_pool?` → `202` + task |
| `GET` | `/tasks?team=&project=&status=` | List tasks |
| `GET` | `/tasks/{team}/{project}/{task_name}` | Task detail + progress counts (`TaskOut`, below) |
| `GET` | `/tasks/{team}/{project}/{task_name}/subtasks` | Paginated subtasks |
| `POST` | `/tasks/{team}/{project}/{task_name}/cancel` | Cancel |
| `GET` | `/executors` | Fleet view: online, busy, last heartbeat |

### 6.2 Executor-facing **[paper: acquire, state update, heartbeat, publish]**
| Method | Path | Purpose |
|---|---|---|
| `POST` | `/executor/register` | Report host capabilities + engines + capacity profile; returns `executor_id` |
| `PUT` | `/executor/preference` | Set/clear `(team?, project?, task_name?)` preference |
| `POST` | `/executor/acquire` | Body: `{engines, budget: {profile, memory_gb, gpu_duty_cycle, max_batch_size}}`. Server filters candidates by `model.memory_gb ≤ budget.memory_gb`, engine and `allowed_pool`. `200` + assignment (presigned input GET URL, presigned output PUT URL, model entry, params, `acquirement_id`, `attempt`) or `204` = no eligible work (empty body) |
| `POST` | `/executor/subtasks/{acquirement_id}/heartbeat` | Liveness + progress + current budget/pause state; response `{cancel: bool}` |
| `POST` | `/executor/presence` | Low-frequency ping while idle/paused: `{budget, paused_reason?}` (fleet view only) |
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
  measured *total* GPU utilization (ioreg `Device Utilization %`)? Decide after M3 benchmark.
- Q6. Should `memory_gb` estimates in `models.yaml` be measured automatically by `minion-executor bench`
  and fed back to the server?
