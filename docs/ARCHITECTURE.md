# open-minions — Architecture

Status: **Draft v0.2** · 2026-10-15 · Companion to [`SPEC.md`](SPEC.md)

## 1. Overview

```
             ┌──────────── users ────────────┐
             │  Web UI / REST (create, status)│
             └───────────────┬───────────────┘
                             ▼
┌────────────┐  SQL  ┌───────────────┐  SQL  ┌──────────────┐
│ PostgreSQL │◀─────▶│ Minion Server │       │   Watchdog   │
│ (7 tables) │◀──────┴───────────────┴──────▶│ (reclaimer)  │
└─────┬──────┘        ▲   REST (acquire,      └──────────────┘
      │ SQL           │   heartbeat, complete,
      ▼               │   fail, release)
┌──────────┐          │
│ Grafana  │   ┌──────┴───────┬──────────────┬──────────────┐
└──────────┘   │ Executor Mac │ Executor Mac │ Executor Mac │  … N
               │ governor+MLX │ governor+MLX │ governor+MLX │
               │ (Ollama/HF)  │ (Ollama/HF)  │ (Ollama/HF)  │
               └──────┬───────┴──────┬───────┴──────┬───────┘
                      │ presigned GET/PUT           │
                      ▼                             ▼
                  ┌──────────────── S3 / SeaweedFS─────────┐
                  │ inputs/…   outputs/…   (no traffic via │
                  │                         the server)    │
                  └────────────────────────────────────────┘
```

**Design principles (from the paper):** pull, not push · Postgres as the single coordination point ·
the server is the only DB client · bulk data bypasses the server · liveness via heartbeats + reclaim.

## 2. Components

### 2.1 minion-server (FastAPI)
```
minion_server/
  app.py               # FastAPI factory, lifespan (DB engine, API keys file)
  api/
    tasks.py           # user endpoints
    executor.py        # executor endpoints
    ui.py              # server-rendered UI (Jinja2 + htmx) — minimal
  services/
    task_service.py    # validation, create, cancel, status derivation
    splitter.py        # async partitioning of input into subtasks
    acquisition.py     # acquire / complete / fail / release logic
    presign.py         # S3 presigned URL generation
  repositories/        # ALL SQL lives here
  auth.py              # user API keys (file), executor tokens (DB)
  settings.py          # ServerSettings: MINION_* env vars (pydantic-settings)
  admin.py             # minion-server-admin CLI (typer)
```
- **Splitter** runs as a background job (`asyncio` task drained from an in-DB `SPLITTING` queue so it
  survives restarts; any server replica can pick it up with `SELECT ... FOR UPDATE SKIP LOCKED` on
  `minion_task`). Inserts subtasks in batches of 1 000 via `COPY`/`executemany`.
- Stateless; run ≥2 replicas behind a load balancer in production.

#### Foundation (implemented, TASK-2.1)
Files that exist today; the endpoints and services above are built on them in TASK-2.2.
- **`settings.py`** — `ServerSettings` reads `MINION_*` (and `.env`). Required: `database_url`,
  `s3_access_key`, `s3_secret_key`. `s3_input_prefixes` / `s3_output_prefixes` must be non-empty and end
  with `/`. `presign_ttl_s` ≤ 3600. The public S3 endpoint (`s3_public_endpoint_url`, defaults to
  `s3_endpoint_url`) must be https unless it is loopback.
- **`db/session.py`** — the only place the async engine and sessionmaker are built. The `app.py`
  lifespan creates the engine, loads the API keys file (if `MINION_API_KEYS_FILE` is set) into
  `app.state`, and disposes the engine on shutdown. `S3Storage` is not yet constructed in the lifespan;
  wiring it in is part of TASK-2.2.
- **`storage.py`** — `S3Storage` wraps sync boto3 and runs every call in `asyncio.to_thread` (plan D3).
  It holds three clients: the default one for head/list/get, a copy client with a longer read timeout
  for CopyObject, and a signer bound to the public endpoint that is used only for presigning. Path-style
  addressing, SigV4.
- **`uris.py`** — `parse_s3_uri(uri, bucket=, prefixes=)` accepts only `s3://<configured bucket>/<key>`
  where the key starts with an allowed prefix. It rejects other schemes, query/fragment, whitespace and
  control characters, `//`, backslashes, and `.`/`..` segments. Violations raise `InvalidUriError`, which
  handlers map to 422.
- **`auth.py`** — `current_user`: API keys from the file loaded at startup (ADR-0007). Each presented key
  is hashed with SHA-256 and compared against every stored hash with `hmac.compare_digest`, with no early
  exit. No file or an empty file fails closed (401). `current_executor`: the token is hashed and looked up
  by its unique index, then compared with `compare_digest`. A missing, REVOKED or expired
  (`token_expires_at`) token gets 401. Secrets are never logged.
- **`models_registry.py`** — `load_models(path)` parses `MINION_MODELS_FILE` (default
  `deploy/models.yaml`) into a dict keyed by `model_id`. Duplicate ids are rejected.
- **`repositories/executors.py`** — the first repository module: token-hash lookup, unregistered-executor
  insert, and revoke.
- **`admin.py`** — `minion-server-admin`: `generate-api-key` (no DB, prints the key once and the YAML
  entry with its hash), `issue-executor-token` and `revoke-executor-token` (DB).
- **Test fixtures** — repo-root `conftest.py` provides session-scoped testcontainers: `postgres_url`
  (`postgres:16-alpine`), `migrated_url` (Alembic head), `engine`, and `s3` (SeaweedFS
  `chrislusf/seaweedfs:4.05` with `deploy/seaweedfs/s3.json` mounted, bucket created). The `s3` fixture
  is the one the presigned round-trip tests use. Integration tests carry the `integration` marker and need
  Docker.

### 2.2 minion-executor (macOS agent)
```
minion_executor/
  main.py              # CLI (typer): run, register, status, prefer, profile, pause, resume, bench
  loop.py              # poll → acquire → run → report state machine (control process)
  heartbeat.py         # background task, 5-min interval, handles cancel
  capacity/
    profiles.py        # light / balanced / full / custom → Budget dataclass
    signals.py         # HID idle time, AC/battery, Low Power Mode, thermal state, memory pressure (pyobjc)
    governor.py        # picks active budget, emits pause/resume/downgrade events (§3.6)
  worker/
    process.py         # spawns + supervises inference subprocess; IPC via multiprocessing Pipe
    throttle.py        # duty-cycle pacer used inside the worker between steps/batches
  runners/             # run INSIDE the worker subprocess only
    base.py            # Runner protocol: load(model, budget) / run(batch) -> results / unload()
    mlx_completion.py  # mlx-lm BatchGenerator (default)
    mlx_embedding.py   # mlx-embeddings (default)
    ollama.py          # fallback: Ollama HTTP (localhost:11434)
    hf.py              # fallback: transformers / sentence-transformers on mps
  model_cache.py       # HF hub cache + LRU size bound; Ollama models tracked by tag
  host.py              # capabilities: chip, unified memory, engines installed
  bench.py             # engine × model throughput / memory benchmark
  io.py                # streaming download/upload via presigned URLs
```
- Installed via `uv tool install minion-executor`; optional `launchd` LaunchAgent in `deploy/`.
- **Two processes**: the async *control process* (polling, heartbeats, governor, I/O) and a *worker
  subprocess* (spawned with `multiprocessing` `spawn`) that owns Metal/MLX and the loaded model. The
  worker is reused across subtasks of the same model and killed to reclaim memory on budget downgrade,
  model switch, idle timeout or crash. Worker crash → subtask reported `fail(retryable=true)`.
- Worker process runs with background QoS / `nice` per `cpu_priority`; MLX allocator limits set at
  model load from `budget.memory_gb`.
- Uses `caffeinate`-free design: it does **not** prevent sleep. If the Mac sleeps, heartbeats stop and
  the watchdog reclaims — this is intended **[paper]**.

### 2.3 minion-watchdog
- Single loop every 60 s. Multiple replicas are safe: each reclaim is a single transaction guarded by
  `DELETE ... RETURNING` on the acquirement row.
- Only examines rows in `minion_executor_heartbeat` (i.e., executors *currently running* work) **[paper]**.

### 2.4 minion-common
Pydantic request/response models, enums (`TaskType`, `SubtaskStatus`, ...), typed `httpx` API client
used by the executor and by tests, shared settings base.

## 3. Key flows

### 3.1 Task submission
```
User ─POST /tasks─▶ Server: validate (team membership, model allow-list + type, team-scoped S3 URIs
                    `{prefix}{team}/…`, input prefix non-empty, no `_attempts` segment in output)
                    INSERT minion_task(status=SPLITTING)  ──▶ 202
Splitter (async):   claim one due SPLITTING task with a lease (split_attempts+1, next_split_at=now+lease)
                    list input objects → scan JSONL → stream batches of slices of N records
                    per batch: compare with existing rows (key, byte range; mismatch → INPUT_CHANGED),
                    INSERT minion_subtask(status=PENDING, attempt=0) ON CONFLICT DO NOTHING
                    UPDATE minion_task SET status=PENDING, total_subtasks=k
```
Splitting is bounded (line length, total bytes, objects, subtasks per task; see `MINION_SPLIT_*`)
and no transaction is held open while reading S3. Deterministic problems (bad URI, empty input,
limit exceeded, `LINE_TOO_LONG`, `INPUT_CHANGED`, S3 `AccessDenied`/`NoSuchKey`/`InvalidObjectState`)
mark the task `FAILED` with `error`. Anything else backs off exponentially via `next_split_at` and
fails the task after `split_max_attempts`, so one bad task never blocks the queue. `output_uri` is
unique per task (`409 OUTPUT_URI_IN_USE`): attempt and promoted keys are derived from it.

### 3.2 Acquire (the core protocol)
```
Executor ─POST /executor/acquire {engines, budget}─▶ Server
  (executor only calls this when governor state ≠ PAUSED)
  candidates = SELECT pending subtasks
               WHERE task.engine = ANY(engines)
                 AND task.model_memory_gb <= budget.memory_gb   -- capacity admission
                 AND (matches preference)            -- pass 1
               ORDER BY task.priority DESC, task.created_at, subtask_id
               LIMIT 16
  if empty: candidates = same query without preference filter (engine + memory filters stay) -- pass 2 (global FIFO)
  for c in shuffle_head(candidates):          -- light shuffle reduces contention
      INSERT INTO minion_subtask_acquirement (...) VALUES (...)
        ON CONFLICT (team, project, task_name, subtask_id) DO NOTHING
        RETURNING acquirement_id
      if row returned:
          UPDATE minion_subtask SET status=RUNNING, attempt=attempt+1 ...  (same txn)
          INSERT minion_executor_heartbeat(...)                           (same txn)
          COMMIT → return assignment + presigned URLs
  return 204
```
Notes:
- The unique constraint is the *only* mutual-exclusion mechanism **[paper]**. The candidate SELECT is
  deliberately unlocked; losing an insert race is cheap.
- Candidate query is served by a partial index:
  `CREATE INDEX ON minion_subtask (team, project, task_name, subtask_id) WHERE status = 'PENDING'`.
- Alternative considered: `SELECT ... FOR UPDATE SKIP LOCKED`. Rejected for v1 to stay faithful to the
  paper and keep the ownership record explicit; revisit if NF-1 isn't met (ADR-0002).

### 3.3 Execute & report
```
Executor: GET input via presigned URL (Range header for byte slice)
          ensure model cached → hand records to worker subprocess
Worker:   load model (or reuse warm one) with MLX memory/cache limits from budget
          loop: run one generation step / embedding batch (≤ budget.max_batch_size)
                throttle.pace()   ← duty-cycle sleep + check governor events
          stream results back to control process
Executor: write results to local temp JSONL
          PUT to presigned URL  {output_prefix}/_attempts/{subtask_id:06d}/{attempt}.jsonl
          POST /complete {record_count, sha256}
Server:   verify acquirement is current (else 409)
          S3 CopyObject → {output_prefix}/{subtask_id:06d}.jsonl   (promotion = idempotency)
          txn: DELETE acquirement, DELETE heartbeat row,
               UPDATE subtask SUCCEEDED, INSERT minion_subtask_attempt,
               UPDATE task counters / status
```

### 3.4 Heartbeat & reclaim
```
Executor (every 300 s while running): POST /heartbeat {progress}
  Server: UPSERT minion_executor_heartbeat.last_seen_at = now()
          respond {cancel: task.status == CANCELLED, refreshed_urls?}

Watchdog (every 60 s):
  FOR each heartbeat row WHERE last_seen_at < now() - heartbeat_timeout:
    BEGIN
      DELETE FROM minion_subtask_acquirement WHERE acquirement_id = $1 RETURNING *
      -- if nothing returned, someone else already handled it → skip
      DELETE heartbeat row
      INSERT minion_subtask_attempt(outcome=EXPIRED)
      UPDATE minion_subtask SET status = CASE WHEN <FAILED+EXPIRED attempts> >= max_attempts
                                              THEN 'FAILED' ELSE 'PENDING' END
    COMMIT
```
**Retry budget.** It is the number of `FAILED` + `EXPIRED` attempts in `minion_subtask_attempt`
(`fail` already counts the attempt being recorded). A voluntary `release` does not consume it.
`attempt` itself keeps counting every acquisition (it keys the output object), so it is *not* the
budget; the watchdog (M2) must use the same count, not `attempt >= max_attempts`.
Because the acquirement row is deleted, the unique constraint frees up and any executor can re-acquire.
A zombie executor that wakes up later gets `409 STALE_ACQUIREMENT` on its next call and discards its work.

### 3.5 Dynamic task binding
Preference row `(executor_id, team, project?, task_name?)`; NULL = wildcard. Server translates it to a
`WHERE` filter in pass 1 of acquire **[paper]**. Set via `minion-executor prefer --team X --project Y`.

### 3.6 Capacity governor (partial allocation)

macOS offers no GPU quota or partitioning, and Metal exposes no public command-queue priority. The
executor therefore enforces the owner's budget cooperatively, at four levers (SPEC §4.6):

```
                 signals.py (polled every 2 s, + NSWorkspace / pressure notifications)
   HID idle time ─┐
   AC / battery ──┤
   Low Power Mode ┤        ┌──────────────┐  Budget / PAUSE / RELEASE   ┌──────────────┐
   thermal state ─┼──────▶ │ governor.py  │ ──────────────────────────▶ │ worker       │
   mem pressure ──┤        │ (state mach.)│        (Pipe)               │ throttle.py  │
   schedule/CLI ──┘        └──────┬───────┘                             └──────────────┘
                                  │ budget + paused_reason
                                  ▼
                       acquire / heartbeat / presence (server)
```

**Governor states:** `ACTIVE(profile)` ⇄ `IDLE(profile)` → `PAUSED(reason)` → back. Transitions:
- user input within `idle_after_min` → `active_profile`; idle ≥ threshold and on AC → `idle_profile`;
- schedule window overrides both; `pause` CLI / pause conditions (SPEC FR-53) → `PAUSED`;
- hysteresis: escalate (more capacity) only after the condition holds 60 s; de-escalate immediately.

**Levers:**
1. **Memory admission** — server only offers subtasks whose `model_memory_gb ≤ budget.memory_gb`.
   Inside the worker: `mx.set_memory_limit(budget)` and a small `mx.set_cache_limit` so freed buffers are
   returned to the OS instead of hoarded. Downgrade below the loaded model → finish batch → `release`
   subtask → kill worker (only reliable way to return all unified memory).
2. **GPU duty cycle** — `throttle.pace()` after each step: measure GPU-synchronized step time `t`
   (`mx.eval` + `mx.synchronize`), then sleep `t·(1−d)/d` for duty `d`. Short bursts (one decode step
   ≈ 10–50 ms) keep the window server and video encoders getting GPU time between bursts. Batch size is
   the second knob: smaller batch → shorter bursts → smoother foreground.
3. **Batch size** — capped by `budget.max_batch_size`; also bounds KV-cache memory.
4. **CPU priority** — worker started via `taskpolicy -b` (background QoS → E-cores, throttled I/O) for
   `background`, `nice 10` for `low`; tokenizer threads = 2 (`TOKENIZERS_PARALLELISM`, `OMP_NUM_THREADS`).

**Optional adaptive mode (Q5):** read total GPU utilization from IORegistry
(`AGXAccelerator` → `PerformanceStatistics` → `Device Utilization %`, no sudo) and lower duty cycle
when utilization from *other* processes rises (e.g. a video call). Off by default until benchmarked.

**Not done on purpose:** no `caffeinate`, no sleep prevention, no kernel extensions, no root.

**Tests:** governor is pure (signals in → events out) and unit-tested with fake signal sources;
throttle tested with a fake clock; budget adherence (NF-8) measured by `minion-executor bench --profile X`.

## 4. Concurrency & correctness argument

| Hazard | Guard |
|---|---|
| Two executors acquire same subtask | UNIQUE constraint on acquirement; only one INSERT returns a row |
| Watchdog reclaims while owner completes | Both paths start by deleting/validating the same acquirement row inside a txn; Postgres row lock serializes them; loser sees 0 rows → no-op / 409 |
| Reclaimed owner uploads late | Writes go to attempt-scoped key; promotion only on accepted `/complete` |
| Two watchdog replicas | `DELETE ... RETURNING` is idempotent per row |
| Server crash mid-split | Splitter is resumable: subtasks inserted with deterministic ids + `ON CONFLICT DO NOTHING` |
| Budget downgrade mid-subtask | Executor `release`s (same txn path as complete/reclaim); output of partial run discarded (attempt-scoped key) |
| Worker crash (Metal / OOM) | Control process survives, reports `fail(retryable)`; attempt counter bounds retries |

Required tests: concurrent-acquire race (≥50 parallel acquires, assert unique owners), complete-vs-reclaim
race, stale-acquirement rejection, splitter restart.

## 5. Data layer details

### 5.1 Key indexes
- `minion_subtask`: partial index on `status='PENDING'`; index on `(team, project, task_name, status)` for progress.
- `minion_executor_heartbeat`: index on `last_seen_at`.
- `minion_task`: index on `(status, priority DESC, created_at)`.

### 5.2 Input slicing **[ours]**
Splitter records `(object_key, byte_start, byte_end, first_record, record_count)` per subtask, aligned to
newline boundaries. Executors fetch with HTTP `Range` on a presigned GET. Avoids rewriting input data.

## 6. Deployment

Local dev uses SeaweedFS instead of MinIO (MinIO community images are no longer pullable).

| Env | Topology |
|---|---|
| Local dev | `deploy/compose.yml`: postgres, s3 (SeaweedFS), grafana, server, watchdog; executor runs natively on the Mac (needs Metal — not in Docker) |
| Prod | Managed Postgres (separate dev/prod DBs **[paper]**), S3, ≥2 server replicas, 1–2 watchdogs, Grafana. One deployment per team/org for isolation **[paper]** |

Executor distribution: `uv tool install`, signed `launchd` plist, config in `~/.config/minion/executor.toml`,
token in macOS Keychain.

## 7. Observability
- Grafana with Postgres datasource; dashboards provisioned from `deploy/grafana/`.
- Panels: tasks by status, subtasks/min (from `minion_subtask_attempt`), active executors
  (heartbeat < 10 min or polled < 2 min), reclaim rate, per-model throughput, top contributors,
  **fleet capacity** (Σ donated memory by profile, paused executors by reason, releases due to downgrade).
- Structured logs with `task`, `subtask_id`, `acquirement_id`, `executor_id` fields.

## 8. Roadmap (milestones)

1. **M0 – Scaffolding:** uv workspace, compose stack, Alembic baseline with 7 tables, CI (ruff, mypy, pytest).
2. **M1 – Core protocol:** task create + splitter, acquire/complete/fail, executor loop + worker subprocess with a fake runner. Race tests green.
3. **M2 – Fault tolerance:** heartbeats, watchdog, release, cancel, stale-acquirement handling.
4. **M3 – Real inference + benchmark:** MLX completion (batched) and MLX embedding runners, Ollama/HF
   fallback runners, model cache, `models.yaml` admission. `minion-executor bench`: same model on
   mlx-lm batched vs Ollama(MLX) vs HF/mps — tokens/s, records/h, peak memory. Confirms/adjusts ADR-0005 defaults.
5. **M4 – Capacity governor:** profiles, signals, duty-cycle throttle, auto-pause, budget reporting,
   downgrade-release. Measure NF-8/NF-9.
6. **M5 – Ops & usability:** Grafana dashboards, web UI, launchd packaging, auth hardening; usability
   test with real participants during video calls (NF-7) to tune default profiles.
7. **M6 – Scale test & menu-bar app:** simulated 500 executors against 1M subtasks (NF-1..NF-3);
   menu-bar UI for profile/pause.

## 9. ADR index
- [ADR-0001](adr/0001-python-for-all-components.md): Python for all components — **Accepted**
- ADR-0002: Unique-constraint acquisition over `SKIP LOCKED` *(to write)*
- ADR-0003: Presigned URLs instead of executor S3 credentials *(to write)*
- ADR-0004: Byte-range input slicing *(to write)*
- [ADR-0005](adr/0005-mlx-default-inference-engine.md): MLX as default inference engine — **Accepted**
- [ADR-0007](adr/0007-api-keys-config-file.md): User API keys from a config file, not a DB table — **Proposed**
- ADR-0006: Cooperative capacity governor (duty cycling) for partial allocation *(to write)*
