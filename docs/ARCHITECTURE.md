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
│ (8 tables) │◀──────┴───────────────┴──────▶│ (reclaimer)  │
└─────┬──────┘        ▲   REST (acquire,      └──────────────┘
      │ SQL           │   heartbeat, complete,
      ▼               │   fail, release)
┌──────────┐          │
│ Grafana  │   ┌──────┴───────┬──────────────┬──────────────┐
└──────────┘   │ Executor Mac │ Executor Mac │ Executor Mac │  … N
               │ governor+MLX │ governor+MLX │ governor+MLX │
               │ (Ollama/HF)  │ (Ollama/HF)  │ (Ollama/HF)  │
               └──────┬───────┴──────┬───────┴──────┬───────┘
                      │ presigned GET/POST          │
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
  services/
    task_service.py    # validation, create, cancel, status derivation
    splitter.py        # async partitioning of input into subtasks
    acquisition.py     # acquire / complete / fail / release logic
    presign.py         # S3 presigned URL generation
  repositories/        # ALL SQL lives here
  auth.py              # user API keys (file), executor tokens (DB)
  ui/                  # server-rendered web UI (ADR-0010); create/cancel call services/tasks.py, the same code as the REST handlers
    routes.py          # /ui: login, logout, task list, task detail (+ ?partial=1 htmx fragment)
    session.py         # stateless HMAC session cookie, CSRF tokens, Origin check
    headers.py         # CSP / nosniff / same-origin / no-store middleware for /ui
    templates/         # Jinja2, autoescape on, no inline scripts
    static/            # htmx.min.js (vendored 2.0.4, sha256 pinned in a test), ui.css
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
  lifespan creates the engine, creates the `KeyStore` (live-reloading API keys file, if `MINION_API_KEYS_FILE` is set) in
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
    sandbox.py         # Seatbelt profile (pure build_profile) + sandbox_init via ctypes (ADR-0009)
  runners/             # run INSIDE the worker subprocess only
    base.py            # Runner protocol v2: load(model, path, budget, EngineConfig) /
                       # run(batch, params, StepControl) -> results / memory_in_use() / unload();
                       # worker ops also include probe (engine versions) and stats
    prompts.py         # pure: record -> chat messages / embedding text, BAD_INPUT rules
    mlx_common.py      # memory/wired-limit levers (invariant 6), OOM detection, pooling mode
    mlx_completion.py  # mlx-lm BatchGenerator (default); checkpoint() every step
    mlx_embedding.py   # BERT encoder on mlx (no mlx-embeddings: its mlx-vlm/audio tree is huge)
    ollama.py          # fallback: Ollama HTTP (localhost:11434)
    hf.py              # fallback: transformers / sentence-transformers on mps
  model_cache.py       # pinned, sha256-verified HF snapshots in a killable fetch subprocess;
                       # LRU size bound (MINION_EXECUTOR_MODEL_CACHE_GB); Ollama models tracked by tag
  settings.py          # ExecutorSettings (MINION_EXECUTOR_*): cache, timeouts, idle unload, sandbox
  host.py              # capabilities: chip, unified memory, engines installed
  bench.py             # engine × model throughput / memory benchmark
  io.py                # streaming download/upload via presigned URLs
```
- Installed via `uv tool install './packages/minion-executor'`; `minion-executor service install|uninstall|status` manages a per-user launchd LaunchAgent (ADR-0011; example plist in `deploy/launchd/`; guide in `docs/ops/executor-install.md`).
- **Two processes**: the async *control process* (polling, heartbeats, governor, I/O) and a *worker
  subprocess* (spawned with `multiprocessing` `spawn`) that owns Metal/MLX and the loaded model. The
  worker is reused across subtasks of the same model and killed to reclaim memory on budget downgrade,
  model switch, idle timeout or crash. Worker crash → subtask reported `fail(retryable=true)`.
- **Model supply chain and sandbox (ADR-0009).** Before a load, `ModelCache.ensure` fetches the pinned
  revision in a separate `spawn` subprocess (the only process that gets `HF_TOKEN`/proxy/CA env); a
  stop or cancel kills it. When the new model fits the cache only if the loaded one goes, the loop
  unloads the worker, unprotects that model and retries `ensure` once. The worker then starts offline,
  applies the Seatbelt profile (no HOME, no network but the Ollama loopback port, no exec, no
  securityd, cache read-only, one tmp dir) and only afterwards imports the engine. A sandboxed worker's
  grants are fixed per spawn, so every load uses a fresh worker. Sandbox failure is fail-closed
  (`SANDBOX_FAILED`); the startup probe runs sandboxed, so `run` exits 2 if it is broken.
- Worker process runs with background QoS / `nice` per `cpu_priority`; MLX allocator limits set at
  model load from `budget.memory_gb`.
- Uses `caffeinate`-free design: it does **not** prevent sleep. If the Mac sleeps, heartbeats stop and
  the watchdog reclaims — this is intended **[paper]**.

#### Executor implementation (M1, TASK-2.3)
Implemented: `main.py` (`run`), `loop.py`, `io.py`, `secrets.py`, `capacity/profiles.py`,
`runners/{base,fake}.py`, `worker/process.py`, `heartbeat.py` (TASK-3.3). Later: MLX/Ollama/HF runners (TASK-4.3,
listed under the worker bullets below), model cache and worker sandbox (TASK-4.2, ADR-0009), and `bench` (TASK-4.4, `bench.py`, results in `docs/bench/`). Not yet: governor/signals, `status` (stub), `register`/`prefer`/`pause` CLI commands.

- **Control loop** (`ExecutorLoop.run`): one private temp dir (`mkdtemp`, mode 0700, removed in
  `finally`). Optional register, then repeat: `acquire` → if transport error or 5xx, backoff and retry;
  if 4xx, raise (exit non-zero); if no assignment, backoff; else handle one subtask.
- **GPU duty guard (TASK-15):** `capacity/gpu_meter.py` (in-process IOKit reader), `capacity/gpu_guard.py` (pure guard, one per worker pid), sampled in the loop's watcher (§3.6). KILL → `interrupt` + release `GPU_DUTY_VIOLATION` / `GPU_METER_UNAVAILABLE`.
- **Per subtask** (`_execute`): ranged GET of `[byte_start, byte_end)` → parse exactly `record_count`
  records → `ensure_model` + `run` in batches of `budget.max_batch_size` (worker) → write JSONL to the
  temp dir → presigned POST → `complete` with `record_count`, `byte_size`, `sha256`. In/out files are
  unlinked in `finally`.
- **Error → report mapping.** Only executor-local connectivity trouble (connect/timeout/network/
  remote-protocol errors, local `OSError`) is `release`d (immediate requeue, no retry budget burned).
  Anything S3 actually answered (403/404/5xx), a backend ignoring `Range`, or a short body is a
  retryable `fail`, so `max_attempts` bounds it (a `release` would livelock the fleet):
  | Condition | Action | Code | Retryable |
  |---|---|---|---|
  | input lines do not match `record_count` / not JSON / nested too deeply | fail | `BAD_INPUT` | no |
  | input GET: connect/timeout/network/remote-protocol/proxy error | release | `INPUT_UNAVAILABLE` | n/a |
  | input GET: HTTP error status, not `206`, or != requested bytes | fail | `INPUT_UNAVAILABLE` | yes |
  | temp-file I/O error | release | `LOCAL_IO_ERROR` | n/a |
  | output POST: connect/timeout/network/remote-protocol/proxy error | release | `UPLOAD_FAILED` | n/a |
  | output POST: HTTP error status incl. a 3xx (e.g. expired policy; SeaweedFS answers 307) | fail | `UPLOAD_FAILED` | yes |
  | output POST: `400 EntityTooLarge` (above the signed size range) | fail | `OUTPUT_TOO_LARGE` | no |
  | presigned URL not https (http only if the server URL is loopback) | fail | `INSECURE_URL` | yes |
  | worker died / timed out (per-op: load 900 s, run 600 s → kill) | fail | `WORKER_CRASHED` | yes |
  | `RunnerError` from worker | fail | runner's code (≤64 chars) | yes |
  | runner returned a different number of records than the input | fail | `OUTPUT_MISMATCH` | yes |
  | any other exception in the handler (poison pill) | fail (type name only in logs) | `EXECUTOR_ERROR` | yes |
  | `complete`: transport error / 5xx | retried with jittered backoff (≤5 tries; server treats a same-owner repeat as success), then given up: if any 5xx was seen, best-effort `fail` (retryable, burns budget); transport-only, nothing (the watchdog reclaims and counts EXPIRED) | `COMPLETE_FAILED` | yes |
  | output file above `min(local, server)` cap (`max_output_bytes`), checked before the upload | fail | `OUTPUT_TOO_LARGE` | no |
  | `complete`: `422 OUTPUT_TOO_LARGE` | fail | `OUTPUT_TOO_LARGE` | no |
  | `complete`: other 4xx except 409 | fail | `COMPLETE_REJECTED` | yes |
  | `complete` or `fail` returns 409 | discard, no retry | — | — |
- **Failure back-off.** After any failed subtask the loop sleeps (same jittered exponential delay,
  growing per consecutive failure); after `max_consecutive_failures` (5) in a row it pauses for
  `failure_pause_s` (300 s) before acquiring again. A success resets the streak.
- **Stop** (SIGTERM / SIGINT → `loop.stop`, exit 0; FR-17): the first signal stops acquiring and
  starts a grace timer (`MINION_EXECUTOR_STOP_GRACE_S`, default 30, 0..300). The in-flight subtask
  keeps running with its heartbeats, so the lease cannot expire, and a cancel or 409 still
  interrupts it. If it finishes in time it is uploaded and completed as usual. At the deadline, on
  a second signal, or with grace 0, the work task is cancelled, the worker killed to reclaim memory
  and the subtask `release`d with its own `acquirement_id` (immediate requeue, not a failure). An
  upload cut off at the deadline publishes nothing final: only `complete` promotes an object, and
  the re-run is a new attempt with its own attempt-scoped key (invariant 5), so a cut attempt's
  leftover object, if any, is never promoted. An acquire that returns after the stop is released
  untouched (no download, no model load). Once the hard stop is set a single absolute deadline
  (hard stop + 10 s, `SHUTDOWN_BUDGET_S`) covers the in-flight `complete` (then the subtask is
  released; a 409 there means `complete` had won), `release`, `fail`, worker kill and close; each
  call gets what is left, floored at 0.5 s so it still gets an attempt. Exit is therefore within
  grace + 10 s plus at most a few 0.5 s floors, even with a slow server. A kill that times out
  skips close and the fallback kill. The wait on a worker call is abandoned but its thread keeps
  running: the bound relies on every worker-lock holder aborting once the handle is `_closed`.
  An `acquire` or `register` still in flight when the first signal arrives is abandoned after
  10 s and the executor exits; an acquirement that had landed server-side is left to the watchdog
  (no heartbeat ever arrives, so it is reclaimed as EXPIRED `NOT_ACKNOWLEDGED`, which is free up to
  `MINION_RELEASE_LIMIT` per subtask, then counts). Bounded calls made during grace each get a full budget; the shared
  deadline is fixed only at the hard stop (or at the final cleanup). When the work finishes
  during the grace period the worker is killed before `complete` so GPU memory is freed
  promptly. launchd `ExitTimeOut` must be at least grace + 15 s (grace + 10 s budget + slack),
  and launchd's default is only 20 s (below the default 30 s grace), so `service install` sets `ExitTimeOut = ceil(grace) + 15` and exports it as `MINION_EXECUTOR_LAUNCHD_EXIT_TIMEOUT_S`; `run` refuses to start when that is below grace + 15 (exit 2; exit 0 with a `permanent_failure` log line under launchd, so KeepAlive does not relaunch it).
  A second signal during the startup engine probe kills the probe worker and exits 0.
- **Backoff**: `min(30 s, 10 s × 2^min(idle, 16)) × U(0.5, 1.0)`, so the actual delay is 5–30 s,
  not a fixed 10–30 s band. Resets on the first assignment.
- **Worker** (`worker/process.py`): `multiprocessing` `spawn` context, framed JSON over a `Pipe`. The
  worker is kept warm across subtasks of the same model, killed and restarted on a model switch,
  and killed on stop. Calls are serialised by a lock; `kill()` is lock-free, marks the handle
  stopped and never respawns. The child gets an allowlisted environment (`PATH`, `HOME`, `TMPDIR`, `LANG`, `LC_*`, `HF_HOME`, `MLX_*`,
  `TOKENIZERS_PARALLELISM`, `MINION_EXECUTOR_DEV`/`_TEST_HOOKS`, `__PYVENV_LAUNCHER__`); because `spawn` hands the parent's
  environ to the child before `worker_main` runs, `main` calls `restrict_environ()` after the httpx
  clients are built (they read proxy env at construction) and before any spawn, so the control
  process itself holds no `HF_TOKEN`/`AWS_*`/`DATABASE_URL`,
  bounded IPC messages (`max_message_bytes`, oversize = crash) and exits by itself if the control
  process dies (parent-pid watch). Runners: `mlx` (completion and embedding), `ollama` (opt-in, loopback only), `hf` (mps)
  (all TASK-4.3) and the test-only `fake` (`runners/fake.py`, never advertised as a real engine). The fake's `fail` / `sleep_s` / `echo_env` per-record hooks
  work only when constructed with `test_hooks=True` and use fixed codes. Inference never runs in the
  control process (invariant 7).
- **Profiles** (`capacity/profiles.py`, `capacity/config.py`, FR-51): `light` / `balanced` / `full`
  plus `custom` (defined in `executor.toml`). Memory is `min(fraction, 75 %) × RAM` (also for
  `custom`), sent in `Budget`. `Budget.cpu_priority` (`background|low|normal`) is on the wire and
  the worker/fetch `nice` is derived from it (15 / 10 / 0; unknown counts as `low`).
  `executor.toml` (`~/.config/minion/executor.toml` or `$MINION_EXECUTOR_CONFIG`; an empty value counts as unset) must be owned by
  the user and not group/world-writable; unknown keys are errors. `run` without `--profile` is
  adaptive: it starts on the file's `active_profile` (default `light`; the bare run used to be
  `balanced`). `custom` is accepted when `[profiles.custom]` is defined.
- **Token and CLI** (`secrets.py`, `main.py`): token from `$MINION_EXECUTOR_TOKEN`, else the macOS
  Keychain item `open-minions-executor` via `/usr/bin/security` (run with a scrubbed environment).
  Token is never logged. `run` exits 2
  on an insecure server URL (`require_secure_url`, localhost excepted), an unknown profile, or a
  missing token or unknown runner. The token is removed from `os.environ` right after loading, before
  any child process. `--engines auto` (default; probes mlx and hf, Ollama is opt-in) or a list such as `mlx,ollama`: after the environ is
  restricted, a short-lived worker subprocess (90 s timeout) probes which engines work (only engines
  `create_runner` implements, see `runners.SUPPORTED`) and the executor registers with those; none
  available or a probe failure exits 2. `fake` is never implicit: `--engines fake` (or hidden
  `--runner fake`) needs `MINION_EXECUTOR_DEV=1`. Subtasks whose engine/memory is not admissible, or
  whose engine turns out unavailable at load (`ENGINE_UNAVAILABLE`), are released, not failed.
  `--profile` accepts `light|balanced|full|custom`.
- **Idle unload** (`loop.py`, FR-13, invariant 6): after `MINION_EXECUTOR_IDLE_UNLOAD_S` (600 s; 0
  disables) without a subtask, the loop calls `worker.unload()` to free model memory; the same check
  runs on the acquire-error path.
- **Heartbeater** (`heartbeat.py`, TASK-3.3, FR-14): an asyncio task beside the work coroutine.
  Interval `--heartbeat-interval` / `MINION_EXECUTOR_HEARTBEAT_INTERVAL_S` (default 300 s, min 1 s),
  each sleep jittered ×U(0.9, 1.1). Each beat sends progress, budget and pause reason, and keeps the
  latest `output_post` from the response (the upload uses the refreshed POST; accepted only if it names the same URL and `key` field).
  `429` waits `Retry-After` (capped at 600 s) and is non-fatal; `5xx` and transport errors retry with
  a jittered delay (≤30 s); other errors are logged and the beat continues.
  `cancel=true` returns `CANCEL` → interrupt worker, `release` (server records `CANCELLED`).
  `409` (`TASK_FINISHED`, stale acquirement) returns `STALE` → interrupt worker, report nothing.
  **Ack beat** (ADR-0014): right after a successful acquire, before the worker starts, one heartbeat
  is sent at once. `429` / `5xx` / transport errors on it are non-fatal; `409` drops the assignment
  without running it.
  Logs carry ids only. Running with `--heartbeat-interval` such that `MINION_HEARTBEAT_TIMEOUT_S` < 2× the interval is
  rejected only when the operator exports `MINION_HEARTBEAT_TIMEOUT_S`; keep the timeout ≥ 2–3×
  the interval (3× recommended) so one lost beat does not get a live executor reclaimed.
- **Housekeeping**: temp dirs are `minion-exec-<pid>-*`; at startup dirs of dead pids owned by the
  user are removed. File I/O in the data path runs in threads (`asyncio.to_thread`).
- **Logging**: `structlog`, ids only (acquirement, team/project/task, subtask id, attempt); no payloads.

### 2.3 minion-watchdog
- Separate process (`minion_watchdog.main:main`, `uv run minion-watchdog`) that reuses the server's
  repositories and services; it never talks to executors. Only `MINION_DATABASE_URL` is required;
  its DB pool is `MINION_WATCHDOG_DB_POOL_SIZE` (default 2; the server uses `MINION_DB_POOL_SIZE`, 10) and each statement has
  `MINION_DB_COMMAND_TIMEOUT_S`.
- Loop every `MINION_WATCHDOG_INTERVAL_S` (60 s). A tick reclaims in batches of
  `MINION_WATCHDOG_BATCH_SIZE` (repeats while batches come back full, max 100 rounds), then
  `settle_stuck` (finishes live tasks whose counters cover every subtask, ADR-0015; repeats while its
  batch comes back full, round-capped), then the cancel sweep. Settle and sweep each have their own
  error handling: a failure is logged and stalls neither reclaim nor the other step. DB errors back off exponentially,
  capped at `MINION_WATCHDOG_MAX_BACKOFF_S` (300 s). SIGTERM abandons the tick; each step is its own
  transaction and rolls back.
- Replicas are safe: candidates are locked with `SKIP LOCKED` and re-checked (§3.4, ADR-0008).
- Scans heartbeat rows (executors *currently running* work) **[paper]**, plus acquirements older than
  `MINION_MAX_LEASE_S` **[ours]**.
- Liveness for container healthchecks: `MINION_WATCHDOG_HEALTH_FILE` (default unset = no file). The file is created (0600) at start and touched after each successful tick; a failed tick does not touch it. A write error is logged (class only) and never stops the loop.

### 2.4 minion-common
Pydantic request/response models, enums (`TaskType`, `SubtaskStatus`, ...), typed `httpx` API client
used by the executor and by tests, shared settings base.

### 2.5 minion-cli (submitter)
- Console script `minion` (`minion_cli.main:app`, typer) in `packages/minion-cli`; subcommands
  `submit`, `status`, `fetch`, `cancel`. Kept out of minion-common so boto3 and submitter S3 credentials
  stay out of the executor's dependency closure (S-2, S-14).
- Uses the typed `UserClient` from minion-common with the user API key (`MINION_API_KEY` or Keychain
  item `open-minions-api-key`). Settings are `MINION_CLI_*` env vars (no env file).
- `submit` uploads the JSONL files to `inputs/{team}/{project}/{name}/` via boto3 (path-style, S3 API),
  creates the task with `output_uri` under `outputs/{team}/{project}/{name}/`, polls progress and downloads
  the `SUCCEEDED` parts to a local directory (`results.jsonl`, `manifest.json`). The server never proxies
  payloads (invariant 4). Output is never printed or logged.
- Exit codes: 0 ok, 1 FAILED/CANCELLED, 2 usage/config/auth/4xx, 3 PARTIAL, 4 timeout or server/S3
  unreachable, 130 Ctrl-C. Ctrl-C never cancels the task; only `minion cancel` does.

### 2.6 minion-scale (dev-only scale harness)
- `packages/minion-scale`, console script `minion-scale` (TASK-7.2); never part of the server image
  (only its `pyproject.toml` is copied for the workspace sync). Operator tooling: it may talk SQL
  directly (bulk seed, verifier, EXPLAIN), but the simulated executors (`sim.py`) use the executor REST
  API and presigned URLs only (invariants 1 and 4) and import no database code.
- `seed` inserts N tasks x M PENDING subtasks with `INSERT ... SELECT generate_series` and issues
  executor tokens into the dedicated pool `scale` (tasks get `allowed_pool = 'scale'`, so no real
  executor can acquire them); `run` drives the simulated executors with fault injection and fails
  fast (exit 1) when the queue does not drain; `verify` checks NF-2/NF-3 in one read-only snapshot;
  `cleanup` revokes the issued tokens. Settings come from `MINION_*` environment variables only, and
  writes refuse a non-loopback database or one holding tasks it did not create unless
  `--yes-this-is-a-scale-db`.
- `deploy/compose.scale.yml` is an overlay with its own project name (`open-minions-scale`) and
  loopback-only ports. Report schema: `docs/bench/scale-report-schema.md`.

## 3. Key flows

### 3.1 Task submission
The `minion submit` CLI (§2.5) performs the same steps from a workstation: it uploads the input JSONL
under `inputs/{team}/…` with its own S3 credentials (S-14), then calls `POST /api/v1/tasks` with the
`output_uri` it derived. Everything below applies unchanged to CLI-created tasks.
```
User ─POST /tasks─▶ Server: validate (team membership, model allow-list + type, team-scoped S3 URIs
                    `{prefix}{team}/…`, input prefix non-empty, no `_attempts` segment in output;
                    at least one object under the input prefix, else `422 EMPTY_INPUT`)
                    INSERT minion_task(status=SPLITTING)  ──▶ 202
Splitter (async):   claim one due SPLITTING task with a lease (split_attempts+1, next_split_at=now+lease)
                    list input objects → scan JSONL → stream batches of slices of N records
                    per batch: renew the lease, compare with existing rows (key, byte range;
                    mismatch → INPUT_CHANGED), GET pinned by listed ETag + size (change → INPUT_CHANGED),
                    INSERT minion_subtask(status=PENDING, attempt=0) ON CONFLICT DO NOTHING
                    UPDATE minion_task SET status=PENDING, total_subtasks=k
```
Splitting is bounded (line length, total bytes, objects, subtasks per task; see `MINION_SPLIT_*`)
and no transaction is held open while reading S3. Deterministic problems (bad URI, empty input,
limit exceeded, `LINE_TOO_LONG`, `INPUT_CHANGED`, S3 `AccessDenied`/`NoSuchKey`/`InvalidObjectState`)
mark the task `FAILED` with `error`. Anything else backs off exponentially via `next_split_at` and
fails the task after `split_max_attempts`, so one bad task never blocks the queue. `output_uri` is
unique per task (`409 OUTPUT_URI_IN_USE`): attempt and promoted keys are derived from it. Stored
URIs are canonical (`s3://{bucket}/{key}/`); a non-lowercase scheme is `422 INVALID_URI`.

The claim is a lease fenced by `split_attempts`: lease renewal (once per batch), done, failed and
retry all match on the claimed value, so a splitter whose lease expired and was re-claimed becomes
a no-op and aborts quietly. Each attempt has a wall-clock budget (`MINION_SPLIT_MAX_SECONDS`,
default 3600; exceeding it backs off like any transient error). A claim never hands out a task with
`split_attempts >= max_attempts`: it is marked `FAILED` with `SPLIT_ATTEMPTS_EXHAUSTED` (crash loop).

### 3.2 Acquire (the core protocol)
```
Executor ─POST /executor/acquire {engines, budget}─▶ Server
  (executor only calls this when governor state ≠ PAUSED)
  for pass in [preference, none]:            -- pass 2 (global FIFO) only if there is a preference;
                                             -- engine, memory and allowed_pool filters stay in both
    repeat up to MAX_ROUNDS=10 times:        -- one transaction per round (ADR-0013, ADR-0019)
      SELECT ... FROM minion_executor WHERE executor_id = me FOR NO KEY UPDATE
                                             -- missing / REVOKED -> 401; serialises this executor's rounds
      SELECT count(*) FROM minion_subtask_acquirement WHERE executor_id = me   -- separate statement
      count >= MINION_MAX_OPEN_ACQUIREMENTS -> 429 TOO_MANY_ACQUIREMENTS, Retry-After 30 (no candidate read)
      WITH cand AS (SELECT pending subtasks of live tasks
                    WHERE task.engine = ANY(engines)
                      AND task.model_memory_gb <= budget.memory_gb   -- capacity admission
                      AND (matches pass preference)
                    ORDER BY task.priority DESC, task.created_at, team, project, task_name, subtask_id
                    LIMIT window),            -- MINION_ACQUIRE_WINDOW, default 16
           pick AS (SELECT * FROM cand WHERE priority = max(priority) ORDER BY random() LIMIT 1),
           ins  AS (INSERT INTO minion_subtask_acquirement (...)
                      SELECT ... FROM pick JOIN subtask JOIN task
                      WHERE subtask.status = PENDING AND task.status IN (PENDING, RUNNING)
                    ON CONFLICT (team, project, task_name, subtask_id) DO NOTHING
                    RETURNING acquirement_id, attempt, key)
      SELECT count(cand), ins.*, pick.* ...
      count = 0:  nothing in this pass -> next pass
      no row:     lost the insert race   -> ROLLBACK, next round (fresh window)
      row:        UPDATE minion_subtask SET status=RUNNING, attempt=attempt+1 ... (same txn, re-check)
                  INSERT minion_executor_heartbeat(...); UPDATE task PENDING -> RUNNING (same txn)
                  re-check failed -> ROLLBACK, next round; else COMMIT -> return assignment + presigned URLs
  return 204   (pass 2 starts when pass 1 finds nothing or exhausts MAX_ROUNDS; exhausting every
                round with work present is logged as `acquire_rounds_exhausted`)
```
Notes:
- The unique constraint is the *only* mutual-exclusion mechanism **[paper]**. The candidate SELECT is
  deliberately unlocked; losing an insert race is cheap.
- Candidate query is index-driven in two steps (TASK-7.4.2; window = MINION_ACQUIRE_WINDOW, default 16), O(window + live tasks) regardless of queue
  length: eligible live tasks are walked in queue order on the partial index
  `ix_minion_task_live_queue (priority DESC, created_at, team, project, task_name) WHERE status IN
  ('PENDING','RUNNING')`, and for each a `LATERAL ... ORDER BY subtask_id LIMIT window` range scan runs on
  `ix_minion_subtask_pending (team, project, task_name, subtask_id) WHERE status = 'PENDING'`; the
  outer `ORDER BY` (same keys + `subtask_id`) `LIMIT window` yields exactly the FR-22 order of the old
  single join (the task key is unique, so the order is total and the global top window holds at most window
  rows per task, each within that task's top window). Status values are SQL literals, not bind
  parameters, so asyncpg's generic plans still match the partial-index predicates. Residual cost:
  ~1 us / ~3 buffers per eligible live task that has no PENDING subtasks ahead of the first match
  (measured at 100 / 1000 / 5000 such tasks: 0.20 / 1.33 / 5.2 ms).
- Alternative considered: `SELECT ... FOR UPDATE SKIP LOCKED`. Rejected for v1 to stay faithful to the
  paper and keep the ownership record explicit; revisit if NF-1 isn't met (ADR-0013).

- Overload (TASK-7.4.4): the acquire handler first tries a per-process slot (`MINION_ACQUIRE_CONCURRENCY`,
  non-blocking; full = `503 SERVER_BUSY`, no candidate work), then each claim round obtains its connection and
  checks a monotonic deadline (`MINION_ACQUIRE_DEADLINE_S`, started at request arrival by a small ASGI
  middleware) *before* the claim statement: past it = `503 ACQUIRE_DEADLINE`, rolled back, nothing committed (no
  ghost acquirement for a client that timed out). The claim transaction also runs under
  `SET LOCAL statement_timeout` = the remaining deadline (+100 ms, min 100 ms); a cancelled statement (SQLSTATE
  57014) maps to the same 503. At most 2 acquires per executor run at once (429, `Retry-After: 1`), so a client
  ignoring the pace limit cannot fill the slots. `MINION_DB_POOL_TIMEOUT_S` + `MINION_ACQUIRE_DEADLINE_S` must
  stay <= 25 s (validated). A pool wait over `MINION_DB_POOL_TIMEOUT_S` surfaces as SQLAlchemy `TimeoutError`,
  mapped app-wide to `503 SERVER_BUSY` + `Retry-After` (2-10 s; a full slot sheds with 1-3 s). At least 4 connections are kept back from acquires (default slots), but slots only bound acquires, so presence/register/UI/auth lookups share those
  connections with owner operations (heartbeat, complete, release). Executor authentication (ADR-0018) does not use those
  connections for unknown tokens: a per-process cache answers known tokens (TTL `MINION_EXECUTOR_AUTH_CACHE_TTL_S`, expiry
  checked on every hit), a known-but-stale token revalidates on the main pool, and a token never seen is looked up on a
  separate pool (`MINION_DB_AUTH_POOL_SIZE`, no overflow, `MINION_AUTH_LOOKUP_TIMEOUT_S` then `503 SERVER_BUSY`), so a
  bearer flood cannot hold a main-pool connection. Revocation reaches a cached token within the TTL for complete/fail/release; acquire, register, presence and heartbeat match the live row and token hash in their UPDATE and refuse a revoked or rotated token at once. At startup the cache is preloaded with the live executors as stale entries, so after a restart known executors revalidate on the main pool, not the auth lane.

### 3.3 Execute & report
```
Executor: GET input via presigned URL (Range header for byte slice)
          ensure model cached → hand records to worker subprocess
Worker:   load model (or reuse warm one) with MLX memory/cache limits from budget (hf: torch.mps fraction)
          loop: run one generation step / embedding batch (≤ budget.max_batch_size)
                ctl.checkpoint()  ← duty-cycle sleep + check governor events
          stream results back to control process
Executor: write results to local temp JSONL
          POST to presigned URL  {output_prefix}/_attempts/{subtask_id:06d}/{attempt}.jsonl
          POST /complete {record_count, sha256}
Server:   verify acquirement is current (else 409)
          S3 CopyObject → {output_prefix}/{subtask_id:06d}.jsonl   (promotion = idempotency;
                          CopySourceIfMatch on the ETag verified at HEAD, mismatch → 422 OUTPUT_CHANGED)
          txn: DELETE acquirement, DELETE heartbeat row,
               UPDATE subtask SUCCEEDED, INSERT minion_subtask_attempt,
               UPSERT counter shard (minion_task_counter, subtask_id % 16)
          COMMIT, then a second short txn "settle": read status, total, shard sums;
               if every subtask is terminal → task status (SUCCEEDED | PARTIAL | FAILED)
               and finished_at, only if the task is still PENDING/RUNNING (ADR-0015)
```
Input GET URLs sign the subtask's exact `Range` header (`bytes=byte_start-(byte_end-1)`), so a URL
reads only its slice. Before promotion the server HEADs the attempt object: above
`MINION_MAX_OUTPUT_BYTES` (4 GiB) → `422 OUTPUT_TOO_LARGE`, no copy, no download; the executor
best-effort deletes the rejected attempt object (errors logged without the key). The assignment
carries the server cap as `max_output_bytes`; the executor checks `min(local, server)` on its
local file before uploading (`MINION_EXECUTOR_MAX_OUTPUT_BYTES`; both settings are `<= 5 GiB`) and
fails `OUTPUT_TOO_LARGE` (not retryable). A `422 OUTPUT_TOO_LARGE` on complete is reported the same
way (fail, non-retryable: a re-run yields the same output). A size equal to the cap is accepted on
both sides. The output is a presigned POST whose signed policy has `content-length-range 0..MINION_MAX_OUTPUT_BYTES`, so S3 itself
refuses a larger body (ADR-0017); the HEAD above is defence in depth. Attempts times the cap can still
accumulate, so operators should: keep bucket versioning off (or expire noncurrent versions, since a
delete otherwise keeps the bytes), set a bucket or tenant quota (SeaweedFS `s3.bucket.quota`, AWS
budget/size alarms), and apply the lifecycle rule below to `_attempts/`.

**Lifecycle rule (operators).** Attempt objects are kept after promotion and unpromoted ones are
never read again, so expire them (e.g. 7 days). S3 lifecycle filters match key prefixes only and
`_attempts/` sits below the per-task prefix, so add one rule per task output prefix
(`outputs/<team>/<project>/<task>/_attempts/`), or run a periodic cleanup job over `*/_attempts/*`.
Never expire the promoted `NNNNNN.jsonl` outputs. Not automated in v1.

### 3.4 Heartbeat & reclaim
```
Executor (every heartbeat interval, 300 s by default, while running): POST /heartbeat {progress, budget, paused_reason}
  Server: owner check under lock (acquirement, else 409); 429 if closer than MINION_HEARTBEAT_MIN_INTERVAL_S
          (the first beat after a claim, the ack, is exempt: ADR-0014)
          UPDATE minion_executor_heartbeat.last_seen_at = now(); report executor row (after the acquirement lock)
          respond {cancel: task.status == CANCELLED, output_post (fresh presigned POST), expires_at}

Watchdog (every 60 s; also reclaims acquirements older than MINION_MAX_LEASE_S regardless of beats,
then sweeps PENDING subtasks of recently cancelled tasks to CANCELLED). Implemented as
SKIP LOCKED + re-check (ADR-0008), not DELETE ... RETURNING:
  candidates = unlocked scan: heartbeat rows WHERE last_seen_at < now() - heartbeat_timeout
               ∪ acquirements WHERE acquired_at < now() - max_lease
  FOR each candidate:
    BEGIN
      SELECT ... FROM minion_subtask_acquirement WHERE acquirement_id = $1 FOR UPDATE SKIP LOCKED
      -- no row (gone) or locked by an owner op / other replica → SKIPPED
      re-check: heartbeat still stale? else max lease exceeded? else → NOT_STALE
      -- stale heartbeat with last_seen_at = acquired_at (never acknowledged) and fewer than
      -- MINION_RELEASE_LIMIT earlier NOT_ACKNOWLEDGED expiries: code NOT_ACKNOWLEDGED, not budget-counted
      finish_failed: INSERT minion_subtask_attempt(outcome=EXPIRED, code NOT_ACKNOWLEDGED | HEARTBEAT_TIMEOUT | MAX_LEASE_EXCEEDED)
                     DELETE acquirement (cascades heartbeat row)
                     UPDATE minion_subtask: FAILED if budget spent, CANCELLED if task cancelled, else PENDING
      UPSERT counter shard when the subtask ended FAILED
    COMMIT   (a task whose last subtask failed here is settled by the watchdog's settle_stuck)
```
**Retry budget.** It is the number of `FAILED` + `EXPIRED` attempts in `minion_subtask_attempt`
(`fail` already counts the attempt being recorded). `EXPIRED` with code `NOT_ACKNOWLEDGED` (never-beaten
acquirement, ADR-0014) is not counted, up to `MINION_RELEASE_LIMIT` per subtask; past the cap it counts.
A voluntary `release` does not consume it, up to
`MINION_RELEASE_LIMIT` releases per subtask (then recorded as a retryable `FAILED`).
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

**Runtime (M4.5).** `capacity/runtime.py` `GovernorRuntime` is an asyncio task beside the control loop.
Each poll (`MINION_EXECUTOR_SIGNAL_POLL_S`, 2 s) it reads the signals (`read_with_timeout`, one source
object per run), `control.json` and runs the pure `decide`; the loop reads `decision` (budget,
`paused_reason`, `release_now`, `pause_expired`, `trim`) and wakes on changes. The loop uses
`decision.budget` everywhere (acquire, admission, heartbeat, presence, per-batch `run`). While paused it
only sends presence (on entering the pause, on a reason/budget change, every 60 s). A watcher task
beside each subtask (next to the heartbeater) pushes duty/pause/trim to the worker, releases the
subtask on `pause_expired` / `release_now`, bounds a downgrade to 60 s, and runs the memory and CPU
priority guards (D7/D8). `status.json` mirrors the state for `minion-executor status`. After the
batches are done the watcher never ends the subtask (it only frees an idle worker); a stale
or errored governor reads as paused `signals_unavailable` and, lasting 60 s, releases the in-flight
subtask; a dead governor task stops the executor (exit 1). One executor per state dir (`flock`).

**Governor states:** `ACTIVE(profile)` ⇄ `IDLE(profile)` → `PAUSED(reason)` → back. Transitions:
- user input within `idle_after_min` → `active_profile`; idle ≥ threshold and on AC → `idle_profile`;
- schedule window overrides both; `pause` CLI / pause conditions (SPEC FR-53) → `PAUSED`;
- hysteresis: escalate (more capacity) only after the condition holds 60 s; de-escalate immediately.

**Levers:**
1. **Memory admission** — server only offers subtasks whose `model_memory_gb ≤ budget.memory_gb`.
   Inside the worker: `mx.set_memory_limit(budget)` and a small `mx.set_cache_limit` so freed buffers are
   returned to the OS instead of hoarded. Downgrade below the loaded model → finish batch → `release`
   subtask → kill worker (only reliable way to return all unified memory).
2. **GPU duty cycle** — `StepControl.checkpoint()` (→ `throttle.step_done()`) after each step: measure GPU-synchronized step time `t`
   (`mx.eval` + `mx.synchronize`), then sleep `t·(1−d)/d` for duty `d`. Short bursts (one decode step
   ≈ 10–50 ms) keep the window server and video encoders getting GPU time between bursts. Batch size is
   the second knob: smaller batch → shorter bursts → smoother foreground. The duty and a pause can change
   while a batch runs: `WorkerHandle.control()` sends a latest-wins `ctl` message over the worker pipe; the
   worker reads it at the next checkpoint (or mid pacer-sleep), acks it, and while paused blocks in the
   checkpoint, still ticking (ADR-0006 D3; implemented in M4.3). The paced duty is
   `min(control duty, budget duty)`: control can only be stricter than the budget.
3. **Batch size** — capped by `budget.max_batch_size`; also bounds KV-cache memory.
4. **CPU priority** — set by the worker on itself (`capacity/priority.py`): Darwin background QoS
   (`setpriority(PRIO_DARWIN_PROCESS, …, PRIO_DARWIN_BG)`, the `taskpolicy -b` equivalent; E-cores,
   throttled I/O) for `background`, `nice` 10 / 15 for `low` / `background`; `OMP_NUM_THREADS=2` when
   throttled (`low` / `background`; niceness and threads at spawn follow the latest budget the handle has
   seen). Raising niceness and toggling background QoS work live, also inside the sandbox; lowering
   nice needs a respawn. The control side verifies it (`WorkerHandle.priority_ok()`) and samples the worker's memory footprint
   (`worker/footprint.py`: `proc_pid_rusage` on macOS, `/proc` on Linux).

**Controller-side GPU guard (TASK-15, ADR-0006 D17).** The duty above is self-paced by the worker, so the control process does not trust it. One sampler per worker pid (`loop.py`, sample times jittered ±25 %) reads that pid's `accumulatedGPUTime` through `capacity/gpu_meter.py` (in-process IOKit, about 1 ms per read) and feeds `capacity/gpu_guard.py` (a pure state machine: 10 s window, limit = allowed duty + 10 pp, KILL when a leaky over-budget accumulator reaches 30 s, at once when paused (after the worker's ack) or idle, KILL above 4 GPU clients per pid). A KILL interrupts the worker and releases the subtask with `GPU_DUTY_VIOLATION`; repeated meter errors or a stalled sampler release with `GPU_METER_UNAVAILABLE`. This is a backstop, not a throttle: SIGSTOP gating is TASK-15.1.

**Optional adaptive mode (Q5):** read total GPU utilization from IORegistry
(`AGXAccelerator` → `PerformanceStatistics` → `Device Utilization %`, no sudo) and lower duty cycle
when utilization from *other* processes rises (e.g. a video call). Off by default until benchmarked.

**Not done on purpose:** no `caffeinate`, no sleep prevention, no kernel extensions, no root.

**Tests:** governor is pure (signals in → events out) and unit-tested with fake signal sources;
throttle tested with a fake clock; budget adherence (NF-8) measured by `minion-executor bench --budget-check` (`bench_budget.py`: per-process GPU time of the worker from `ioreg -c AGXDeviceUserClient` via `capacity/gpu_meter.py`, device utilization informational, worker footprint, pacer share; verdicts per ADR-0006 Gap 1); reaction time (NF-9) by `tests/test_governor_reaction.py` (CI) and `bench --watch-reactions` (Mac); idle/paused CPU (NF-4) by `bench --cpu-check`.

## 4. Concurrency & correctness argument

| Hazard | Guard |
|---|---|
| Two executors acquire same subtask | UNIQUE constraint on acquirement; only one INSERT returns a row |
| Watchdog reclaims while owner completes | Both paths lock the same acquirement row (`FOR UPDATE`; reclaim uses `SKIP LOCKED`, so a busy row is skipped); reclaim re-checks staleness under the lock; loser sees 0 rows → no-op / 409 |
| Reclaimed owner uploads late | Writes go to attempt-scoped key; promotion only on accepted `/complete` |
| Two watchdog replicas | `SKIP LOCKED` per row + re-check under the lock; a skipped row is handled on the next tick |
| Cancel of a large task | Lazy two-phase: txn 1 task only; txn 2 in batches of 10 000 PENDING rows; RUNNING owners learn via `heartbeat.cancel`; watchdog sweep heals stragglers |
| Live executor reclaimed between beats | `MINION_HEARTBEAT_TIMEOUT_S` ≥ 2–3× heartbeat interval (checked only when the timeout is exported to the executor); 429 keeps beats bounded |
| Server crash mid-split | Splitter is resumable: subtasks inserted with deterministic ids + `ON CONFLICT DO NOTHING` |
| Budget downgrade mid-subtask | Executor `release`s (same txn path as complete/reclaim); output of partial run discarded (attempt-scoped key) |
| Rogue executor hoards subtasks (never acks) | Per-executor cap on open acquirements under an executor-row lock (ADR-0019): at most `MINION_MAX_OPEN_ACQUIREMENTS` (4) per token across all workers; revocation also stops acquire at once. Ownership of a subtask is still decided only by the unique-constraint INSERT |
| Worker crash (Metal / OOM) | Control process survives, reports `fail(retryable)`; attempt counter bounds retries |

**Lock order (all owner ops and reclaim):** acquirement → heartbeat row → subtask → counter shard
(`minion_task_counter`, one of 16 rows per task; the task row is not row-locked for update by completions; the first insert per shard takes an FK KEY SHARE on it). The
executor row (`minion_executor`) is written after the acquirement lock, never before it. The acquire claim round takes the
executor row first (`FOR NO KEY UPDATE`, ADR-0019), then the new acquirement, subtask, heartbeat and task rows. No
cycle: the claim never waits on a lock an owner op holds (it inserts a new acquirement row, and `ON CONFLICT DO NOTHING`
waits only on an in-progress insert/delete of the same key by another claim, which holds a different executor row, or by
complete/release/reclaim, which **never write `minion_executor`**; keep it that way). `FOR NO KEY UPDATE` does not conflict with
the FK `KEY SHARE` locks of attempt/acquirement inserts, so the watchdog is never blocked. Heartbeats and `touch` of the same
executor wait for its claim transaction (milliseconds); the lock wait is bounded by the acquire deadline (`statement_timeout`).
Cancel txn 1
locks the task only; txn 2 locks PENDING subtask rows (`SKIP LOCKED`, bounded) and never a running
acquirement, so it cannot deadlock with owner ops.

Required tests: concurrent-acquire race (≥50 parallel acquires, assert unique owners), complete-vs-reclaim
race, stale-acquirement rejection, splitter restart.

## 5. Data layer details

### 5.1 Key indexes
- `minion_subtask`: partial index on `status='PENDING'`; index on `(team, project, task_name, status)` for progress.
- `minion_executor_heartbeat`: index on `last_seen_at`.
- `minion_subtask_acquirement`: `ix_minion_subtask_acquirement_acquired_at` on `acquired_at` serves the watchdog max-lease scan (migration 0008); `ix_minion_subtask_acquirement_executor_id` on `executor_id` serves the per-executor open-acquirement count of the acquire path and the fleet join (migration 0012).
- `minion_task`: index on `(status, priority DESC, created_at)`.
- `minion_task`: `ix_minion_task_team_created` on `(team, created_at DESC, project DESC, task_name DESC)` serves `GET /tasks` keyset pages; the multi-team listing runs one capped (`limit + 1`) index-ordered branch per team (UNION ALL) and merge-sorts, so cost is O(teams x limit).

- `minion_task_counter` (ADR-0015, migration 0011): PK `(team, project, task_name, shard)`, `succeeded` / `failed` per shard, `shard = subtask_id % 16`; the task counters are the sum over shards (`minion_ro.task` still exposes `succeeded_subtasks` / `failed_subtasks`). A completion upserts one shard row instead of updating the task row; the terminal task status is set by a post-commit settle (and healed by the watchdog), so `minion_task.updated_at` no longer moves per completion.

### 5.2 Input slicing **[ours]**
Splitter records `(object_key, byte_start, byte_end, first_record, record_count)` per subtask, aligned to
newline boundaries. Executors fetch with HTTP `Range` on a presigned GET. Avoids rewriting input data.

## 6. Deployment

Local dev uses SeaweedFS instead of MinIO (MinIO community images are no longer pullable).

| Env | Topology |
|---|---|
| Local dev | `deploy/compose.yml`: postgres, s3 (SeaweedFS), grafana; with `--profile app` also migrate, server, watchdog (one image, `deploy/Dockerfile`); executor runs natively on the Mac (needs Metal — not in Docker); `-f deploy/compose.dev.yml` adds the fake models for the quick start (dev only) |
| Prod | Managed Postgres (separate dev/prod DBs **[paper]**), S3, ≥2 server replicas, 1–2 watchdogs, Grafana. One deployment per team/org for isolation **[paper]** |

Container hardening (`deploy/Dockerfile`, `x-minion-app` in `compose.yml`): one image for migrate, server and watchdog, built from the lockfile with `uv sync --locked --no-dev`; base images pinned by digest; the executor and pyobjc are not installed. Runs as uid/gid 10001 (non-root), `read_only` rootfs with `tmpfs` `/tmp`, `cap_drop: [ALL]`, `no-new-privileges`, setuid/setgid bits stripped, `/tmp` tmpfs capped at 64 MB. `server` and `watchdog` use `restart: unless-stopped` (`migrate` is one-shot). Third-party images in `compose.yml` are pinned by digest. No `.env` or secrets in any layer (`.dockerignore` is an allow-list). The watchdog liveness file lives in the `/tmp` tmpfs (`MINION_WATCHDOG_HEALTH_FILE`, touched at start and after each successful tick, so it goes stale while DB errors back off).

Startup order with `docker compose -f deploy/compose.yml --profile app up -d --build`: postgres (healthy via `pg_isready -h 127.0.0.1`, TCP because the socket-only initdb server would report healthy too early) → `migrate` (one-shot `alembic upgrade head`, replaces the manual `uv run alembic upgrade head` in container mode) and `s3-init` → `server` (healthy on `/healthz`) and `watchdog` (healthy while its health file is newer than 3 × interval + 30 s). Only the `app` profile starts these; without it the infra-only flow runs the server natively.

Ports: the server is published on `127.0.0.1:${MINION_SERVER_HOST_PORT:-8080}` only. Secrets: `${MINION_SECRETS_DIR:-./secrets}` (default `deploy/secrets/`, git-ignored) is mounted read-only at `/run/minion-secrets` and holds `api-keys.yaml`; the directory is mounted, not the file, so the atomic temp+rename reload reaches the container. `MINION_UI_SESSION_SECRET` and `MINION_METRICS_TOKEN` come from the environment (compose reads the shell or `deploy/.env`), never from the image. Ownership: `auth.py` refuses a keys file or directory not owned by root or the server uid. On macOS Docker Desktop the bind mount shows the file as owned by the container uid, so it works as is. On Linux hosts, either `chown -R 10001:10001` the secrets directory and file, or leave them `root:root` (file 0644 in a 0755 directory); the keys file must not be group/world-writable. `api-keys.yaml` must exist before `up`, otherwise Docker creates an empty directory at the mount source. The watchdog healthcheck reads `MINION_WATCHDOG_INTERVAL_S`, which compose passes to the watchdog too (default 60) so both use one value.

Server sizing (TASK-7.4.6): `MINION_SERVER_WORKERS` (1..32, default 1) runs that many uvicorn worker processes (`minion_server.app:create_app` factory); `1` keeps the in-process single-app path. Every worker is a full replica: its own SQLAlchemy pool (`MINION_DB_POOL_SIZE` + `MINION_DB_MAX_OVERFLOW`), its own acquire semaphore (`MINION_ACQUIRE_CONCURRENCY`, default pool + overflow - 4, per process, not divided by workers), its own splitter loop (safe: claims are lease-based, `test_concurrent_splitters_no_duplicates_full_coverage`) and its own in-memory rate limiter and per-executor in-flight cap (ADR-0012). Connection budget = workers x (pool + overflow + `MINION_DB_AUTH_POOL_SIZE`, 2) + the watchdog pool (`MINION_WATCHDOG_DB_POOL_SIZE`, 2) and must stay below Postgres `max_connections` (default 100) minus other clients; `main()` logs `db_connection_budget_exceeded` at startup when the budget is >= `MINION_DB_MAX_CONNECTIONS` (default 100; set it to the real limit) minus `MINION_DB_RESERVED_CONNECTIONS` (default 7: superuser_reserved 3 + Grafana 4). `MINION_WATCHDOG_DB_POOL_SIZE` (>= 1) must be set on the server service as well as the watchdog's. Also give the application DB role a `CONNECTION LIMIT` so a runaway server cannot starve the watchdog or an admin. Rule of thumb: size workers x pool to the concurrent queries the database can serve (a few per core), not to the executor count; shed load (503 + Retry-After) beats a deeper queue. With N workers one executor can get up to N x the per-executor acquire budget and N x the rate limit; enforce per-executor limits at the proxy or route by executor with sticky routing if that matters. Metrics: with workers > 1 and `MINION_METRICS_ENABLED`, the parent creates a fresh private `mkdtemp` directory (0700, under `/tmp`, removed when the server exits) and sets `PROMETHEUS_MULTIPROC_DIR`; an operator-set `MINION_METRICS_MULTIPROC_DIR` (or an inherited `PROMETHEUS_MULTIPROC_DIR`, also with one worker) must sit under parent directories owned by you or root and not writable by others unless sticky, and is used only if it is a real directory (not a symlink) owned by the server user with mode 0700, else the server exits 2, and at startup only its `counter_*`/`histogram_*`/`gauge_*` `.db` files are deleted (via a no-follow directory fd); each worker calls `mark_process_dead` on shutdown (drops live-gauge files only; counter and histogram files stay so totals survive); `/metrics` then sums all workers (counters and histograms only; a future Gauge needs a `multiprocess_mode`). Samples of a restarted worker stay in the totals until the next full restart. `compose.scale.yml` runs 4 workers with pool 8+4 and acquire concurrency 8. Migration 0010 sets aggressive autovacuum reloptions on `minion_subtask` (scale factor 0.02, threshold 1000) to keep the hot-updated table's bloat bounded.

`MINION_MAX_OPEN_ACQUIREMENTS` (4, 1..64) is per executor and DB-enforced (executor-row lock + count in the claim transaction), so unlike the ADR-0012 limits it holds across workers and replicas and needs no matching setting elsewhere; the scale overlay raises it to 64 for M6.4 parity.

`MINION_RELEASE_LIMIT` (0..100) is read by both server and watchdog and must have the same value on both (the watchdog applies it to free `NOT_ACKNOWLEDGED` expiries, the server to releases).

Database role: the compose `minion` superuser is dev only. A non-dev deployment runs `migrate` as the schema owner and the server and watchdog as a separate non-superuser, DML-only application role.

Executor distribution: `uv tool install`, a generated per-user `launchd` plist (`minion-executor service install`, ADR-0011; no code signing), config in `~/.config/minion/executor.toml`,
token in macOS Keychain.

## 7. Observability
- Grafana with Postgres datasource; dashboards provisioned from `deploy/grafana/` (`dashboards/tasks.json`, `dashboards/fleet.json`).
- Grafana reads through the `minion_ro` schema (migration 0007): views `executor` (no `token_hash`), `task` (no `params`, no S3 URIs), `subtask` (no input/output keys, `error_code` only), `attempt` (plus model and record count; `error_code` is the failure code or release reason) and `heartbeat` (running subtasks). The views run with their owner's rights, so the role `minion_readonly` has no privilege on the `public` tables. `deploy/postgres/initdb` creates the login `minion_grafana` (member of `minion_readonly`; password `MINION_GRAFANA_DB_PASSWORD`, a dev-only default). Enforced limits: privileges (`SELECT` on `minion_ro` only), `CONNECTION LIMIT 5`, `temp_file_limit` 1 GB (superuser-only setting), `TEMPORARY` and `EXECUTE` on the large-object creators (`lo_creat`, `lo_create`, `lo_from_bytea`, `lo_import`), all advisory-lock functions and `pg_logical_emit_message` revoked from PUBLIC (the app owner keeps the large-object creators; a non-superuser app role that uses large objects needs the grant; the app uses no advisory locks) and `CONNECT` on `postgres`/`template1` revoked (never on the database the script runs in); the SQL is `deploy/postgres/initdb/01-grafana-role.sql.in`, replayed by an integration test; `statement_timeout` 10 s and `default_transaction_read_only` are only session defaults the role can override, and so are `work_mem` and similar: **memory/CPU exhaustion by a hostile Grafana user is NOT prevented** (`temp_file_limit` caps disk only), and neither are LISTEN/NOTIFY queue growth, long-open transactions (they hold AccessShare locks that stall `ACCESS EXCLUSIVE` migrations and hold back vacuum; `idle_in_transaction_session_timeout` is user-settable) or XID burn. Production should point Grafana at a read replica or put a query-timeout proxy in front. The view `minion_ro.executor` has a computed `online` column (same rule as `GET /executors`; 600 s busy / 120 s idle are hardcoded in the view, recreate it if `MINION_EXECUTOR_ONLINE_*` change); the error-code columns expose a value only when it matches `^[A-Z0-9_]{1,64}$`. **Upgrade note for an existing `pgdata` volume:** initdb runs only on an empty data volume, so create the login by hand before starting the new Grafana: `docker compose exec postgres sh /docker-entrypoint-initdb.d/01-grafana-role.sh` works if `MINION_GRAFANA_DB_PASSWORD` is set in the container (or run `01-grafana-role.sql.in` with `psql -v pw=...` as the superuser), then `alembic upgrade head`. The role hardening (connection limit, `TEMPORARY` and large-object creation revoked) only applies when you run it. Managed Postgres without CREATEROLE: migration 0007 skips the role with a NOTICE; create it and grant `USAGE` on the schema and `SELECT` on its tables.
- Grafana is an operator tool: the fleet dashboard exposes per-owner activity (S-11); never expose it to ordinary users. Grafana OSS has no per-datasource permissions, so any account in the Grafana org gets full `minion_ro` read access: production Grafana must be operator-only or a separate org. Changing a column the views use needs the views dropped and recreated in the same migration.
- Web UI in production (ADR-0010): set `MINION_UI_ALLOWED_HOSTS` (bare `host[:port]` list; guards against DNS rebinding) and generate `MINION_UI_SESSION_SECRET` with `minion-server-admin generate-ui-secret`; hand-picked phrases are weak.
- `/metrics` (prometheus-client, off unless `MINION_METRICS_ENABLED`, optional `MINION_METRICS_TOKEN`, >= 32 chars, empty = unset; a startup warning when enabled without a token on a non-loopback host): HTTP counters by route template, method and status, and acquire outcomes. No identifiers.
- Panels: tasks by status, subtasks/min (from `minion_subtask_attempt`), active executors
  (heartbeat < 10 min or polled < 2 min), reclaim rate, per-model throughput, top contributors,
  **fleet capacity** (Σ donated memory by profile, paused executors by reason, releases due to downgrade).
- Structured logs with `task`, `subtask_id`, `acquirement_id`, `executor_id` fields.

## 8. Roadmap (milestones)

1. **M0 – Scaffolding:** uv workspace, compose stack, Alembic baseline with 7 tables, CI (ruff, mypy, pytest).
2. **M1 – Core protocol:** task create + splitter, acquire/complete/fail, executor loop + worker subprocess with a fake runner. Race tests green.
3. **M2 – Fault tolerance:** heartbeats, watchdog, release, cancel, stale-acquirement handling. Implemented (TASK-3.1–3.3, ADR-0008).
4. **M3 – Real inference + benchmark:** MLX completion (batched) and MLX embedding runners, Ollama/HF
   fallback runners, model cache, `models.yaml` admission. `minion-executor bench`: same model on
   mlx-lm batched vs Ollama(MLX) vs HF/mps — tokens/s, records/h, peak memory. Confirms/adjusts ADR-0005 defaults.
   Done (TASK-4.4): mlx vs hf measured on M3 Pro, ADR-0005 confirmed; Ollama unmeasured (TASK-13).
5. **M4 – Capacity governor:** profiles, signals, duty-cycle throttle, auto-pause, budget reporting,
   downgrade-release. Measure NF-8/NF-9 (TASK-5.6: `bench --budget-check`, reaction tests; numbers in ADR-0006 and docs/bench/).
6. **M5 – Ops & usability:** Grafana dashboards, web UI, launchd packaging, auth hardening; usability
   test with real participants during video calls (NF-7) to tune default profiles. NF-7 proxy: `bench --foreground-check` (TASK-6.9); human protocol in docs/usability/nf7-video-call-protocol.md.
7. **M6 – Scale test & menu-bar app:** simulated 500 executors against 1M subtasks (NF-1..NF-3; results in [docs/bench/m6.4-scale-apple-m3-pro.md](bench/m6.4-scale-apple-m3-pro.md));
   menu-bar UI for profile/pause.

Reverse proxy and flood limits (ADR-0018): the server never reads `X-Forwarded-For` and has no per-IP limiter. Executors on a corporate network often share one NAT egress IP, so a per-IP limit must be sized for the whole fleet and lives at the TLS proxy. Inside the server, unauthenticated floods are bounded by the auth pool and the token cache, not by IP.

## 9. ADR index
- [ADR-0001](adr/0001-python-for-all-components.md): Python for all components — **Accepted**
- [ADR-0011](adr/0011-launchd-user-agent.md): launchd packaging as a per-user LaunchAgent — **Accepted**
- [ADR-0013](adr/0013-acquisition-claim-window.md): Single-statement acquisition claim over a random window; unique-constraint acquisition over `SKIP LOCKED` — **Accepted**
- [ADR-0014](adr/0014-unacknowledged-expiry-budget.md): Unacknowledged EXPIRED attempts do not consume the retry budget (capped) — **Accepted**
- [ADR-0015](adr/0015-sharded-task-counters.md): Sharded task counters and post-commit task settle — **Accepted**
- [ADR-0016](adr/0016-nf1-workload-scope.md): NF-1 is defined against a tested numeric workload scope; overload is shed, not queued — **Accepted**
- [ADR-0017](adr/0017-presigned-post-output-uploads.md): Output uploads use a presigned POST with a signed size range — **Accepted**
- [ADR-0018](adr/0018-executor-token-cache-and-auth-lane.md): Executor token cache with a bounded lane for unknown tokens — **Accepted**
- [ADR-0019](adr/0019-per-executor-open-acquirement-cap.md): Per-executor open-acquirement cap under an executor-row lock — **Accepted**
- ADR-0003: Presigned URLs instead of executor S3 credentials *(to write)*
- [ADR-0004](adr/0004-byte-range-input-slicing.md): Byte-range input slicing — **Accepted**
- [ADR-0005](adr/0005-mlx-default-inference-engine.md): MLX as default inference engine — **Accepted**
- [ADR-0007](adr/0007-api-keys-config-file.md): User API keys from a config file, not a DB table — **Accepted**
- [ADR-0008](adr/0008-fault-tolerance-reclaim-and-cancel.md): Fault-tolerance deviations: SKIP LOCKED reclaim, budget, lazy cancel, max lease, executor interrupt — **Accepted**
- [ADR-0009](adr/0009-offline-sandboxed-inference-worker.md): Offline, sandboxed inference worker with a pinned, verified model fetch — **Accepted**
- [ADR-0010](adr/0010-web-ui-session-auth.md): Web UI login with an API key and a stateless session cookie; OIDC via reverse proxy — **Accepted**
- [ADR-0012](adr/0012-in-memory-executor-rate-limits.md): In-memory per-replica rate limits on register/acquire/presence (soft state, not coordination) — **Accepted**
- [ADR-0006](adr/0006-cooperative-capacity-governor.md): Cooperative capacity governor (duty cycling) for partial allocation; D17 controller-side GPU guard (TASK-15) — **Accepted**
