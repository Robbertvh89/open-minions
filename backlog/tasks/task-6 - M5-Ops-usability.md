---
id: TASK-6
title: 'M5: Ops & usability'
status: In Progress
assignee: []
created_date: '2026-10-15 14:02'
updated_date: '2026-10-17 03:30'
labels:
  - milestone
  - M5
dependencies: []
ordinal: 6000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Grafana dashboards, web UI, launchd packaging, auth hardening, usability test during video calls (NF-7).
<!-- SECTION:DESCRIPTION:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
BLOCKING (from TASK-5.2): the executor launchd plist must set ExitTimeOut >= MINION_EXECUTOR_STOP_GRACE_S + 15 s (default grace 30 -> >= 45 s). launchd's default ExitTimeOut is 20 s; a SIGKILL before the bounded shutdown finishes skips release and leaves payload temp files until the next start (cleanup_stale_tmp).

# TASK-6 (M5) Ops & usability: plan (planner, 2026-10-17)

## 1. Goal and acceptance criteria (restated)
M5 makes open-minions operable by people who are not its developers:
- operators get dashboards, a fleet view and a web UI;
- participants get a launchd service that starts at login, stops cleanly and never keeps the Mac awake;
- authentication is hardened where the M1-M4 audits left gaps;
- NF-7 (foreground responsiveness during video calls) gets the evidence that can be collected
  automatically, plus a protocol for the human test that cannot.

Proposed parent AC:
- #1 Grafana dashboards provisioned from deploy/grafana, using a read-only DB role (FR-40, FR-42).
- #2 Web UI: log in, list, detail, create, cancel and fleet view (FR-1, FR-7, FR-8, FR-42). Answers Q4 (ADR-0010).
- #3 launchd LaunchAgent with ExitTimeOut >= MINION_EXECUTOR_STOP_GRACE_S + 15 s (BLOCKING note, FR-17).
- #4 Auth hardening:
  - per-team pools (TASK-10, S-1);
  - per-executor rate limits (TASK-9);
  - Range-scoped presigned GET and an output size cap (TASK-9 #2);
  - key expiry, reload and an operator role (ADR-0007 follow-up).
- #5 NF-7: automated proxy measured on this Mac (`bench --foreground-check`). The human video-call study
  is documented as a protocol: NOT VERIFIABLE HERE.

SPEC map:
- FR-1, FR-7, FR-8 -> 6.6, 6.7
- FR-40, FR-41 -> 6.5
- FR-42 -> 6.4, 6.5, 6.7
- FR-17 -> 6.8
- FR-22/G4 (`prefer` CLI, never built: ARCHITECTURE §2.2 lists it) -> 6.2
- FR-53 optional "call active" item -> 6.10
- S-1 -> 6.1, 6.3
- S-2 -> 6.3
- S-3 -> 6.1, 6.2, 6.8
- S-5 -> 6.5
- S-8 -> 6.1
- S-11 -> 6.4, 6.5, 6.10
- NF-5 note -> 6.2
- NF-7 -> 6.9
- Q4 -> ADR-0010
- Q5 -> input from 6.9

## 2. Touchpoints (current code)
Server:
- App, routers and /healthz: packages/minion-server/src/minion_server/app.py:18-60. There is no /metrics and no UI router.
- User auth:
  - packages/minion-server/src/minion_server/auth.py:48-102. The keys file is loaded once at
    app.py:24 and has no expiry, no roles and no reload.
  - Executor auth: auth.py:105-122.
- Task API: packages/minion-server/src/minion_server/api/tasks.py:119-218.
  - Only create, get and cancel exist. GET /tasks, /subtasks and /executors (SPEC §6.1) are missing.
  - create_task applies no pool policy, so allowed_pool passes through as given (tasks.py:150-158).
- Executor API: packages/minion-server/src/minion_server/api/executor.py:50-165.
  - presence (90-97), acquire (100-114) and register (50-70) have no rate limit.
  - The heartbeat 429 pattern is at services/heartbeat.py:36-44 and repositories/heartbeats.py:29-40.
  - release (157-165) takes no body.
- Presigning: packages/minion-server/src/minion_server/storage.py:69-80 signs Bucket and Key only (no Range).
  The input GET is built at services/acquisition.py:89-90.
- The complete size check is at services/completion.py:67 (it compares sizes; there is no cap).
  The release attempt record is at services/completion.py:141-180.
- Settings: packages/minion-server/src/minion_server/settings.py:17-88.
- Admin CLI: packages/minion-server/src/minion_server/admin.py:26-107. There is no list or rotate command.
- DB: packages/minion-server/src/minion_server/db/models.py.
  - minion_executor.token_hash is at :180; minion_task.params is at :105.
  - Migrations stop at migrations/versions/0005_engine_fake.py.
- Grafana: deploy/grafana/provisioning/datasources/postgres.yml has only the datasource, logged in as
  `minion`, which is the table OWNER. There are no dashboards. deploy/compose.yml:1-3 says the
  server and watchdog services are "added in a later milestone".

Executor:
- run: packages/minion-executor/src/minion_executor/main.py:332-403. status: :467-487. pause, resume and profile: :410-464.
- Settings: settings.py:16-132. stop_grace_s is at :25 and state_dir at :43 (default
  capacity/control.py:52, ~/Library/Application Support/open-minions).
- Token: secrets.py:16-35 (env var, then Keychain `open-minions-executor`).
- Loop error handling: loop.py:465-467 (register) and :516-518 (acquire) raise on any 4xx, so a 429
  would kill the executor. Presence is at loop.py:420-441 (non-fatal, retry <= 10 s, ignores
  Retry-After). Release call sites: loop.py:530, 703, 746 (BUDGET_DOWNGRADE), 932, 1066, 1085,
  1105, 1296; _release is at :1311.
- Signals: capacity/signals.py:56-64 (SignalSnapshot has no call or camera field). Pure governor:
  capacity/governor.py:155-200.
- The input Range header is already sent: io.py:31.

Docs:
- SPEC FR-17 (:145) and ARCHITECTURE §2.2 (:201-202) carry the ExitTimeOut rule.
- ARCHITECTURE §6 (:496) says "signed launchd plist". That is inaccurate: plists are not signed. Fix it in 6.8.
- ADR-0009:196 defers the separate macOS user to "M5 packaging". Decision: stays deferred (needs root and a privileged helper); record it in ADR-0009.
- ADR-0007:26/36 defers the created_by FK to M5. Decision: won't do (no users table, keep the 7 tables); record it in ADR-0007.

## 3. Triage of open follow-ups
IN M5:
- TASK-10, the per-team default and allowed pools -> 6.1. Close TASK-10 when 6.1 merges.
- TASK-9:
  - per-executor rate limit on presence, acquire and register, honoured by the executor -> 6.2.
  - #2: Range-signed GET plus an output size cap -> 6.3.
- ADR-0007 follow-ups: key expiry, reload without a restart, operator role -> 6.1.

DEFERRED (stay where they are; reason):
- TASK-9 rest:
  - macOS ACL check and openat component walk: local same-user hardening, not auth. Revisit with the M6 menu-bar app.
  - Per-batch wall-clock cap and tick liveness; server cap on records x max_tokens: resource limits, not auth.
  - bench and the Ollama opt-in: TASK-13.
- TASK-11 (split lease timer): M2 robustness. Matters at scale, so M6.
- TASK-12 (worker IPC and Ollama robustness): Low; not auth.
- TASK-15 (controller-side GPU duty enforcement): security against a compromised worker. Deferred,
  BUT gated by 6.9: if the foreground proxy fails at light or balanced, TASK-15 and/or the Q5
  adaptive mode become M5 blockers (orchestrator decides).
- TASK-13: unrelated (Ollama bench).
- PUT size enforcement at upload time: needs a presigned POST with content-length-range, which
  changes the wire contract (output_url -> form fields) and is unverified on SeaweedFS. M5 caps at
  `complete` instead (nothing oversize is ever promoted) and documents an S3 lifecycle rule on `_attempts/`.

## 4. Sub-tasks (one PR each)
Order:
- 6.8 first (BLOCKING, executor only, independent).
- 6.1 -> 6.4 -> 6.6 -> 6.7.
- 6.1 -> 6.5.
- 6.2, 6.3, 6.9 and 6.10 are independent.
- 6.11 is optional, after 6.5.

Can run in parallel: 6.8 ∥ 6.1 ∥ 6.3 ∥ 6.9.

### 6.1 Auth: API keys file v2 (operator role, expiry, live reload, per-team pools)
Files:
- auth.py: `_KeyEntry` gains `operator: bool = False` and `expires_at: datetime | None`. `_KeysFile`
  gains `teams: dict[slug, TeamPolicy{default_pool: str | None, allowed_pools: list[str] | None}]`.
- `KeyStore` holds the parsed file. It is reloaded when the file's mtime or size changes, checked at
  most every `MINION_API_KEYS_RELOAD_S` (default 5). An invalid file after a change fails CLOSED
  (every user request 401, error logged): a revocation must never be silently ignored.
- `current_user` rejects expired keys. New `require_operator` dependency.
- app.py:24 builds the KeyStore.
- api/tasks.py create_task:
  - no allowed_pool -> the team's default_pool;
  - a pool outside allowed_pools -> 403 POOL_NOT_ALLOWED;
  - an explicit null when the team has allowed_pools -> the default, or 422 POOL_REQUIRED if there is no default.
- settings.py, admin.py:
  - `generate-api-key --operator --expires-days N`;
  - `list-executors` (id, owner, pool, status, token_expires_at, last_seen_at; never the hash);
  - `rotate-executor-token --executor-id` (new hash, same id, prints once).
- repositories/executors.py: list and rotate.
- Docs: ADR-0007 amendment, SPEC S-1/S-8, .env.example.

AC:
- Removing a key from the file stops it within reload_s + 1 s without a restart (test with a short interval).
- A malformed rewrite gives 401 for all keys and an error log; fixing the file restores access.
- An expired key gives 401.
- A task without allowed_pool gets the team default. A pool outside the allowed set gives 403. Teams without a policy behave as today.
- Non-operators get 403 from require_operator.
- rotate-executor-token: the old token gets 401 and the new one works with the same executor_id. list-executors never prints the hash.
- Unit tests for the parser and reload, plus integration tests (task create, rotate).

Invariants: none touched. Executor auth stays DB-backed.

### 6.2 Executor endpoint rate limits (TASK-9) and `minion-executor prefer` (G4)
Server:
- New minion_server/ratelimit.py: an in-memory per-(executor_id, op) minimum interval on the
  monotonic clock, with a bounded LRU (e.g. 100k entries).
- Settings: MINION_PRESENCE_MIN_INTERVAL_S (5), MINION_ACQUIRE_MIN_INTERVAL_S (0.5),
  MINION_REGISTER_MIN_INTERVAL_S (5). 0 disables.
- Hook in api/executor.py, after auth and before any DB write or acquisition logic:
  429 `TOO_MANY_REQUESTS` with Retry-After.
- NF-5 note: this is soft per-replica state, so the effective limit is N replicas x limit. It is not
  coordination state and decides no ownership. The alternative is a DB conditional UPDATE: one
  column per op plus a migration, rejected as heavier.

Executor:
- loop.py: a 429 on acquire and register sleeps max(Retry-After, backoff) and continues instead of
  raising (:466, :517). Presence honours Retry-After (:436-441).
- main.py: new `prefer --server URL (--team T [--project P] [--task-name N] | --clear)`. It uses the
  token from secrets.load_token and PUT /executor/preference. require_secure_url, and exit 2 on
  usage errors.

AC:
- Presence faster than the interval gets 429 with Retry-After and no DB write (row unchanged).
- Same for acquire and register.
- The executor survives a 429 on acquire, register and presence and waits at least Retry-After (unit tests with a fake client).
- The concurrent acquire race test is still green with limits enabled. Existing e2e tests pass, at
  default settings or with limits disabled where they acquire back-to-back.
- `prefer` sets and clears the preference (integration: the next acquire prefers that team).

Invariants:
- 2: the limiter runs before acquisition and never replaces ON CONFLICT.
- 1: the prefer CLI uses REST only.

### 6.3 Presigned scope: Range-signed GET and output size cap (TASK-9 #2)
Files:
- storage.py: `presign_get(key, ttl, byte_range=(start, end_excl))` passes `Range="bytes=s-e"` in
  Params, so Range is a signed header.
- services/acquisition.py:89 passes the subtask range.
- services/completion.py: `stat.size > MINION_MAX_OUTPUT_BYTES` (default 4 GiB) -> 422
  OUTPUT_TOO_LARGE (not promoted; the executor maps it to retryable COMPLETE_REJECTED).
- executor loop: refuse to upload above the same cap. Use FailRequest code OUTPUT_TOO_LARGE, non-retryable.
- Docs: ARCHITECTURE §3.3, the lifecycle rule for `_attempts/` (deploy docs), SPEC S-2.

AC:
- Integration (SeaweedFS):
  - a GET with the signed range gives 206;
  - a GET without the Range header, or with a different range, gives 403;
  - the existing e2e passes unchanged (io.py:31 already sends the exact header).
- If SeaweedFS does not enforce signed Range, record it in the PR and keep the code. The S3/AWS behaviour is documented.
- An oversize attempt object is never promoted (422) and its subtask is retried, then fails after max_attempts.

Invariants:
- 4 (strengthened).
- 5: no promotion of an oversize object.

### 6.4 Read API: task list, subtasks, fleet (FR-7, FR-42)
Files:
- minion-common schemas: `TaskPage{items: list[TaskOut], next_cursor}`,
  `SubtaskOut{subtask_id, status, attempt, max_attempts, record_count, error_code, updated_at}`,
  `SubtaskPage`, `ExecutorOut{executor_id, hostname, owner, pool, chip, total_memory_gb, engines,
  status, profile, budget, paused_reason, online, busy, last_seen_at, token_expires_at}`.
- client.py methods.
- repositories/tasks.py:
  - list_tasks: keyset (created_at DESC, team, project, task_name), limit <= 100, counts in one grouped query per page;
  - list_subtasks: keyset on subtask_id, status filter.
- repositories/executors.py: list_fleet (busy = has an acquirement; online = last_seen < 2 min or heartbeat < 10 min, per ARCHITECTURE §7).
- api/tasks.py: new routes. New api/fleet.py: `GET /api/v1/executors` (require_operator).
- SPEC §6.1 rows.

AC:
- GET /tasks returns only the caller's teams. A filter by another team gives 403.
- Pagination is stable under concurrent inserts.
- /subtasks paginates, filters by status, and gives 403/404 like get_task.
- /executors: 403 for non-operators. It never returns token_hash, and busy/online are correct in an integration test.
- Opaque, validated cursors: a tampered one gives 422.

Invariants: read-only SQL in repositories/ only.

### 6.5 Observability: read-only views, release reason, Grafana dashboards, /metrics
Files:
- Migration 0007 creates schema `minion_ro` with views over the 7 tables:
  - minion_executor without token_hash;
  - minion_task without params (the `system` prompt is payload, S-5);
  - minion_subtask without input keys;
  - minion_subtask_attempt;
  - heartbeat.
- The same migration creates role `minion_readonly` NOLOGIN if it does not exist (DO block; a
  NOTICE and skip without CREATEROLE), GRANT USAGE and SELECT on minion_ro only.
- deploy/postgres/initdb: `minion_grafana` LOGIN IN ROLE minion_readonly with statement_timeout 10s, dev password.
- The datasource uses minion_grafana.
- deploy/grafana/provisioning/dashboards/{dashboards.yml, minion-tasks.json, minion-fleet.json}:
  - Panels: tasks by status, subtasks/min, records/min, per-model throughput, attempt outcomes over
    time, reclaims/hour by code, top failure codes, active/busy/paused executors, paused by reason,
    donated memory by profile, top contributors, releases by reason.
- Release reason:
  - minion-common `ReleaseRequest{code: [A-Z_]{1,64} | None}` as an optional body (old executors send none);
  - services/completion.py:141-180 stores it in attempt.error;
  - executor _release(a, bound, code) at every call site in §2.
- /metrics: `prometheus-client`.
  - Counters and histograms by route template, method and status, plus acquire outcome.
  - No ids or team labels.
  - MINION_METRICS_ENABLED (default true).
- Docs: SPEC §7 (views are not tables; the 7-table count holds), ARCHITECTURE §7, ops doc.

AC:
- Integration: every panel's SQL, with Grafana macros substituted, runs as minion_readonly against the migrated DB.
- As that role, `SELECT token_hash FROM minion_executor` fails with permission denied.
- A static test checks that every panel queries minion_ro only.
- A release with a code shows in v_attempt. A release without a body still works and the ownership check is unchanged.
- /metrics exposes the counters without ids.
- Migration up/down test.
- Live (verifier): compose up shows both dashboards with data from an e2e run.

Invariants:
- 3: release still requires the current acquirement.
- S-5: no params or payload in views or metrics.

### 6.6 Web UI part 1: session auth, security headers, task list and detail (ADR-0010)
Files:
- New minion_server/ui/{routes.py, session.py, templates/*.html, static/htmx.min.js (vendored, version and sha256 pinned in a test)}.
- Deps: jinja2, python-multipart.
- Session: a stateless cookie `minion_session`, HMAC-SHA256 with MINION_UI_SESSION_SECRET (SecretStr, >= 32 bytes; the UI is off with 404 when unset).
  - Payload: key sha256, issued_at, expires_at (MINION_UI_SESSION_TTL_S, 8 h).
  - Re-resolved against the KeyStore on every request (6.1), so a removed or expired key ends the session.
  - Flags: HttpOnly, SameSite=Strict, Secure unless the host is loopback.
- CSRF: a per-session HMAC token on every POST, plus an Origin/Referer check (login included).
- Headers:
  - CSP `default-src 'self'; script-src 'self'; frame-ancestors 'none'; form-action 'self'; base-uri 'none'`;
  - nosniff, Referrer-Policy no-referrer, Cache-Control no-store.
- Pages: login and logout, task list (team/status filter, pagination), task detail (counts,
  progress, subtask table; htmx refreshes the fragment every 5 s).
- Reuse the 6.4 repository and service functions, not HTTP.

AC:
- Login with a valid key sets a cookie with the required flags. Wrong, expired or removed keys are rejected.
- A tampered or expired cookie redirects to login.
- Another team's task gives 403.
- A POST without a valid CSRF token or with a foreign Origin gives 403.
- The CSP header is present.
- An error string containing `<script>` renders escaped.
- The UI is 404 when the secret is unset.

Invariants: 4 (no S3 content through the server: the UI shows output URIs, it never downloads).

### 6.7 Web UI part 2: create, cancel, fleet page
Files:
- Extract the create_task and cancel_task logic from api/tasks.py:119-218 into
  services/tasks.py, so handlers stay thin. Both API and UI call it.
- UI forms: create (fields per TaskCreate; params as a JSON textarea -> parse_params; the same
  error codes are shown), cancel (confirm and CSRF).
- Fleet page (operator only): per-executor online/busy/profile/budget/paused_reason, Σ memory by profile, paused by reason.

AC:
- Creating via the UI gives the same row and validation as POST /tasks (422 codes shown, 409 TASK_EXISTS).
- Cancel via the UI follows FR-8 semantics (idempotent; 409 on a finished task).
- The fleet page gives 403 for non-operators.
- The API tests are unchanged and green after the extraction.

### 6.8 launchd packaging: `minion-executor service install|uninstall|status` (BLOCKING: ExitTimeOut)
Files:
- New minion_executor/service.py with pure `render_plist(...) -> bytes` (plistlib, deterministic) and `exit_timeout_for(grace_s) = ceil(grace_s) + 15`.
- main.py: a `service` typer sub-app.
- Example file deploy/launchd/com.open-minions.executor.plist, generated by render_plist with
  defaults. A test asserts the file equals render().
- Docs: docs/ops/executor-install.md (uv tool install, Keychain item via
  `security add-generic-password -s open-minions-executor -a "$USER" -w` with an interactive
  prompt, never the token on argv), ARCHITECTURE §2.2 and §6, SPEC FR-17.

Plist:
- Label, ProgramArguments [absolute path to minion-executor, run, --server, URL, optional --profile, --engines].
- EnvironmentVariables {MINION_EXECUTOR_STOP_GRACE_S: the effective grace, plus an allow-list of
  non-secret MINION_EXECUTOR_* that are set (cache dir/GB, state dir, config, heartbeat, idle
  unload)}. Never TOKEN, DEV, TEST_HOOKS, SANDBOX, FETCH_SANDBOX or FAKE_SIGNALS. Never HF_TOKEN or proxy credentials.
- ExitTimeOut = ceil(grace) + 15. RunAtLoad true.
- KeepAlive {SuccessfulExit: false}, ThrottleInterval 60.
- LimitLoadToSessionType Aqua (needs the GUI session for HID idle and the login Keychain).
- ProcessType Standard (see decision D1).
- StandardOut/ErrPath ~/Library/Logs/open-minions/executor.log.
- No KeepAlive-on-network or wake keys and no caffeinate (invariant 6: never blocks sleep).

install:
- Preflight runs the same validation as run (secure URL, settings, executor.toml, state dir, a
  Keychain token is present). Refuses MINION_EXECUTOR_DEV=1. Refuses when the token is only in
  $MINION_EXECUTOR_TOKEN (launchd cannot see it).
- Writes the plist 0644 (not writable by others), then `launchctl bootstrap gui/$UID` (`--no-load` to skip).
- `--dry-run` prints the plist.

uninstall and status:
- uninstall = `launchctl bootout gui/$UID/<label>` (SIGTERM, then launchd waits ExitTimeOut), then removes the plist.
- status = parses `launchctl print` (state, pid, last exit code, exit timeout). It warns when exit
  timeout < grace + 15, then runs the existing `status`.
- run: an optional MINION_EXECUTOR_LAUNCHD_EXIT_TIMEOUT_S (written by install) that is
  < stop_grace_s + 15 makes run exit 2. This catches a hand-edited grace.

AC:
- render_plist with grace 0 / 30 / 300 gives ExitTimeOut 15 / 45 / 315.
- No secret keys or values appear in the plist (test with the token and HF_TOKEN set in env).
- `plutil -lint` passes (macos marker).
- install refuses with DEV=1 and when the token is env-only.
- run exits 2 on an inconsistent launchd timeout.
- Live (verifier, this Mac): install with a test label against a local server, using the real mlx
  engine and a small model (bge-small-en), so that no DEV mode is needed. Then uninstall during a
  running subtask: the process exits within grace + 10 s, the subtask is RELEASED, and there is no
  SIGKILL in `launchctl print` last exit.
- Verify the worker sandbox self-test passes from a `uv tool install` location (site-packages under ~/.local/share/uv).

### 6.9 NF-7: synthetic foreground-load benchmark and usability-test protocol
Files:
- New minion_executor/runners/fg_probe.py: MLX, kept under runners/ per CLAUDE.md. A deadline
  probe in its own spawn subprocess submits a fixed small GPU kernel (about 5 ms) every 33.3 ms
  (30 fps) and records completion latency.
- New minion_executor/bench_foreground.py:
  - Phases per profile: baseline (worker loaded and idle), then loaded (worker running the real
    model at light / balanced, full as information).
  - Also measures process-launch latency (`/usr/bin/osascript -e 'return 1'`, N=20, p50/p95) as an app-launch proxy.
  - Pure `nf7_proxy_verdict`: frame p95 <= 1.10 x baseline, deadline-miss rate +<= 1 pp, launch p50 <= 1.10 x baseline. Median of --repeats.
- main.py: `bench --foreground-check`.
- docs/usability/nf7-video-call-protocol.md: participants, Teams/Zoom call, a blind A/B over
  off/light/balanced, Zoom/Teams statistics (fps, jitter), rating scale, results template.
- docs/bench/m5-foreground-<mac>.md with results.

AC:
- The pure verdict is unit-tested.
- The CI path runs with a fake probe and the fake engine.
- The Mac run of light and balanced on qwen2.5-0.5b-mlx and minilm-mlx is committed (derived figures only).
- If the proxy FAILS at light or balanced: open a follow-up and flag TASK-15/Q5 to the orchestrator. Do not loosen thresholds.

NOT VERIFIABLE HERE:
- the human study (real participants, real video calls, audible/visible degradation);
- default-profile tuning from its results.
The protocol doc is the deliverable. The parent AC #5 remains partially open until the study is run.

### 6.10 Call-aware governor: camera or microphone in use -> light or pause (FR-53 optional item)
Files:
- capacity/signals.py: MacSignals reads `kAudioDevicePropertyDeviceIsRunningSomewhere` (default
  input device, CoreAudio) and `kCMIODevicePropertyDeviceIsRunningSomewhere` (CoreMediaIO
  cameras) through ctypes. These are public APIs with no TCC prompt. SignalSnapshot gains
  `call_active: bool | None`. FileSignals and NullSignals are extended.
- capacity/config.py: `on_call = "light" | "pause" | "ignore"` (default "light").
- governor.py: on a call, de-escalate at once to min(current, light), or pause with
  PausedReason.IN_CALL (a new enum value; the wire is a pattern string, so it is compatible).
  Escalation needs the usual 60 s hold after the call ends.
- status shows it.

AC:
- Pure governor tests: call on drops to light within one poll; call off keeps light for >= 60 s;
  on_call=pause pauses with in_call; ignore changes nothing.
- The NF-9 reaction test is extended with a call event (< 5 s).
- MacSignals returns a bool on this Mac (macos marker). Microphone and camera activation needs
  a person or TCC permission: manual check, NOT FULLY VERIFIABLE HERE.
- SPEC FR-53 and S-11 are updated: a profile drop during calls tells the server "probably in a call". Owners who object set on_call = "ignore".

### 6.11 (optional) Compose: migrate, server and watchdog containers
Files:
- deploy/Dockerfile: one image, non-root, read-only rootfs.
- deploy/compose.yml services: migrate (alembic upgrade head), server, watchdog (healthchecks, depends_on).

AC:
- `docker compose up -d` then an e2e smoke run with an executor on the host reaches SUCCEEDED, and the dashboards show it.
- This can slip to M6 if time is short.

## 5. Decisions and deviations (ADRs)
- D1 ADR-0011 launchd packaging ([ours]):
  - per-user LaunchAgent, not a LaunchDaemon, and no separate macOS user (ADR-0009 alternative stays deferred);
  - ProcessType Standard, because Background would clamp the whole tree including `full` overnight
    and coalesce governor timers (NF-9); throttling stays per worker via cpu_priority;
  - KeepAlive SuccessfulExit=false plus preflight at install;
  - no code signing or notarisation: needs an Apple Developer ID, NOT VERIFIABLE HERE.
- D2 ADR-0010 web UI auth (answers Q4): API-key login, stateless HMAC session cookie, CSRF token
  plus Origin check. OIDC is deferred to a reverse proxy (oauth2-proxy) in front of /ui. Residual:
  logout cannot revoke a stolen cookie before its TTL; removing the key does.
- D3 ADR-0007 amendment: keys file v2. Reload fails closed. The created_by FK is closed as won't-do (it would need an 8th table).
- D4 SPEC note: in-memory per-replica rate limits (NF-5 soft state). An ADR is only needed if the reviewer objects.
- D5 SPEC: /executors and the fleet dashboards are operator-only (S-11: they reveal owner activity).
- D6 ADR-0006 amendment: the on_call signal (6.10) and the NF-7 proxy definition (6.9). SPEC NF-7
  keeps the human criterion as the acceptance test; the proxy is an extra automated gate, not a replacement.

## 6. Risks and open questions
- SeaweedFS may ignore signed Range or POST policies (6.3). That is why it is a cap at complete, and the integration test records the behaviour.
- Views block `ALTER COLUMN TYPE` on referenced columns. Future migrations must drop and recreate
  them; add this to the db-migration skill notes. CREATE ROLE needs CREATEROLE on managed PG, hence the DO block fallback.
- launchd logs are not rotated: newsyslog needs root. Log volume is low (ids only). Document truncation and consider a size check in `service status`.
- `uv tool install` of a workspace member with workspace sources is unverified. Fallback: ProgramArguments points to the repo venv binary.
- The 10 % NF-7 proxy thresholds may be noisy, hence the median of repeats. The thresholds are provisional until the human study.
- A camera or microphone "in use" can also be a dictation tool or music app. The default `light` is a de-escalation only (safe direction).
<!-- SECTION:NOTES:END -->
