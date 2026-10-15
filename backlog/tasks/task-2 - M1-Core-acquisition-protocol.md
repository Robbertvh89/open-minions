---
id: TASK-2
title: 'M1: Core acquisition protocol'
status: In Progress
assignee: []
created_date: '2026-10-15 14:02'
updated_date: '2026-10-15 14:25'
labels:
  - milestone
  - M1
dependencies: []
priority: high
ordinal: 2000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Task create + async splitter, acquire/complete/fail endpoints, executor loop + worker subprocess with fake runner. See ARCHITECTURE §3.1-3.3.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Concurrent-acquire race test (>=50 parallel) yields unique owners
- [ ] #2 End-to-end: task submitted, split, executed by fake runner, outputs promoted in MinIO
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
# TASK-2 plan — M1 core acquisition protocol

## Goal & AC mapping
Users submit a task; the server validates it, splits it asynchronously into byte-range subtasks, and executors pull them through the unique-constraint protocol, run them with a fake runner in a worker subprocess, upload to an attempt-scoped key, and the server promotes the output with CopyObject.
- AC#1: >=50 parallel acquires give unique owners. Covers FR-20/21/22, invariant 2, ARCH §4.
- AC#2: e2e submit -> split -> fake run -> promoted in S3 (SeaweedFS). Covers FR-1..6, FR-10/11/15/16, FR-33, FR-34 (stale rejection on complete/fail/release), S-2, S-7, invariants 1/3/4/5/7.
- Security carry-forward in scope: (1) user keys + team mapping, (2) token expiry + hmac.compare_digest + never logged, (3) allowed_pool + ownership check, (4) attempt-scoped keys + bucket/prefix restriction, (6) https unless localhost. Deferred: (5) Grafana role (M5), (7) prod S3 creds (deploy, M5), Keychain storage (M5; M1 reads `$MINION_EXECUTOR_TOKEN`).
- Out of scope (M2): heartbeat endpoint, watchdog, reclaim, cancel, presence, task list and subtasks listing, fleet view. Acquire still inserts the `minion_executor_heartbeat` row (ARCH §3.2) so M2 needs no protocol change.

## Decisions
- **D1 User API keys come from a config file, not an 8th table (ADR-0007).** `MINION_API_KEYS_FILE` (YAML: `name`, `sha256`, `teams[]`). Lookup by SHA-256, then `hmac.compare_digest`. `Task.created_by` stays a string holding the key's `name`, so there is no FK. Why: SPEC §7 and the paper fix 7 tables, and auth for the web UI/OIDC (Q4) is undecided for M5, so a table now would likely be thrown away. The FK part of carry-forward (1) is consciously deferred to M5 auth hardening. Requires a SPEC §6/§8 note and ADR-0007.
- **D2 Executor tokens are pre-issued by an admin CLI** (`minion-server-admin issue-executor-token --owner --pool --ttl-days`). The CLI inserts a `minion_executor` row (hostname placeholder `"(unregistered)"`) and prints the token once. Migration 0002 adds `minion_executor.token_expires_at timestamptz NULL`. Auth rejects tokens that are expired or REVOKED with 401. Tokens are `pydantic.SecretStr` on the client and never logged.
- **D3 S3 access uses sync `boto3` wrapped in `asyncio.to_thread`.** aioboto3 is rejected because aiobotocore pins botocore tightly, presigning is CPU-only, and CLAUDE.md names boto3. A separate `s3_public_endpoint_url` is used for presigning, so executors can reach a different host than the server does.
- **D4 SeaweedFS runs through testcontainers** (`chrislusf/seaweedfs:4.05`, the same command as compose, with `deploy/seaweedfs/s3.json` mounted). This keeps tests hermetic: CI (`.github/workflows/ci.yml` runs `pytest -v` on ubuntu with Docker) has no compose stack. Mounting `s3.json` means SigV4 presigned URLs are really verified, and the image tag matches compose (`deploy/compose.yml` s3 service).
- **D5 Acquire is one txn per candidate:** `INSERT INTO minion_subtask_acquirement ... SELECT ... FROM minion_subtask WHERE key AND status='PENDING' ON CONFLICT DO NOTHING RETURNING`. The winner then runs `UPDATE minion_subtask SET status='RUNNING', attempt=attempt+1 WHERE key AND status='PENDING' RETURNING attempt`, and rolls back if 0 rows. That re-check closes the race where a completion deletes the acquirement concurrently. It also runs `INSERT heartbeat` and `UPDATE minion_task SET status='RUNNING' WHERE key AND status='PENDING'`. No locks, no check-then-insert: the UNIQUE constraint (`models.py:199`) is still the only arbiter.
- **D6 Complete happens in one txn.** It does `SELECT acquirement FOR UPDATE` (must exist, `executor_id == caller`, else 409 STALE_ACQUIREMENT, identical body for not-found and not-owner). Then it does a HEAD on the attempt key to check that the object exists and its size matches, then CopyObject to `{output_prefix}/{subtask_id:06d}.jsonl`. Finally it deletes the acquirement (cascades the heartbeat), sets the subtask SUCCEEDED, sets `output_uri`, inserts the attempt row, updates counters and the derived task status, and commits. The row lock serializes against the M2 watchdog (ARCH §4).
- Attempt key: `{output_prefix}/_attempts/{subtask_id:06d}/{attempt}.jsonl`. The presigned PUT covers only that key, with TTL `min(settings, 3600)`.
- URI rules: scheme must be `s3`, bucket must equal `MINION_S3_BUCKET`, the key must start with one of `MINION_S3_INPUT_PREFIXES` / `MINION_S3_OUTPUT_PREFIXES` (default `inputs/`, `outputs/`), and `..`, `//`, a leading `/` or control chars are rejected. Violations return 422.

## Dependencies
- minion-server: `boto3` (presign, list, get, head, copy), `pyyaml` (models.yaml, keys file), `typer` (admin CLI).
- minion-common: `httpx` (typed client shared by executor and tests).
- dev group: `boto3-stubs[s3]`, `types-PyYAML` (needed for mypy --strict). testcontainers is already present: the SeaweedFS container uses generic `DockerContainer` with no extra install.
- No new executor deps: multiprocessing, hashlib and tempfile are stdlib.

## Steps (each <= ~300 changed lines)
Lanes: **A** = minion-common, **B** = minion-server, **C** = minion-executor. Order: A1 -> A2. B1 -> B2 -> (B3 ∥ B4) -> B5. C1 starts after A1 and runs ∥ to B*; C2 after A2 + C1. E1 last.

**A1 common: API schemas** (`minion_common/schemas.py`, tests)
- `Budget{profile, memory_gb, gpu_duty_cycle, max_batch_size}`
- `TaskCreate{team, project, task_name (slug regex), type, model, input_uri, output_uri, params, records_per_subtask=500 (1..100k), priority=0, allowed_pool?}`, `TaskOut` (+counts)
- `ModelEntry{model_id, type, engine, ref, memory_gb}`
- `RegisterRequest{hostname, chip, total_memory_gb, os_version, engines, profile, budget}`, `RegisterResponse{executor_id}`, `PreferenceRequest`
- `AcquireRequest{engines, budget}`, `Assignment{acquirement_id, attempt, team, project, task_name, subtask_id, type, model: ModelEntry, params, input_url, byte_start, byte_end, record_count, output_url, expires_at}`
- `CompleteRequest{record_count, byte_size, sha256}`, `FailRequest{retryable, code, message (<=1k chars, no payload)}`, `ErrorBody{code, message}`
- Unit tests: validation edge cases.

**A2 common: typed client** (`minion_common/client.py`)
- `ExecutorClient(base_url, token: SecretStr, transport=None)` with `register/set_preference/acquire -> Assignment|None/complete/fail/release`
- `UserClient(base_url, api_key)` with `create_task/get_task`
- `StaleAcquirementError` on 409
- `require_secure_url(url)`: https required unless host in {localhost, 127.0.0.1, ::1}
- Unit tests use `httpx.MockTransport`.

**B1 server foundation** (settings, DB, migration, fixtures)
- `settings.py:4-11` gains: S3 endpoint/public endpoint/keys/bucket/region, input/output prefixes, `presign_ttl_s` (<=3600), `models_file`, `api_keys_file`, `splitter_poll_s`, `executor_token_ttl_days`. Update `.env.example`.
- `db/session.py`: async engine and sessionmaker. `app.py:6-13` gets a lifespan that creates and disposes the engine.
- Migration `0002_executor_token_expiry` + `models.py:171` field `token_expires_at`. Follow the db-migration skill and run the round-trip test.
- Move the container fixtures from `minion-server/tests/conftest.py:14-34` to a repo-root `conftest.py` and add a `s3` fixture (SeaweedFS container + boto3 client + bucket create).

**B2 server: config and security primitives**
- `models_registry.py` loads `models.yaml` into `dict[str, ModelEntry]`. Add `deploy/models.yaml` with example entries plus a `fake-embed` test entry.
- `uris.py`: parse and validate S3 URIs.
- `storage.py`: `S3Storage` with `presign_get/presign_put/list_keys/head/iter_object/copy`, using `to_thread`.
- `auth.py`: the `current_user` dependency (keys file, D1) and the `current_executor` dependency (sha256 + compare_digest, expiry/revoked -> 401).
- `admin.py`: typer `issue-executor-token`, `hash-api-key`, plus a `minion-server-admin` script.
- Unit tests for URI validation, auth (constant-time path, expiry) and the registry.

**B3 server: tasks + splitter** (∥ B4)
- `splitting.py`: pure `slice_jsonl(chunks: Iterable[bytes], start_offset, records_per_subtask) -> Iterator[Slice]`. Slices are newline-aligned and `byte_end` is exclusive; blank lines are skipped; the last line may lack a trailing newline.
- `repositories/tasks.py`.
- `services/splitter.py`: a background loop claims one `SPLITTING` task with `FOR UPDATE SKIP LOCKED` (claim txn held). It iterates input keys in sorted order and assigns deterministic sequential `subtask_id`s. It inserts batches of 1000 in their own txns with `ON CONFLICT DO NOTHING`, so the split is resumable. Then it sets `PENDING` and `total_subtasks`. An empty input ends the task as `FAILED` with an error.
- `api/tasks.py`: `POST /api/v1/tasks` (auth, team in key.teams else 403, model allow-list and type match else 422, URI rules, input prefix non-empty, duplicate -> 409, response 202) and `GET /api/v1/tasks/{team}/{project}/{task_name}`.
- Tests: slicing unit tests (boundaries, CRLF, no trailing newline, N=1). Integration: split, then kill mid-way and resume, giving identical subtasks.

**B4 server: register, preference, acquire** (∥ B3)
- `repositories/acquisition.py`: candidate SQL per ARCH §3.2. It joins the task and filters on `engine = ANY(:engines)`, `model_memory_gb <= :mem`, `(allowed_pool IS NULL OR allowed_pool = :pool)`, `task.status IN (PENDING, RUNNING)` and `subtask.status='PENDING'`. Pass 1 applies the preference filter (NULL = wildcard). Order is `priority DESC, created_at, subtask_id`, LIMIT 16, with a light shuffle of the head. D5 handles each candidate.
- `services/acquisition.py` + `api/executor.py`: register (update host caps and budget, return id), `PUT preference`, and acquire (200 + presigned URLs | 204). Acquire also stores budget, profile and last_seen on the executor row.
- **Race test (AC#1):** 60 executors run `asyncio.gather` acquire against (a) 1 hot subtask and (b) 20 subtasks, with pool_size >= 60. Assert winners == min(N, 60), one acquirement row per subtask, `attempt == 1`, and distinct acquirement_ids. Run it at the service level and once over HTTP (ASGITransport).
- Further tests: memory, engine and pool filters, preference-before-FIFO.

**B5 server: complete / fail / release + status derivation**
- Ownership is checked on every route (D6).
- complete per D6. A `record_count` that differs from the subtask's gives 422.
- fail: retryable -> `PENDING` if `attempt < max_attempts`, else `FAILED`. Non-retryable -> `FAILED`. Inserts an attempt row with outcome FAILED and an error code (no payload).
- release -> `PENDING`, outcome RELEASED.
- `derive_task_status(counts)`: when all subtasks are terminal the task becomes SUCCEEDED (0 failures), PARTIAL (some succeeded) or FAILED, and `finished_at` is set.
- Integration tests: stale or foreign acquirement gives 409 on all three routes; a late complete after release gives 409 and the promoted key is untouched; promotion is verified in S3.

**C1 executor: runner and worker** (after A1, ∥ B*)
- `runners/base.py`: `Runner` Protocol with `load(model, budget)`, `run(batch) -> list[dict]`, `unload()`.
- `runners/fake.py`: embedding gives a deterministic vector from sha256(text); completion echoes. `{"fail": "oom"}` in a record raises, for fault tests.
- `worker/process.py`: `WorkerHandle` uses the spawn ctx and a Pipe. It keeps the model warm across same-model subtasks and kills it on switch. A crash raises `WorkerCrashed`.
- `capacity/profiles.py`: a static profile -> `Budget` (FR-51 table, 75 % cap; RAM via `os.sysconf`). The governor is M4.
- Unit tests: fake runner determinism, worker round-trip, a killed worker raises.

**C2 executor: control loop + CLI** (after A2, C1)
- `io.py`: ranged GET (`Range: bytes=start-(end-1)`) to a temp file, streaming PUT, sha256.
- `loop.py`: `ExecutorLoop(client, worker, budget, engines, backoff=(10, 30))`. Steps: register; then loop acquire (204 -> jittered sleep). Download, parse JSONL (bad JSON -> `fail(retryable=False, code="BAD_INPUT")`), run in batches of <= `max_batch_size`, write the temp JSONL, PUT, complete. WorkerCrashed -> `fail(retryable=True)`; 409 -> discard. Temp files are deleted (S-4). SIGTERM/stop -> release the current subtask if not completed.
- `main.py:11-20` `run`: `require_secure_url(--server)`, token from `$MINION_EXECUTOR_TOKEN`, a hidden `--runner fake` option.
- Unit tests with MockTransport (204 backoff, 409 discard, bad-input path).

**E1 e2e + docs** (last)
- `packages/minion-executor/tests/test_e2e.py` (integration). Uses Postgres + SeaweedFS fixtures; uvicorn runs in a background task on a free port with real settings. It uploads 2 JSONL objects (e.g. 1 234 records, `records_per_subtask=100`), issues a token, and runs 2 `ExecutorLoop`s with the fake runner until the task is SUCCEEDED. It asserts:
  - every `outputs/.../{id:06d}.jsonl` exists, with record ids == input ids and no duplicates
  - the attempt rows are SUCCEEDED
  - no acquirements remain
- docs-keeper:
  - ADR-0007 (D1)
  - SPEC §6: admin token issue, 409 body
  - SPEC §7: `token_expires_at`
  - ARCH §2.1: storage, admin
  - CLAUDE.md commands: `minion-server-admin`
  - ADR-0004 (byte-range slicing) now that it is implemented

## Invariants
1 the executor only uses REST and presigned URLs (C2 imports only `minion_common.client`). 2 D5, enforced by the race test. 3 the ownership check on every executor route, plus 409. 4 presigned only, the server never proxies bodies (the splitter reads input server-side, which is allowed: the server is not on the executor data path). 5 attempt-scoped keys + promotion inside the locked txn. 7 the fake runner runs only inside the worker subprocess. 6 the governor is M4; M1 only does admission by memory_gb.

## Risks / open questions
- Commit failure after CopyObject leaves the promoted object from a valid attempt. That is acceptable (it is identical content-wise per attempt semantics), but note it for NF-2. Holding the row lock across S3 calls is bounded by the S3 timeout (set 10 s).
- Should release burn an attempt? M1: release -> PENDING without a `max_attempts` check. Revisit in M2.
- sha256 is reported but not verified server-side (S3 has no cheap sha256 without `x-amz-checksum` on the PUT). M1 checks size + record_count. ADR candidate for M3.
- Counter updates on the hot `minion_task` row on every complete: fine for M1, measure in M6 (NF-1).

## Proposed sub-tasks (orchestrator decides; not run)
```
backlog task create "M1 common: API schemas + typed client" -p 2
backlog task create "M1 server: settings, migration 0002, auth, storage, models.yaml" -p 2
backlog task create "M1 server: task create + resumable splitter" -p 2
backlog task create "M1 server: register/preference/acquire + race test" -p 2
backlog task create "M1 server: complete/fail/release + promotion" -p 2
backlog task create "M1 executor: fake runner, worker subprocess, control loop" -p 2
backlog task create "M1 e2e test + ADR-0007 + docs sync" -p 2
```
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Security carry-forward from TASK-1 audit: (1) user/API-key table with hashed key + team mapping; Task.created_by → FK. (2) Executor tokens: rotation/expiry column, hmac.compare_digest, never logged. (3) Acquire enforces allowed_pool vs Executor.pool; every executor endpoint checks acquirement.executor_id == caller. (4) Attempt-scoped output keys; restrict input_uri/output_uri to configured bucket/prefixes (SSRF). (5) Grafana read-only DB role; no dev passwords in prod config. (6) Executor --server: require https except localhost; token in Keychain. (7) Prod S3 creds bucket-scoped, server-only.
<!-- SECTION:NOTES:END -->
