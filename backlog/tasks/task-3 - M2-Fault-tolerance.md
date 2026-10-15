---
id: TASK-3
title: 'M2: Fault tolerance'
status: Done
assignee: []
created_date: '2026-10-15 14:02'
updated_date: '2026-10-15 20:31'
labels:
  - milestone
  - M2
dependencies: []
priority: high
ordinal: 3000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Heartbeats, watchdog reclaim, release, cancel, stale-acquirement 409. ARCHITECTURE §3.4.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Complete-vs-reclaim race test passes
- [ ] #2 Killed executor's subtask is re-run by another executor
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
# TASK-3 plan: M2 fault tolerance

## Goal and AC mapping
A subtask whose executor dies, sleeps or loses the network is reclaimed and re-run by another executor. A late (zombie) owner can never corrupt results. Users can cancel a task, and running executors learn about it and stop.
- **AC#1: the complete-vs-reclaim race test passes.**
  - Covers FR-31, FR-32, FR-34, NF-2, ARCH §4 rows "Watchdog reclaims while owner completes" and "Two watchdog replicas".
  - Covers invariants 2, 3 and 5.
- **AC#2: a killed executor's subtask is re-run by another executor.**
  - Covers FR-14, FR-30, FR-31, G3 and NF-3.
  - Covers invariants 1, 6 (the Mac is never kept awake: reclaim is the answer to sleep) and 7.
- **Also in scope:**
  - FR-8: the cancel endpoint, with executors learning via `heartbeat.cancel` or a 409.
  - FR-17: release, plus a server-side release cap (carry-over 3).
  - A 409 STALE_ACQUIREMENT on every owner endpoint, including the new heartbeat.
  - S-2: the presigned output URL is refreshed via heartbeat.
  - Carry-overs: (1) budget = FAILED+EXPIRED and a release costs no budget; (2) the watchdog is the safety net when `complete` gives up after transport-only errors; (4) the heartbeat endpoint and its sender.
- **Out of scope:**
  - The `presence` endpoint, `paused_reason` semantics and the governor (M4).
  - The FR-17 30 s grace period (unchanged from M1).
  - `GET /tasks` list, the subtasks listing and `/executors` (M5).
  - Compose services for the server and watchdog (M5).
- **TASK-11 stays separate.** It is splitter-only (`services/splitter.py:143-189`, `repositories/tasks.py:166-173`). It matters only with more than one server replica and very large single objects. It shares no code path with heartbeats or reclaim, and it is not needed for either AC. Run it after TASK-3 (it is still labelled M2), or in parallel because the files do not overlap. It does not block closing M2.

## Touchpoints (current code)
- **Acquire** (`repositories/acquisition.py:130-194`)
  - Already inserts the heartbeat row at `:183`, so acquire counts as the first beat.
  - Increments `attempt` at `:177`. That is the reason reclaim does NOT increment it (ARCH §3.4).
- **Owner ops** (`repositories/completion.py`)
  - `lock_owned` at `:50-92`: `SELECT ... FOR UPDATE` on the acquirement row, then an executor check.
  - `Owned` at `:27-39`, `failed_attempt_count` at `:107-123` (budget = FAILED+EXPIRED), `finish_attempt` at `:126-167` (the acquirement delete cascades to the heartbeat, `db/models.py:238-240`).
  - `bump_counters` and `set_task_terminal` at `:170-200` (only from PENDING/RUNNING, so CANCELLED is never overwritten).
- **Services** (`services/completion.py`): complete `:43-83`, fail `:86-108`, release `:111-123`, `derive_task_status` `:20-26`.
- **Routes** (`api/executor.py`): owner routes at `:102-135`; no heartbeat route yet.
  - `api/tasks.py:150-163` has GET only; no cancel route.
- **Splitter fencing**: lease ops match `status='SPLITTING'` (`repositories/tasks.py:157-173, 228-259`), so a cancelled task makes the splitter abort quietly via `LeaseLostError`.
- **Schemas, client, watchdog stub**
  - `minion_common/schemas.py:131-163`: no heartbeat or cancel models.
  - `client.py:124-160`: no `heartbeat` or `cancel_task`.
  - `minion_watchdog/main.py:1-8` is a stub. `packages/minion-watchdog/pyproject.toml` already depends on minion-server, so the watchdog reuses its repositories, services and `db/session.py`.
- **Executor loop** (`loop.py`)
  - `_handle` `:228-263` waits on {work, stopper} only.
  - `_execute` `:312-349` uploads to `a.output_url` (`:341`).
  - `_run_batches` `:351-362`; `_Outcome` `:61-65`; the `Worker` protocol `:94-98`.
  - `WorkerHandle.kill()` (`worker/process.py:267-276`) sets `_closed` and never respawns, so it is only suitable for stop.
- **No migration needed.** These already exist:
  - `minion_executor_heartbeat(last_seen_at idx, progress jsonb)`.
  - `minion_executor.budget/profile/paused_reason`.
  - Outcomes EXPIRED and CANCELLED.
  - SubtaskStatus CANCELLED.
  - `ix_minion_task_queue(status, …)`.

## Decisions
**D1. Global lock order: acquirement -> heartbeat -> subtask -> task.** Every path takes row locks in this order, so cancel, reclaim and owner ops cannot deadlock:
- complete/fail/release: `lock_owned`, then cascade delete of the heartbeat, then the subtask UPDATE, then the task UPDATE. This is already the case.
- acquire: INSERT acquirement, then the subtask, then a new heartbeat row, then the task. This is already the case.
- heartbeat (new): lock the acquirement, then UPDATE the heartbeat row (and the executor row).
- reclaim (new): lock the acquirement, then delete it (cascading the heartbeat), then the subtask, then the task.
- cancel (new): task in txn1, subtasks in txn2. It never holds both, see D4.

Document this in ARCH §4.

**D2. Heartbeat endpoint** `POST /executor/subtasks/{id}/heartbeat`. One txn:
1. `lock_owned` (409 STALE_ACQUIREMENT with an identical body if the acquirement is gone or foreign).
2. UPDATE the heartbeat row: `last_seen_at=now(), progress`.
3. Record `budget/profile/paused_reason/last_seen_at` on the executor row.

Response: `{cancel: task_status == CANCELLED, output_url, expires_at}`.
- The output URL is a fresh presign of the same attempt key (S-2, ARCH §3.4 `refreshed_urls`), signed after commit.
- Only the output URL is refreshed. The input is read once at start.
- Taking the acquirement lock (instead of a bare UPDATE) makes heartbeat-vs-reclaim exclusive under D1.

**D3. Watchdog reclaim**: `services/reclaim.py`, so that budget and derivation logic sit next to fail/complete. It runs one short txn per stale acquirement:
1. Unlocked `SELECT acquirement_id FROM heartbeat WHERE last_seen_at < now() - timeout ORDER BY last_seen_at LIMIT batch` (uses the `last_seen_at` index).
2. Per id: `SELECT acquirement ... FOR UPDATE SKIP LOCKED`. No row means it is gone or an owner op is in flight (e.g. complete holding the lock during CopyObject), so skip it this round.
3. Re-check staleness with a NEW statement. Under READ COMMITTED it sees a heartbeat committed while we waited. If it is fresh, skip.
4. `finish_attempt(outcome=EXPIRED)`, then the budget check: `used = failed_attempt_count + 1`.
   - If `used >= max_attempts`, the subtask becomes FAILED with error code `HEARTBEAT_TIMEOUT`, bump `failed`, and derive the task status.
   - Otherwise it goes back to PENDING (or CANCELLED, see D4).

Why this design:
- SKIP LOCKED keeps one slow complete from stalling the watchdog loop.
- Two watchdog replicas are safe: SKIP LOCKED, plus the UNIQUE `subtask_attempt.acquirement_id`.
- `attempt` is not touched; the next acquire bumps it. SPEC FR-31's "increments attempt" wording gets fixed.
- No app-level locks. Ownership is still decided only by the acquire INSERT ON CONFLICT (invariant 2 untouched).

**D4. Cancel is lazy and two-phase.** `POST /tasks/{team}/{project}/{task_name}/cancel` (user key, team check):
- **txn1**: `UPDATE task SET status=CANCELLED, finished_at=now(), next_split_at=NULL WHERE key AND status IN (SPLITTING,PENDING,RUNNING) RETURNING`. Commit.
- **txn2**: `UPDATE subtask SET status=CANCELLED WHERE task key AND status='PENDING'`.
- Two txns because acquire locks subtask then task. One txn (task, then subtasks) would deadlock against a concurrent acquire.
- RUNNING subtasks keep their acquirement. The owner learns through `heartbeat.cancel=true` (FR-8, FR-14) and calls `release`.
- **Owner ops on a cancelled task** (`Owned.task_status` is read in `lock_owned`):
  - release: outcome CANCELLED, subtask CANCELLED.
  - fail: outcome FAILED, subtask CANCELLED, no `failed` bump.
  - reclaim: outcome EXPIRED, subtask CANCELLED.
  - complete: accepted as normal (SUCCEEDED and promoted; work is not wasted). `set_task_terminal` cannot overwrite CANCELLED.
- **Healing races.** A requeue that read the task status just before txn1 committed can leave a PENDING subtask under a CANCELLED task. Such rows are never acquired: the candidate filter is at `repositories/acquisition.py:89`. The watchdog runs `sweep_cancelled` each tick: PENDING to CANCELLED for tasks cancelled in the last hour, driven by the task status index. So NF-3 holds, and counts are consistent within one watchdog interval.
- **Responses**:
  - 200 with TaskOut.
  - Re-cancel returns 200 (idempotent).
  - A task that is already SUCCEEDED/FAILED/PARTIAL returns 409 `TASK_FINISHED`.
  - Unknown task: 404. Not in the team: 403.
- **SPLITTING**: the splitter's fenced lease ops fail and it aborts. txn1 waits on a splitter batch txn's task lock, so txn2 also cancels rows inserted by that batch.

**D5. Release cap** (carry-over 3). Setting `MINION_RELEASE_LIMIT` (default 10, `ge=0`).
- When a subtask already has at least that many RELEASED attempts, a further `release` is recorded as a retryable FAILED attempt (code `RELEASE_LIMIT`). It then consumes budget like `fail`, so a misclassified deterministic error cannot livelock the fleet.
- The response is still 204, so executor behaviour is unchanged.
- The budget definition stays FAILED+EXPIRED.

**D6. Executor heartbeater** (new `minion_executor/heartbeat.py`, per ARCH §2.2): one asyncio task per subtask, started at the top of `_handle` and cancelled in `finally`, so it never blocks stop.
- Interval: `heartbeat_interval_s` (an ExecutorLoop param, default 300, FR-14). The first beat comes after one interval.
- On transport error or 5xx: retry after `min(30 s, interval)`, jittered.
- Sends `{progress: {records_done, records_total}, budget}`.
- Keeps `latest_output_url`, which `_execute` uses at upload time.
- Returns only on a signal:
  - **CANCEL**: cancel the work coroutine, then `release`. The server records CANCELLED. New `_Outcome.CANCELLED`, with no failure backoff.
  - **STALE** (409): cancel the work coroutine, report nothing (`DISCARDED`).
- Signals are acted on only during the work phase. During `_complete` retries the heartbeater keeps the lease alive and its signals are ignored: complete is accepted on cancelled tasks, and complete itself sees the 409.
- After `_complete` gives up on transport-only errors, the heartbeater stops, so the watchdog reclaims and counts EXPIRED (carry-over 2).
- Cancel and stale do NOT kill the worker: the model stays warm, and the in-flight batch finishes in its thread with the result dropped. The worker lock serialises the next subtask behind it, bounded by the 600 s run timeout. SIGTERM keeps the M1 kill path. A `WorkerHandle.interrupt()` (kill without `_closed`) is the alternative if the reviewer wants an immediate GPU release; M4 owns that.

**D7. Watchdog process** (`minion_watchdog`):
- `WatchdogSettings` (pydantic-settings, `MINION_` prefix):
  - `database_url` (required; no S3 settings needed: reclaim never touches S3, and EXPIRED attempt objects stay as unpromoted garbage).
  - `heartbeat_timeout_s=900`, `watchdog_interval_s=60`, `watchdog_batch_size=500`, `db_pool_size=2`.
- `run_watchdog(sessionmaker, settings, stop: asyncio.Event)` runs each tick: `reclaim_stale`, then `sweep_cancelled`, then logs counts (ids only).
- Errors are logged and retried on the next tick, as `run_splitter` does (`services/splitter.py:268-283`).
- SIGTERM/SIGINT set `stop`. The engine comes from `minion_server.db.session.create_engine`.

**D8. ADR-0008 "Fault-tolerance semantics"**, needed because these points deviate from or extend the paper:
- reclaim uses SKIP LOCKED plus a re-check (paper: a bare `DELETE…RETURNING`);
- `attempt` is bumped at acquire, not at reclaim;
- lazy two-phase cancel plus the sweep;
- the release cap;
- the D1 lock order.

## Steps (each ≤ ~300 changed lines). Order: S1 -> S2 -> S3 -> (S4 ∥ S5 ∥ S6) -> S7

**S1 common: schemas and client** (`minion_common/schemas.py`, `client.py`, tests)
- `Progress{records_done:int>=0, records_total:int>=0}`.
- `HeartbeatRequest{progress: Progress|None=None, budget: Budget|None=None, paused_reason: str|None (max 64, ^[a-z_]+$)}`. Bounded, with no free-form dicts (S-5).
- `HeartbeatResponse{cancel: bool, output_url: str, expires_at: datetime}`.
- `ExecutorClient.heartbeat(acquirement_id, req) -> HeartbeatResponse`.
- `UserClient.cancel_task(team, project, task_name) -> TaskOut`. Factor the slug-checked path builder out of `get_task` (`client.py:153-160`).
- Unit tests: validation, plus MockTransport for 200, 409 (`StaleAcquirementError`) and 409 `TASK_FINISHED` (`ApiError`).

**S2 server: heartbeat endpoint**
- `repositories/completion.py`:
  - `Owned.task_status` (select `Task.status` in `lock_owned:63-79`).
  - Extract `_owned(session, row)` so S3 can reuse it.
- New `repositories/heartbeats.py`: `beat(session, acquirement_id, progress: dict|None) -> None`.
- `repositories/executors.py`: `report_state(session, executor_id, *, profile, budget, paused_reason)`. It also sets `last_seen_at` and skips REVOKED, like `touch :110-126`.
- New `services/heartbeat.py`: `heartbeat(session, principal, acquirement_id, req, *, settings, storage) -> HeartbeatResponse` (D2). Presign via `attempt_key` (`services/acquisition.py:24`), TTL `min(presign_ttl_s, 3600)`.
- `api/executor.py`: the route returns 200 with HeartbeatResponse.
- Integration tests (`tests/test_heartbeat.py`, using `seed_task`/`make_executors` from `conftest.py:184-252`):
  - `last_seen_at` advances and progress is stored;
  - budget is recorded on the executor;
  - 409 with an identical body for unknown, foreign, after-complete and after-release acquirements;
  - `cancel=false`, then `true` after the task is set CANCELLED;
  - the refreshed `output_url` PUT works against SeaweedFS and is scoped to the attempt key.

**S3 server: reclaim service, budget, release cap, cancelled requeue**
- New `repositories/reclaim.py`:
  - `stale_ids(session, timeout_s, limit) -> list[UUID]`
  - `lock_for_reclaim(session, acquirement_id) -> Owned|None` (FOR UPDATE SKIP LOCKED, no executor check)
  - `heartbeat_stale(session, acquirement_id, timeout_s) -> bool`
- `repositories/completion.py`: `released_attempt_count(session, o)`.
- `services/completion.py`:
  - Pure `next_status(used, max_attempts, retryable, task_cancelled) -> SubtaskStatus`.
  - A shared `_finish_failed(session, o, outcome, error, retryable)` used by fail, reclaim and release-over-cap.
  - release applies D5 and the cancelled rule (D4). `api/executor.py` release gets the settings dependency.
- New `services/reclaim.py`:
  - `ReclaimResult` enum: EXPIRED_REQUEUED | EXPIRED_FAILED | EXPIRED_CANCELLED | SKIPPED | NOT_STALE.
  - `reclaim_one(session, acquirement_id, *, timeout_s)` runs inside a caller-owned txn, so tests can hold it open.
  - `reclaim_stale(sm, *, timeout_s, batch_size) -> Counter[ReclaimResult]`.
- `settings.py`: `release_limit`.
- Unit tests: `next_status` table.
- Integration tests (`tests/test_reclaim_races.py`), **AC#1**:
  - (a) **Randomized complete vs reclaim.** 16 subtasks are acquired by 16 executors, attempt outputs are PUT, and heartbeats are backdated by 1 h. Then `gather(HTTP complete, reclaim_one)` per subtask, plus a final `reclaim_stale`. Per subtask, check exactly one attempt row and no acquirement, and either:
    - 204, SUCCEEDED, promoted, or
    - 409, EXPIRED, PENDING, final key absent, attempt object not promoted.

    Then check counters are exact and that no 5xx/deadlock occurred.
  - (b) **Deterministic complete-first.** Monkeypatch `storage.copy` to wait on an event. While complete holds the lock, `reclaim_one` returns SKIPPED. Release the event, complete returns 204, and a later reclaim is a no-op.
  - (c) **Deterministic reclaim-first.** The test holds the `reclaim_one` txn open. Complete blocks (the task is not done after 0.2 s). Commit, and complete returns 409 with nothing promoted.
  - (d) **Heartbeat vs reclaim, gathered.** Never both heartbeat 200 and EXPIRED in the same race, and a fresh heartbeat gives NOT_STALE.
  - (e) **Two `reclaim_stale` concurrently** over 30 stale rows: exactly 30 EXPIRED rows.
  - (f) **Budget.** `max_attempts=3`: retryable FAIL, RELEASE, EXPIRE, EXPIRE ends FAILED on the third budget event. The task becomes FAILED/PARTIAL with `finished_at` set. RELEASED is not counted.
  - (g) **Release cap.** The (limit+1)th release is recorded FAILED `RELEASE_LIMIT`, and the subtask ends FAILED after budget.
  - (h) **Zombie.** After reclaim, the old owner's heartbeat, complete, fail and release all return 409 with an identical body. A re-acquire gets attempt 2. A late PUT to the attempt-1 key is never promoted.

**S4 server: cancel**
- `repositories/tasks.py`:
  - `cancel_task(session, team, project, name) -> TaskStatus|None` (txn1)
  - `cancel_pending_subtasks(...) -> int` (txn2)
  - `sweep_cancelled(session, window_s=3600) -> int`
- `api/tasks.py`: the `POST /{team}/{project}/{task_name}/cancel` route (slug and team checks as in `get_task:150-163`), with a thin service in the route or `services/tasks.py`.
- Integration tests (`tests/test_cancel.py`):
  - **Cancel PENDING**: all subtasks CANCELLED, acquire gives 204.
  - **Cancel RUNNING with an owner**:
    - heartbeat returns `cancel=true`;
    - release gives attempt CANCELLED and subtask CANCELLED;
    - fail on a cancelled task gives subtask CANCELLED with the counter untouched;
    - complete after cancel gives SUCCEEDED and the task stays CANCELLED;
    - reclaim gives EXPIRED and CANCELLED.
  - **Cancel during split**: drive `split_one` with a delayed `iter_object`. Expect lease lost, and no PENDING left after cancel plus sweep.
  - **Responses**: re-cancel 200, finished 409 `TASK_FINISHED`, 403, 404.
  - **Sweep** heals an injected PENDING-under-CANCELLED row.
  - **gather(20 acquires + cancel)**: no 5xx, no PENDING left after the sweep.

**S5 watchdog process**
- `minion_watchdog/settings.py` and `minion_watchdog/main.py` per D7, with structlog JSON.
- Tests (`packages/minion-watchdog/tests/`):
  - unit: settings defaults and validation;
  - integration: `run_watchdog` with interval 0.05 and timeout 0.5 reclaims seeded stale acquirements and sweeps;
  - `stop` exits within 1 s;
  - a DB error in one tick does not kill the loop (monkeypatched `reclaim_stale` raises once).

**S6 executor heartbeats** (`heartbeat.py`, `loop.py`)
- `Heartbeater(client, acquirement_id, *, interval_s, budget, total, initial_output_url, pause_fn)`. It exposes `records_done`, `output_url` and `async run() -> HeartbeatSignal`.
- In `loop.py`:
  - `_handle` waits on {work, stopper, hb};
  - the D6 outcome mapping;
  - `_execute` reads `hb.output_url` at upload;
  - `_run_batches` updates `records_done`;
  - new `_Outcome.CANCELLED`;
  - `heartbeat_interval_s` param.
- Unit tests (FakeClient gains a `heartbeat` method, `tests/test_loop.py:57-80`):
  - beats at the interval with a fake sleep;
  - CANCEL gives release, no fail, the worker is not killed, and the loop continues;
  - 409 gives no report;
  - a transport error retries and the work is unaffected;
  - stop mid-beat exits promptly;
  - the refreshed URL is used for the PUT;
  - no beats after the subtask ends.

**S7 fault e2e and docs**
- **AC#2 test.** In `tests/test_e2e.py`, reuse the `live` fixture `:89-116` and the subprocess pattern `:231-276`:
  - Run executor A as a `minion-executor run --runner fake` subprocess with `TEST_HOOKS=1`, on a one-subtask task whose first record has `sleep_s: 60`.
  - Wait for the acquirement, then SIGKILL A. Its worker exits via parent-watch.
  - Then start an in-process `run_watchdog` (timeout 1 s, interval 0.2 s) and executor B, an in-process `ExecutorLoop` with `WorkerHandle("fake")`. B has no hooks, so it ignores `sleep_s`, and its `heartbeat_interval_s` is 0.2.
  - Assert:
    - attempts are [EXPIRED by A attempt 1, SUCCEEDED by B attempt 2];
    - the task is SUCCEEDED;
    - all record ids appear exactly once in the final outputs;
    - the acquirement table is empty;
    - A's return code is -9.
- **Cancel e2e**: an in-process loop with hooks, a short sleep and a 0.2 s heartbeat. `UserClient.cancel_task` leads to a released attempt, and attempt, subtask and task are all CANCELLED. The loop keeps polling.
- **Docs** (docs-keeper):
  - **SPEC**:
    - FR-8 lazy cancel and complete-after-cancel;
    - FR-14 implemented;
    - FR-31 attempt wording;
    - FR-32 release cap;
    - §5 SPLITTING->CANCELLED;
    - §6 heartbeat body and response, cancel codes and `TASK_FINISHED`, status lines;
    - §4.2 status note.
  - **ARCH**:
    - §2.2 heartbeat block;
    - §2.3 watchdog as built;
    - §3.4 pseudocode (SKIP LOCKED, re-check, sweep);
    - §4 lock order plus rows for heartbeat-vs-reclaim and cancel-vs-requeue;
    - §9 ADR-0008.
  - ADR-0008, `.env.example` (`MINION_RELEASE_LIMIT`, `MINION_HEARTBEAT_TIMEOUT_S`, `MINION_WATCHDOG_INTERVAL_S`, `MINION_WATCHDOG_BATCH_SIZE`), and CLAUDE.md commands (`minion-watchdog` is now real).

## Invariants
1. **Executors never talk to the DB.** The executor only adds client calls. The watchdog uses `minion_server` repositories, which is allowed.
2. **Ownership comes only from the acquire INSERT ON CONFLICT.** Reclaim only deletes the row, under a row lock.
3. **Owner-only mutation.** Heartbeat joins the 409 matrix with an identical body.
4. **No payloads through the server.** URL refresh is a presign only.
5. **Idempotent outputs.** Reclaimed attempts stay at attempt-scoped keys and promotion happens only through an accepted complete. Tests (a), (c) and (h) cover this.
6. **Yield to the user.** The heartbeater never blocks stop, and there is no sleep prevention.
7. **Inference only in the worker.** Unchanged.

## Risks
- **Lock order regressions cause deadlock 500s.** D1 is documented, and tests (a)/(d)/S4 assert no 5xx under concurrency.
- **Timeout vs interval misconfiguration** (`heartbeat_timeout_s < ~3×` the executor interval) causes mass reclaim. Document this. The defaults are 900/300.
- **`presign_ttl_s` below the heartbeat interval** can let the upload URL expire. Document that TTL must exceed the interval (defaults 900 > 300).
- **Cancel latency** is up to one heartbeat interval (5 min) of wasted work per running subtask (FR-8 accepts "next heartbeat").
- **Counts can show PENDING under CANCELLED** for up to one watchdog interval (healed by the sweep).
- **Abandoned batch.** After cancel or stale, an abandoned batch holds the worker for up to one batch (worst case the 600 s run timeout).
- **Release cap vs M4 churn.** Under heavy M4 budget-downgrade churn, the release cap could turn legitimate releases into failures. It is configurable; revisit in M4.
- **No watchdog in compose**, so local dev has no reclaim unless `uv run minion-watchdog` is started (documented in CLAUDE.md).

## Open questions
1. Complete after cancel: accept it (plan) or reject it with 409 `TASK_CANCELLED`?
2. Cancel response: 200 (plan) or 202?
3. Release cap: default 10, converted to FAILED `RELEASE_LIMIT` (plan)?
4. Refresh the output URL via heartbeat in M2 (plan, because `extra="forbid"` makes later schema additions breaking) or defer it to M3?
5. On cancel or stale: abandon without killing the worker (plan) or add `interrupt()`?

## Proposed sub-tasks (orchestrator decides; not created)
- `backlog task create "M2.1: Heartbeat endpoint, watchdog reclaim service, release cap" -p task-3 -l M2 --priority high -d "Plan steps S1-S3 of TASK-3 (D1-D3, D5)" --ac "Complete-vs-reclaim race test passes (randomized + both deterministic orderings)" --ac "Heartbeat-vs-reclaim and two-watchdog races safe; stale 409 on heartbeat/complete/fail/release" --ac "Budget = FAILED+EXPIRED, release free up to MINION_RELEASE_LIMIT"`
- `backlog task create "M2.2: Task cancel and watchdog process" -p task-3 -l M2 --priority high --dep task-3.1 -d "Plan steps S4-S5 of TASK-3 (D4, D7)" --ac "Cancel endpoint: pending subtasks CANCELLED, running owners get heartbeat cancel=true, owner ops finish as CANCELLED" --ac "minion-watchdog reclaims stale acquirements and sweeps cancelled tasks; stops cleanly on SIGTERM"`
- `backlog task create "M2.3: Executor heartbeats, cancel/stale handling, fault e2e, docs" -p task-3 -l M2 --priority high --dep task-3.1 -d "Plan steps S6-S7 of TASK-3 (D6, D8)" --ac "Killed executor's subtask is re-run by another executor (e2e)" --ac "Executor stops work on heartbeat cancel/409 and keeps polling" --ac "ADR-0008 written; SPEC/ARCH match code"`

S6 can start once S2 has landed, in parallel with S4 and S5. The S7 e2e needs S5.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Carry-over from M1 (TASK-2.x reviews): (1) watchdog must count EXPIRED attempts toward max_attempts (budget = FAILED+EXPIRED; release does not consume it, ARCH §3.4 / FR-32); (2) executor leaves subtask RUNNING when complete retries exhaust on transport errors only — watchdog reclaim is the safety net; (3) add server-side per-subtask release cap so repeated releases (e.g. proxy/S3 gateway resetting one object, RemoteProtocolError) cannot livelock a subtask; (4) heartbeat endpoint not implemented yet (SPEC §6 marks it); executor must start sending heartbeats; (5) TASK-11 lease renewal relevant for multi-replica.

Orchestrator decisions on plan open questions (2026-10-15): (1) complete after cancel is ACCEPTED (work done, harmless); (2) cancel returns 200 (synchronous phase 1); (3) release cap MINION_RELEASE_LIMIT=10, overflow counts as retryable FAILED attempt; (4) heartbeat refreshes output_url NOW (schemas are extra=forbid, adding later breaks executors); (5) on cancel/409 the executor KILLS the worker via new WorkerHandle.interrupt() that allows respawn (invariant 6: no GPU on cancelled work), not abandon-and-continue.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
M2 fault tolerance: heartbeat endpoint, reclaim service and retry budget (TASK-3.1); cancel endpoint, minion-watchdog process, max lease and heartbeat 429 (TASK-3.2); executor heartbeater, cancel/409 worker interrupt and fault e2e (TASK-3.3). Reclaim uses SKIP LOCKED + re-check, a deviation from the paper's DELETE RETURNING (ADR-0008). Open: FR-17 grace period; test_heartbeat presigned-PUT timeout in isolation (TASK-3.3 notes).
<!-- SECTION:FINAL_SUMMARY:END -->
