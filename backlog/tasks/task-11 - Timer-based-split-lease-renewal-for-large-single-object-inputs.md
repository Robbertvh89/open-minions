---
id: TASK-11
title: Timer-based split lease renewal for large single-object inputs
status: In Progress
assignee: []
created_date: '2026-10-15 16:51'
updated_date: '2026-10-20 04:37'
labels:
  - M2
dependencies: []
ordinal: 14000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
From TASK-2.2 review (MINOR): split lease is renewed and wall-clock budget checked only per 1000-subtask batch. Listing plus one large batch (single object up to 50 GiB) can exceed lease_s (900s); with >1 server replica another splitter claims, first gets LeaseLostError, repeats can end a valid task as SPLIT_ATTEMPTS_EXHAUSTED. Renew + check deadline on a timer (e.g. every lease_s/3) inside the read loop (smaller to_thread reads or background renew task). Also: note in ARCHITECTURE that If-Match must be honoured by the S3 backend (same-size replacement otherwise undetected) or compare GET ETag to listed ETag.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 Lease renewed at least every lease_s/3 during a single long object read
- [x] #2 Test: lease_s shorter than one batch's read time still completes with 2 concurrent splitters
- [x] #3 GET response ETag compared to listed ETag
<!-- AC:END -->



## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
# TASK-11 plan: timer-based split lease renewal for large single-object inputs

Worktree `/Users/robbertvanhintum/Documents/claude_code/open-minions-task11`, branch `fix/task-11-split-lease-timer`.
Paths below are relative to that worktree.

## 1. Goal and acceptance criteria

Today the split lease (`next_split_at`, fenced by `split_attempts`) is renewed, and the wall-clock budget checked,
only once per 1000-subtask batch: inside `_insert_batch` (`services/splitter.py:154-156`) and in
`_split` (`services/splitter.py:180-182`). One batch is produced by a single blocking
`asyncio.to_thread(_take, slices, batch_size)` (`services/splitter.py:115`). For a large object
(`records_per_subtask` up to 100 000, lines up to 16 MiB, objects up to 50 GiB) that one call can take
longer than `lease_s` (900 s). On a multi-replica deployment (NF-5) a second splitter then claims the task
(`split_attempts` +1), the first one gets `LeaseLostError` at its next insert, the second one hits the same wall, and
after `split_max_attempts` claims a valid task ends as `SPLIT_ATTEMPTS_EXHAUSTED`
(`repositories/tasks.py:228-241`).

| AC | In my words | SPEC / ARCH |
|---|---|---|
| #1 | While a split attempt holds its claim, the lease is renewed on a timer at least every `lease_s/3`, also while one long object read is blocking a worker thread. The wall-clock budget is checked on the same timer. | FR-3 (async, resumable split), NF-5 (several replicas), SPEC §7 error list (`SPLIT_ATTEMPTS_EXHAUSTED`, lines 379-383), ARCH §3.1 (lines 329-348) |
| #2 | A test where one batch's read takes longer than `lease_s`, with a second splitter polling the whole time: the task completes correctly on the first claim (`split_attempts == 1`, `PENDING`, no `SPLIT_ATTEMPTS_EXHAUSTED`, rows equal the pure slicer). | FR-3, FR-5 (byte-range slices), NF-2 spirit (one writer) |
| #3 | `iter_object` compares the GET response `ETag` with the listed ETag. A mismatch is an `INPUT_CHANGED` failure, the same as an `If-Match` 412. ARCHITECTURE gains a note that the S3 backend must honour `If-Match`. | FR-5, ADR-0004 (inputs immutable), SPEC `INPUT_CHANGED` (line 381-382), ARCH §3.1 line 332 |

Found while planning (in scope because it is the same failure mode, see Risk R1): **cancelling the splitter during a
read turns `CancelledError` into `ValueError("generator already executing")`.** I reproduced this with a pure-Python
model of `_plan_batches`. `finally: slices.close()` (`services/splitter.py:137-138`) runs while the worker thread is
still inside the generator. Effects: `split_one` treats it as transient (`services/splitter.py:226-227`), so
`_retry` burns one split attempt. `run_splitter` swallows the cancellation and keeps looping
(`services/splitter.py:275-283`), so `app.py:148-149` (`splitter.cancel(); await gather(...)`) does not return.
Every rolling restart during a long split therefore uses up one attempt, which is a second road to
`SPLIT_ATTEMPTS_EXHAUSTED`.

## 2. Touchpoints

- `packages/minion-server/src/minion_server/services/splitter.py`
  - `:1-16` module docstring ("lease renewal per batch"), to be updated.
  - `:46-70` `SplitLimits` (`lease_s`, `max_seconds`). No new field.
  - `:77-82` `LeaseLostError`, `SplitTimeoutError`. Reused.
  - `:88-89` `_take`. `:92-140` `_plan_batches`: `iter_object(...)` at `:108-113`, `to_thread` at `:115`,
    `PreconditionFailedError -> INPUT_CHANGED` at `:131-132`, `finally slices.close()` at `:137-138`.
  - `:143-166` `_insert_batch`: in-transaction fenced renew at `:154-156`. Keep it, it is the fence for the insert.
  - `:169-189` `_split`: deadline at `:177`/`:180-181`.
  - `:196-235` `split_one` (exception mapping), `:268-283` `run_splitter`.
- `packages/minion-server/src/minion_server/repositories/tasks.py:264-271` `renew_split_lease`: unchanged, reused by
  the timer. `:255-261` `_held` (status SPLITTING and same `split_attempts`), `:202-252` `claim_splitting`.
- `packages/minion-server/src/minion_server/storage.py:198-229` `iter_object`: `IfMatch` at `:212`, `get_object`
  at `:214`, size guard at `:223-227`. `:109-120` `_list_keys` (listed ETag at `:116`).
- `packages/minion-server/src/minion_server/settings.py:145-146` comments on `split_lease_s` / `split_max_seconds`.
- `packages/minion-server/src/minion_server/app.py:136-149` splitter lifecycle (cancel + gather). No change, but the
  regression test exercises this contract.
- Tests: `packages/minion-server/tests/test_tasks_api.py` has the fixtures `storage` (`:274`) and `fresh_engine`, the
  helpers `_put`, `_jsonl`, `_insert_task`, `_task_row`, `_subtasks`, and `EXPIRE`/`BUMP` (`:627-628`).
  Existing lease tests: `test_renewal_keeps_lease_across_slow_split` `:685`, `test_lost_lease_aborts_quietly` `:723`,
  `test_split_wall_clock_budget_backs_off` `:748`, `test_object_replaced_after_listing_is_input_changed` `:779`,
  `test_iter_object_guards` (~`:798`). Concurrent splitters: `test_protocol_races.py:524`.
- Docs: `docs/ARCHITECTURE.md:329-348` (§3.1). `docs/SPEC.md:379-383` needs no change (INPUT_CHANGED already covers
  "an object changed while being read").

## 3. Design (simplest option that meets AC#1)

**Option chosen: a background asyncio renewer task plus a cross-thread abort flag checked once per S3 chunk.**
Rejected alternative: smaller or time-bounded `_take` calls. One slice can be `records_per_subtask (<=100k) x 16 MiB`
long, so no `_take` granularity can bound the time between renewals. The renewer runs on the event loop, which stays
free while the read blocks in `to_thread`. That makes renewal independent of slice and batch size.

### 3.1 `_SplitGuard` (new, private, `services/splitter.py`)
```python
class _SplitAborted(Exception):
    """Internal: the read thread was asked to stop (shutdown); never escapes `_plan_batches`."""


class _SplitGuard:
    """Abort flag for one split attempt, set on the event loop, checked by the read thread per chunk."""

    def __init__(self) -> None: ...  # threading.Event + reason: type[Exception]
    def trip(
        self, reason: type[Exception]
    ) -> None: ...  # first reason wins; set reason before event
    def check(self) -> None: ...  # raise reason() if tripped
    def chunks(
        self, it: Iterator[bytes]
    ) -> Iterator[bytes]: ...  # for c in it: self.check(); yield c
```
`reason` is `LeaseLostError`, `SplitTimeoutError` or `_SplitAborted`.

### 3.2 Renewer (new, `services/splitter.py`)
```python
async def _renew_lease(
    sessionmaker: async_sessionmaker[AsyncSession],
    task: Task,
    limits: SplitLimits,
    deadline: float,
    guard: _SplitGuard,
) -> None:
    interval = limits.lease_s / 3
    while True:
        await asyncio.sleep(max(0.0, min(interval, deadline - time.monotonic())))
        if time.monotonic() > deadline:
            guard.trip(SplitTimeoutError)
            return
        try:
            async with sessionmaker() as s, s.begin():  # own short tx, one UPDATE
                held = await tasks_repo.renew_split_lease(s, task, limits.lease_s)
        except Exception as exc:  # DB blip: fencing keeps us safe, retry next tick
            log.warning("task_split_renew_error", task=tid_of(task), error_class=type(exc).__name__)
            continue
        if not held:  # re-claimed or cancelled (status != SPLITTING)
            guard.trip(LeaseLostError)
            return
```
- **Session and transactions:** each tick opens its own session from the same sessionmaker and runs one fenced
  `UPDATE` in a short transaction. No transaction is ever held across an S3 read. The main path holds none during
  reads either: `_insert_batch` runs between reads, as today. The renewer can wait briefly on the task row lock held
  by a concurrent `_insert_batch` transaction (same row, one resource, so no deadlock). That wait is bounded by one
  1000-row insert. Rival claimers use `SKIP LOCKED`, so the brief lock does not block them. At peak the splitter
  uses 2 pool connections instead of 1. Pool exhaustion surfaces as the 5 s pool timeout, which is logged and
  retried on the next tick.
- **Why a renew error does not abort:** every holder write is fenced (`_held`). If renewals keep failing and the
  lease expires, a rival claims and our next fenced write raises `LeaseLostError`. That is safe. Aborting on a DB
  blip would throw away a 50 GiB scan and burn an attempt.
- The in-transaction renew in `_insert_batch` (`:154-156`) **stays**. It is the fence that keeps a stale holder from
  inserting, so it must not be removed.

### 3.3 Wiring in `_split` / `_plan_batches`
- `_split` creates `guard = _SplitGuard()` and starts `renewer = asyncio.create_task(_renew_lease(...))` **before**
  listing, so the listing is covered too. In `finally`: `renewer.cancel()`, then await it while suppressing only the
  renewer's own `CancelledError`. If the current task is itself being cancelled (`asyncio.current_task().cancelling()`),
  re-raise, so an outer cancel is never swallowed. Per batch: call `guard.check()` before `_insert_batch` and keep the
  existing `time.monotonic() > deadline` check (cheap, and keeps `test_split_wall_clock_budget_backs_off` exact).
- `_plan_batches(storage, task, limits, batch_size, guard)` gets a new parameter:
  - `raw = storage.iter_object(key, etag=etag, size=size)`, then
    `slices = slice_jsonl(guard.chunks(raw), 0, rpp, max_line)`.
  - Replace the bare `await asyncio.to_thread(_take, ...)` with a cancellation-safe helper:
    ```python
    async def _take_in_thread(slices: Iterator[Slice], n: int, guard: _SplitGuard) -> list[Slice]:
        fut = asyncio.ensure_future(asyncio.to_thread(_take, slices, n))
        try:
            return await asyncio.shield(fut)
        except asyncio.CancelledError:
            guard.trip(_SplitAborted)  # thread stops at its next chunk
            await asyncio.wait(
                [fut], timeout=STOP_WAIT_S
            )  # STOP_WAIT_S = 15.0 (> boto read_timeout 10 s)
            raise
    ```
    Then `guard.check()` after each returned chunk list.
  - `finally`: call `slices.close()` (and `raw.close()`) **only if no thread is still running the generator**. Track
    the in-flight future and close only when it is `None` or `done()`. Otherwise leave the generator to the GC (the
    thread exits by itself at the next chunk). This removes the `ValueError` that replaced `CancelledError`.
  - Change the return type of `iter_object` from `Iterator[bytes]` to `Generator[bytes, None, None]` so `raw.close()`
    type-checks (`mypy --strict`).
  - `LeaseLostError` and `SplitTimeoutError` raised in the thread propagate unchanged through `_plan_batches` (its
    `except` clauses only map `PreconditionFailedError` / `LineTooLongError`) to the existing handlers in `split_one`
    (`:211-212` quiet abort, `:226-227` backoff). `_SplitAborted` only appears together with a pending
    `CancelledError`, so it never reaches `split_one`.
- **Cancel by a user (`cancel_task` sets CANCELLED):** the renew `UPDATE` no longer matches `_held` (status), so the
  read now stops within `lease_s/3` instead of after the batch. The task stays CANCELLED. Log event:
  `task_split_lease_lost`, which is the existing behaviour.
- Stop latency after a trip is at most one 1 MiB chunk (or one boto read timeout of 10 s on a stalled socket).

### 3.4 AC#3: GET ETag check (`storage.py:iter_object`)
- After `get_object`, if `etag` was given: compare `_norm_etag(resp.get("ETag", ""))` with `_norm_etag(etag)`.
  `_norm_etag` strips whitespace, a leading `W/` and surrounding `"`. On a mismatch, or when the GET returned no
  ETag although one was listed (fail closed), close `body` and raise `PreconditionFailedError`. The existing mapping
  (`splitter.py:131-132`) makes that `FAILED` with `INPUT_CHANGED: input object changed while it was being split`.
  This is deterministic, with no retry, the same as a 412 today, and consistent with ADR-0004 ("inputs must be
  immutable"). No new log line in `iter_object` (it has no task context). `split_one` already logs
  `task_split_failed` with the error class only.
- If no ETag was listed (`etag` empty), behave as today (no pin, no comparison).
- Residual (document it, do not code it): one GET is a single object version on AWS S3 (strong consistency). A
  backend that streams a mix of versions within one GET is not supported.

### 3.5 Invariants
- **Inv. 4 (large payloads never pass through the server):** the splitter *does* already read every input object
  server-side (`_plan_batches` -> `iter_object`, a full streaming GET). This is by design: ADR-0004 and ARCH §3.1 make
  it a one-pass, server-side scan for newline offsets. Bytes are only scanned (memory bounded by `max_line_bytes` +
  batch), never forwarded to executors or the API, and never stored. This task adds no new data path; it only adds
  a stop flag. The invariant's intent is the API/executor data path. If the orchestrator wants it, CLAUDE.md could
  be worded "...never pass through the server API" (docs-only, not part of this task).
- **Inv. 2/3 (ownership / fencing):** the split lease is the splitter's analogue of ownership. All holder writes stay
  fenced on `split_attempts` (`_held`). The renewer only adds another fenced write. No check-then-act: losing is
  detected by `rowcount == 0` of the fenced `UPDATE`.
- **Inv. 5 (idempotent):** deterministic ids + `ON CONFLICT DO NOTHING` are unchanged. An aborted read inserts
  nothing past the last committed batch.
- **Inv. 1, 6, 7:** not touched (server-only change).
- Logging: error class and task id only, never payload or object key contents (S-5).

## 4. Steps

### Step 1: AC#3, GET ETag pinning (implementer, ~60 lines)
- `storage.py`: `_norm_etag(etag: str) -> str` (module-level), the ETag comparison in `iter_object`, and the
  return type `Generator[bytes, None, None]`.
- Tests:
  - unit `packages/minion-server/tests/test_storage.py` (no network): build `S3Storage` from settings (client creation
    is local), monkeypatch `st._client.get_object` to return `{"ETag": '"b"', "Body": FakeBody}`.
    `iter_object(..., etag='"a"')` raises `PreconditionFailedError` and `FakeBody.closed` is set. Matching ETags with
    different quoting (`'a'` vs `'"a"'`, `W/"a"`) pass. A missing GET ETag with a listed etag raises.
    `etag=None` skips the check.
  - integration: extend `test_iter_object_guards` (`test_tasks_api.py` ~`:798`) with a same-size-replacement case.
    Monkeypatch `get_object` on `storage._client` to drop `IfMatch` (simulating a backend that ignores it), replace
    the object with same-size different bytes after listing, and expect `PreconditionFailedError`. Optional: a
    SeaweedFS multipart object (`upload_fileobj`, 5 MiB part threshold, ~11 MiB) whose listed ETag equals the GET
    ETag (`-N` suffix). This guards against a false `INPUT_CHANGED` (Risk R3).
- Invariants: inv. 4/5 unchanged.

### Step 2: AC#1, renewer + guard + cancellation-safe reads (implementer, ~150 lines)
- `services/splitter.py`: `_SplitAborted`, `_SplitGuard`, `_renew_lease`, `_take_in_thread`, `STOP_WAIT_S`, the new
  `_plan_batches(..., guard)` parameter, renewer lifecycle in `_split`, docstring `:13-14` updated.
  No public API change (`split_one` / `run_splitter` signatures unchanged).
- Unit tests, new file `packages/minion-server/tests/test_split_guard_unit.py` (no DB):
  - `_SplitGuard`: first reason wins; `chunks()` raises the reason at the next chunk after `trip`; `check()` is a
    no-op until tripped.
  - `_renew_lease` with a fake sessionmaker (async context manager whose `begin()` is one too) and monkeypatched
    `tasks_repo.renew_split_lease`: (a) returns True 3x and is then cancelled, so it was called with ~`lease_s/3`
    spacing (`lease_s=0.3`); (b) returns False, so the guard trips `LeaseLostError` and the coroutine returns;
    (c) raises `OSError` once and then True, so there is no trip and a warning is logged; (d) deadline in the past,
    so `SplitTimeoutError` is tripped without calling renew.
  - `_take_in_thread`: a generator that sleeps 50 ms per item wrapped by `guard.chunks`. Cancelling the awaiting task
    raises `CancelledError` (not `ValueError`), and the generator is not executing afterwards (`gi_running` is False),
    so `close()` succeeds.
- Existing tests stay valid. `test_lost_lease_aborts_quietly` counts `renew_split_lease` calls (`n == 3`). With the
  default `lease_s=900` the timer never fires during that test, so the count is unchanged. Note this in a test
  comment if it is touched.

### Step 3: AC#2, deterministic integration tests (test-engineer, ~200 lines, `integration`)
Add to `packages/minion-server/tests/test_tasks_api.py` in the "lease fencing" block (reuses `fresh_engine`,
`storage`, `_put`, `_jsonl`, `_insert_task`, `_task_row`, `_subtasks`, `BUMP`). Shared helper `_slow_reads(monkeypatch,
delay_s, chunk=32)`: monkeypatch `S3Storage.iter_object` with a wrapper that calls the real one with `chunk_size=chunk`,
`time.sleep(delay_s)` before each yielded chunk, and counts chunks and closure in a dict. Input: 12 records,
`records_per_subtask=1`, `batch_size=100`, so the whole object is **one** `_take` call. Size ~360-400 B / 32 B is
~12 chunks; delay 0.25 s gives a ~3 s read. `lease_s=1.0` (renew every 0.33 s).
1. `test_long_single_read_keeps_lease_with_rival_splitter` (AC#2 + AC#1 evidence): splitter A =
   `create_task(split_one(sm, storage, lim, batch_size=100))`. After 0.2 s, rival B loops
   `claimed_b += await split_one(sm, storage, lim, batch_size=100); await asyncio.sleep(0.05)` until A finishes.
   Note: two plain `gather`ed loops would not test anything, because B returns False at once. Spy on
   `tasks_repo.renew_split_lease` to record `time.monotonic()`, plus the claim time. Assert: A returned True;
   `claimed_b == 0`; task `("PENDING", None, 1)` (status, error, split_attempts); `total_subtasks == 12`; rows equal
   `slice_jsonl(data, 0, 1, max)` (see `test_split_slices_match_pure_slicer` `:328`); at least 5 renewals before the
   first insert; max gap claim->renewal->renewal `< 0.7 * lease_s` (interval is lease/3 = 0.33 s; the slack absorbs CI
   jitter but still fails if renewals were per batch). **Red first:** confirm it fails on `main` (B claims at ~1 s,
   attempts becomes 2).
2. `test_lease_lost_mid_read_stops_read`: after A's claim, run `BUMP`. `split_one` returns True in `< 2 s` (full
   read ~3 s); chunks read `<` total; no subtasks; row `("SPLITTING", None, 2)` untouched.
3. `test_deadline_mid_read_backs_off`: `SplitLimits(lease_s=1.0, max_seconds=0.8, backoff_base_s=1000)`. Returns in
   `< 2 s`; row `("SPLITTING", None, 1)`; `next_split_at > now() + 500 s` (backoff, not lease); no subtasks.
4. `test_cancel_mid_read_is_clean`: `t = create_task(run_splitter(sm, storage, poll_s=3600, limits=lim))`, sleep 0.6 s,
   `t.cancel()`, `await asyncio.wait_for(asyncio.gather(t, return_exceptions=True), 5)`. The result is a
   `CancelledError` (not `ValueError`), so shutdown no longer hangs. Row `split_attempts == 1` and
   `next_split_at < now() + 10 s` (no `_retry` backoff written), and the slow generator was closed or stopped. Then the
   lease expires (`EXPIRE`) and a fresh `split_one` completes the task (`PENDING`, attempts 2).
5. `test_cancelled_task_stops_long_read`: `cancel_task` via repo/SQL during the read. The read stops within
   `< 1.5 s`; status stays `CANCELLED`.
- Keep timings margin-based (each assertion has >= 2x headroom). No `sleep`-based asserts on exact renew counts.

### Step 4: Docs (docs-keeper, ~25 lines)
- `docs/ARCHITECTURE.md` §3.1:
  - line 331: "per batch: renew the lease" becomes "a background renewer extends the lease every `lease_s/3` and
    enforces the wall-clock budget (also during one long object read: the read thread checks a stop flag per chunk);
    each batch's insert transaction re-checks the lease (fence)".
  - line 332: after "GET pinned by listed ETag + size": add "the S3 backend **must honour `If-Match` on GetObject**;
    as a backstop the splitter also compares the GET response's `ETag` with the listed one, because a backend that
    ignores `If-Match` would otherwise miss a same-size replacement (mismatch → `INPUT_CHANGED`)".
  - line 344: "lease renewal (once per batch)" becomes "lease renewal (timer, every `lease_s/3`, and in each batch
    insert)". Add one sentence: shutdown during a read stops the read thread at the next chunk and does not count as a
    failed attempt.
- `settings.py:145` comment: "claim lease, renewed every lease_s/3; crashed splits retry after".
- Splitter module docstring (Step 2). SPEC: no change. ADR: not needed (no deviation from the paper or the
  architecture; this is a robustness fix inside ARCH §3.1's design).

## 5. Risks and open questions
- **R1 (fixed here):** the `ValueError`-on-cancel bug (§1). It is in scope because it burns split attempts on
  every restart, the same failure class as the task. If the orchestrator prefers a separate task, split Step 2's
  `_take_in_thread` and test 4 out. They are independent of the renewer.
- **R2:** `asyncio.wait([fut], timeout=15)` on shutdown. If S3 stalls longer, we stop waiting and skip `close()`,
  and the thread finishes on its own (daemon pool thread). Uvicorn's graceful timeout must be >= 15 s, or the
  process is killed, which is harmless.
- **R3:** ETag format differences between LIST and GET on some backends (quoting, multipart `-N`, SeaweedFS). The
  normalisation handles quoting. A real mismatch would fail every task as `INPUT_CHANGED` (fail closed). The
  optional multipart integration check in Step 1 covers SeaweedFS. AWS S3 returns identical ETags.
- **R4:** timing-based integration tests on loaded CI. All bounds have >= 2x headroom, and the red-first check on
  `main` proves the test discriminates.
- **Open Q1:** should a GET-ETag mismatch be retryable (a re-list might pick up the new version)? Recommendation: no.
  Keep it deterministic `INPUT_CHANGED`, consistent with the 412 path and ADR-0004 (immutable inputs). Rows from an
  older version may already be committed, so a retry would fail on resume anyway.
- **Open Q2:** no new setting (`MINION_SPLIT_RENEW_S`). `lease_s/3` is fixed per the AC. Add a setting only if
  operators need it.

## 6. Size
4 steps, one milestone (M2), about 435 changed lines. No sub-tasks needed.
<!-- SECTION:PLAN:END -->
