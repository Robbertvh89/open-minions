---
id: TASK-12
title: Worker IPC and Ollama hardening follow-ups (from TASK-4.3 review)
status: In Progress
assignee: []
created_date: '2026-10-16 03:29'
updated_date: '2026-10-20 03:06'
labels:
  - executor
  - security
dependencies: []
priority: low
ordinal: 22000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Leftover MINOR/LOW findings from TASK-4.3 review and security rounds 4:
- Ollama release_model: real overall wall-clock cap (httpx timeouts are per-phase; a dripping local listener can hang kill/interrupt/_reset >10 s).
- WorkerHandle._send has no watchdog: a worker that stops reading blocks the control thread once a batch exceeds the pipe buffer.
- Watchdog creates one Timer thread per received frame; use one reusable deadline thread per _call and/or rate-limit ticks control-side.
- Timer may fire between _recv returning a final reply and cancel(): reply returned with a dead worker; check proc.is_alive() after cancel.
- stats() and probe() results not type-checked (list result crashes probe_available at startup).
- kill()/interrupt() with a call in flight send the Ollama release twice (up to ~3 s each).
- After a successful worker unload the control side skips its own release even if the runner's keep_alive:0 POST failed.
- run() validates its reply outside the _call critical section; move into _call (expect_list).
- Dev-only (sandbox off): a forked grandchild holding the pipe fd defeats the watchdog kill; shutdown the socket or kill the process group on fire.
- Ollama completions: no PROMPT_TOO_LONG detection (daemon truncates silently).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 Each item fixed with a test or explicitly accepted in ADR-0009
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
# TASK-12 plan: worker IPC and Ollama hardening follow-ups

Paths are relative to the repo root. P = packages/minion-executor/src/minion_executor, T = packages/minion-executor/tests.
Line numbers are against origin/main d97892e.

## Goal and AC
Close the leftover MINOR/LOW findings from the TASK-4.3 review (10 bullets) and the follow-ups added to
the notes (TASK-5.3 a/b, TASK-5.5, TASK-5.6, TASK-4.4, TASK-6.3). AC #1: each item is fixed with a
test, or accepted explicitly in ADR-0009.
SPEC mapping: S-10 (worker confinement, ADR-0009), FR-11/FR-13 (executor loop, engines, warm worker),
FR-6 (PROMPT_TOO_LONG lines), invariant 6 (yield / bounded stop), invariant 7 (inference only in the
worker), "never log payloads".

## Status of each item (checked against current code)
| # | Item | Status | Evidence |
|---|------|--------|----------|
| 1 | release_model: no total wall-clock cap | OPEN | P/runners/ollama.py:86-95: one `client.post` with per-phase `_RELEASE_TIMEOUT` (:57); a server that drips bytes resets the read timeout each chunk. Callers P/worker/process.py:1006, 1033, 1067 |
| 2 | `_call` request send has no watchdog | OPEN (ctl sends fixed in TASK-5.3) | process.py:595-598: `with self._send_lock: _send(conn, msg)`, no lock timeout and no kill watchdog; ctl sends are bounded (:828-843) |
| 3 | one Timer thread per received frame | OPEN | process.py:620-628, a new `threading.Timer` per frame in the recv loop |
| 4 | timer fires between `_recv` and `cancel()` | OPEN | process.py:626-634: no check after `cancel()`; a final reply is returned from a worker that was just killed |
| 5 | stats()/probe() results not type-checked | OPEN | process.py:942 and :958 return `reply["result"]` unchecked (`_call` only checks dict/list/None, :673-677); P/main.py:284 `found.get(...)` raises AttributeError on a list |
| 6 | kill()/interrupt() + call in flight = two releases | OPEN | kill :999/:1006 and interrupt :1026/:1033 read `_ollama_ref` without clearing it; the failing call's `_reset` (:1045, :1067) releases it again |
| 7 | graceful unload skips the control-side release even if the runner's POST failed | OPEN | process.py:987 clears `_ollama_ref` once the unload call returns; OllamaRunner.unload swallows HTTPError (ollama.py:380-383). Pinned by test T/test_runner_worker.py:948-966 |
| 8 | run() validates outside the `_call` lock | OPEN | process.py:784-788 (re-takes `_lock` after the call returned) |
| 9 | dev-only (sandbox off): grandchild holding the pipe fd defeats the watchdog kill | OPEN (reaping accepted in ADR-0009:133, wedging not) | `_kill_quietly` (:132) kills only the child pid; the socketpair stays open through the grandchild's fd |
| 10 | Ollama: no PROMPT_TOO_LONG detection | ALREADY ACCEPTED in ADR-0009:165 and SPEC FR-13 (SPEC.md:117-118). Recommend a cheap best-effort check (step 4) | ollama.py:240-275 never compares `prompt_eval_count` with `max_prompt_tokens` |
| N1 | TASK-5.3 (a) `_call` send lock without timeout | OPEN = item 2 | process.py:595 |
| N2 | TASK-5.3 (b) ack `paused` not checked against the delivered gen; acks may go backwards | OPEN | P/worker/control.py:104-115 (`parse_ack` checks only `gen <= issued`); process.py:639 |
| N3 | TASK-5.5 ensure_model ignores revision | OPEN | process.py:702 compares `model_id` only; loop compares (model_id, revision) (P/loop.py:185-187) and always calls `ensure_model` (loop.py:1819-1822), so a revision bump keeps the old weights warm |
| N4 | TASK-5.6 warn when Ollama is enabled (duty is per request only) | OPEN | P/main.py:404-411: no warning; the GPU duty guard skips Ollama (main.py:125-145, METERED_ENGINES) |
| N5 | TASK-4.4 MPSGraph cache dir write denied | ACCEPT in ADR-0009 (step 5) | ADR-0009:181-182 mentions MPSGraph debug-dump noise for HF only; no allow in P/worker/sandbox.py |
| N6 | TASK-6.3 lenient ModelEntry variant | DEFER (trigger: first new ModelEntry field). Not hardening; propose its own task |

Nothing here was fixed by TASK-15 (GPU guard: process.py:903-917 adds `gpu_clients` only) or TASK-42
(probe timeout: main.py:224-285 only). TASK-5.3 fixed the ctl-send half of item 2.

## Design decisions
- D1 One deadline thread per `_call` (`_Deadline`), armed for the request write and for each frame
  read, instead of one Timer per frame. No control-side tick rate limit: the worker already rate-limits
  ticks (`_Ctl._tick`, :218-220), and once a frame costs no thread a flood only costs parse time; the
  hard deadline / GPU guard bound misbehaviour.
- D2 Every watchdog fire (call send, call recv, ctl send) does `kill(proc)` and then
  `shutdown(SHUT_RDWR)` on the control end of the socketpair (`multiprocessing.Pipe(duplex=True)` is a
  `socket.socketpair` on POSIX). shutdown acts on the socket, not the fd, so a grandchild's copy of the
  fd cannot keep a blocked `send_bytes`/`recv_bytes` alive. No killpg (ADR-0009:131-133 rejects process
  groups because of pid reuse). Safe against fd reuse: armed only while the call holds `_lock`, and
  `_reset` closes the conn only under `_send_lock` (:1053).
- D3 The request write is bounded by the call's own limit (`min(now + timeout_s, hard_deadline)`), the
  same bound as the reply. No new knob. A fire raises `WorkerTimeout`.
- D4 Result validation moves into `_call` (`expect=` predicate, checked under `_lock`); invalid ->
  `_reset()` + `WorkerCrashed("malformed worker reply")`.
- D5 Ollama release: `release_model` runs the POST on a daemon thread and waits at most
  `RELEASE_TOTAL_S = 3.0` in total. At most 2 release threads may be outstanding (module
  `BoundedSemaphore`); when both are stuck it returns False at once, so a dripping listener can leak
  at most 2 threads (loopback socket only; ADR-0009 already treats a hostile service on that port as
  trusted). Returns bool, never raises.
- D6 `_ollama_ref` is taken (read and cleared) once, under a new `_ref_lock`, by whoever releases.
  After a graceful unload the control side ALWAYS sends its own release too (it does not trust the
  worker's word; `keep_alive: 0` is idempotent and takes milliseconds on a healthy daemon, at most
  RELEASE_TOTAL_S on a sick one). Simpler than adding a "released" flag to the Runner protocol.
- D7 Ollama PROMPT_TOO_LONG (optional, recommended): after the fact, `prompt_eval_count >
  max_prompt_tokens` -> per-record `{"id", "error": "PROMPT_TOO_LONG", "prompt_tokens": n}`, the same
  shape as mlx_completion.py:117 / hf.py:314. Best effort: false negatives when the prompt cache reuses
  a prefix or the daemon truncates below the cap. Never a false positive.
- D8 Ack checks stay advisory-safe (no history): reject an ack whose gen is below the last ack of the
  same worker, and an ack for the gen in force (`self._ctl.gen`, or 0 = unpaused default) whose
  `paused` differs. A gen that is neither is still accepted if `<= issued` (no per-gen history, so
  a healthy worker can never be killed by a lost history entry).

## Steps

### Step 1: one deadline thread per call, bounded request write, socket shutdown on fire (items 2, 3, 4, 9, N1)
Files: P/worker/process.py, T/test_worker_ipc.py (new; keeps the 964/~900-line files from growing).
- New private helpers in process.py:
  ```python
  def _shutdown_quietly(conn: Connection) -> None:
      """shutdown(SHUT_RDWR) the socketpair end: EOF/EPIPE for every fd copy (grandchildren too)."""
      # socket.socket(fileno=os.dup(conn.fileno())) -> shutdown -> close the dup; suppress OSError,
      # ValueError, AttributeError (closed conn, non-socket test peer)

  class _Deadline:
      """One daemon thread per `_call`: when an armed deadline passes it sets `fired` and calls
      `on_fire` once. `arm`/`disarm` are cheap and create no threads."""
      def __init__(self, on_fire: Callable[[], None], *, clock: Callable[[], float] = time.monotonic) -> None
      def arm(self, at: float) -> None          # starts the thread lazily on the first arm
      def disarm(self) -> bool                  # True if it already fired
      @property
      def fired(self) -> bool
      def close(self) -> None                   # stops the thread (join with a short timeout)
  ```
  Implemented with `threading.Condition`; `fired` is set under the condition BEFORE `on_fire` runs,
  so `disarm()` returning False means it will never fire for that arm.
- `WorkerHandle.__init__`: test seam `self._deadline_factory: Callable[[Callable[[], None]], _Deadline] = _Deadline`.
- `_fire(proc, conn)`: `_kill_quietly(proc)` then `_shutdown_quietly(conn)`.
- `_call` (:586-670):
  - `if not self._send_lock.acquire(timeout=CTL_SEND_TIMEOUT_S + 1): raise WorkerCrashed("worker pipe busy")`
    (goes through the existing `except WorkerCrashed: self._reset()`).
  - Create `dl = self._deadline_factory(partial(self._fire, proc, conn))` once per call; `dl.arm(limit)`
    around `_send(conn, msg)` (limit per D3); `if dl.disarm(): raise WorkerTimeout("worker timed out")`.
    An OSError/BrokenPipe from the write after a fire is mapped to WorkerTimeout too (check `dl.fired` in
    the `except (EOFError, ...)` branch at :656).
  - Replace the Timer at :620-628 with `dl.arm(limit)` / `fired = dl.disarm()`; `if fired: raise
    WorkerTimeout("worker timed out")` BEFORE the reply is looked at (item 4).
  - `finally: dl.close()` next to `self._op = None`.
- `_send_ctl` (:830-842): keep the Timer (ctl sends are rare and already bounded), but
  `_abort_stuck_send` (:850-859) also calls `_shutdown_quietly(conn)`; pass conn through.
Tests (unit, deterministic: in-process `mp.get_context("spawn").Pipe()` + a `_FakeProc` with
`is_alive/kill/join/exitcode/pid`; frames are pre-written into the socket buffer before the call, so
there is nothing to wait for):
- `_Deadline`: arm at `clock()` (past) fires once (wait on an Event set by on_fire, 5 s cap); arm far
  future + disarm -> returns False, never fires, close() joins the thread; 1000 arm/disarm cycles leave
  `threading.active_count()` at baseline + 1 (single thread), item 3.
- `_shutdown_quietly`: with an extra `os.dup` of the child fd held open (the "grandchild"), a
  `recv_bytes` on the parent end returns EOFError right after shutdown, and a writer thread blocked in
  `send_bytes(8 MiB)` raises OSError (join 5 s) - item 9.
- Fake deadline whose `disarm()` returns True; peer has a valid final `{"ok": true, "result": {...}}`
  queued -> `stats()` raises WorkerTimeout and the handle is reset (`pid is None`) - item 4.
- Non-reading peer (nobody reads the child end), `_FakeProc.kill` is a no-op (models the grandchild),
  `run()` of a ~8 MiB batch with `step_timeout_s=0.2`: raises WorkerTimeout, the thread returns (outer
  join 10 s), `_FakeProc.killed` is True - items 2/9. (The 0.2 s is the deadline under test, not a sleep.)
- `_send_lock` held by the test thread, `CTL_SEND_TIMEOUT_S` monkeypatched to 0.01: `stats()` raises
  WorkerCrashed("worker pipe busy") and the handle is reset (its `_reset` gives up on the held lock after
  ~1 s and leaves the conn to GC, existing behaviour at process.py:1053-1060) - N1.
- Regression: existing T/test_runner_worker.py:890-912 (partial-frame stall) and T/test_worker_control.py:733
  (non-reading worker vs control) stay green.
Invariants: 7 untouched (no engine code). Payloads: error messages carry no batch content (fixed strings).

### Step 2: result validation inside `_call`; ensure_model honours the revision (items 5, 8, N3)
Files: P/worker/process.py, T/test_runner_worker.py.
- `_call(..., expect: Callable[[object], bool] | None = None)`; after the `valid` check (:673-680) and
  only when `ok`: `if expect is not None and not expect(reply.get("result")): self._reset(); raise
  WorkerCrashed("malformed worker reply")`.
- Module predicates (pure, typed):
  - `_is_records(r)`: list of dicts (moves :785).
  - `_is_stats(r)`: dict with `active_bytes, peak_bytes, max_rss_bytes, trim_errors` each a non-bool int >= 0.
  - `_is_probe(r)`: dict, every key str, every value `str | None`.
- `run()`: pass `expect=_is_records`; delete :784-788. `stats()` (:942) `expect=_is_stats`; `probe()`
  (:950) `expect=_is_probe`.
- ensure_model (:702): new `self._model_key: tuple[str, str | None] | None`, set with `_model_id` at
  :722 to `(model.model_id, model.revision)`, cleared in `_reset` (:1039). Warm check becomes
  `self._model_key == (model.model_id, model.revision) and ...is_alive()`; the switch branch (:704) is
  unchanged, so a revision bump goes through `_graceful_reset` (Ollama: keep_alive 0) and a fresh load.
Tests (unit, fake engine / Peer):
- Peer answers `{"ok": true, "result": [1, 2]}` to stats and to probe -> WorkerCrashed("malformed"),
  worker killed; `{"ok": true, "result": {"active_bytes": true, ...}}` -> same; a good stats dict passes.
- `probe_available` with a monkeypatched handle returning a list now surfaces WorkerCrashed, not
  AttributeError (T/test_cli.py or a new test next to the existing probe tests).
- Rewrite T/test_runner_worker.py:937-946 (`run_requires_a_list_of_dicts`) to feed the bad result
  through a Peer instead of monkeypatching `_call` (which would now bypass the check).
- Revision: fake-engine worker, `ensure_model(m)` then `ensure_model(m.model_copy(update={"revision":
  "r2"}))` -> new pid; same revision twice -> same pid.
Invariants: 7 untouched. Note: bench (P/bench.py:339, P/bench_budget.py:441) uses stats(); a bad result
now raises WorkerCrashed there, which is correct.

### Step 3: Ollama release semantics (items 1, 6, 7)
Files: P/runners/ollama.py, P/worker/process.py, T/test_ollama_runner.py, T/test_runner_worker.py.
- ollama.py:
  ```python
  RELEASE_TOTAL_S = 3.0
  _MAX_RELEASE_THREADS = 2
  _release_slots = threading.BoundedSemaphore(_MAX_RELEASE_THREADS)

  def release_model(url: str, ref: str, *, total_s: float = RELEASE_TOTAL_S) -> bool:
      """Best-effort keep_alive: 0 under a TOTAL wall-clock cap. True only for a 2xx in time.
      Never raises, no env proxies, never logs."""
  def _post_release(base: str, ref: str) -> bool  # the existing httpx call (seam for tests)
  ```
  `check_loopback` failure -> False. No free slot -> False without a thread. The thread releases its
  slot in `finally`. Keep `_RELEASE_TIMEOUT` (each phase still bounded, so a normal stuck thread ends).
  Existing monkeypatches `lambda url, ref: ...` stay compatible (callers pass no `total_s`).
- process.py:
  - `self._ref_lock = threading.Lock()`; `_take_ollama_ref() -> str | None` (swap to None under the lock).
    ensure_model (:709) sets the ref under the same lock.
  - kill (:999-1006) / interrupt (:1026-1033): `ref = self._take_ollama_ref()` only inside `if proc is
    not None`, then release; `_reset` (:1045) also uses `_take_ollama_ref()`. The in-flight call's
    `_reset` then finds None: one release per hard kill (item 6).
  - `_graceful_reset`: delete :987 so `_reset` always sends the control-side release (item 7).
Tests:
- ollama: `_post_release` monkeypatched to block on an Event: `release_model(..., total_s=0.05)` returns
  False; two blocked calls fill the slots and a third returns False without starting a thread
  (`threading.active_count()` unchanged); set the Event and join -> slots free, next call True. A dripping
  loopback listener (socket server thread that sends one header byte every 0.2 s) with `total_s=0.5`
  returns within 2 s (marked `integration`? no: loopback only, fast; keep unit). Existing
  test_release_model_sends_keep_alive_zero (T/test_ollama_runner.py:569) keeps passing.
- process: `_FakeProc` + ref set: `kill()` then `_reset()` -> exactly one release; `interrupt()` then
  `_reset()` -> one; real fake-engine in-flight run killed from a thread (pattern of
  test_kill_interrupts_inflight_run, :265) -> `seen == [ref]`.
- Update T/test_runner_worker.py:948-966: graceful `unload()` now releases from the control side too
  (`seen == ["q:1", "q:2"]`); add a case where the worker's unload "succeeds" (Peer replies ok) and the
  control side still releases.
Invariants: 6 (stop path bounded: interrupt/kill <= KILL_REAP_TIMEOUT_S + RELEASE_TOTAL_S); 7 (control
side only sends a keep_alive POST, no inference); no prompt in the release body.

### Step 4: Ollama PROMPT_TOO_LONG (best effort) and the Ollama duty warning (item 10, N4)
Files: P/runners/ollama.py, P/main.py, T/test_ollama_runner.py, T/test_cli.py (or test_executor wiring test).
- ollama.py `_chat` (:264-275): `n = int(data.get("prompt_eval_count") or 0)`; if `n >
  self._cfg.max_prompt_tokens`: return `{"id": rec.get("id"), "error": PROMPT_TOO_LONG, "prompt_tokens":
  n}` (local constant `PROMPT_TOO_LONG = "PROMPT_TOO_LONG"`, as hf.py:42 / fake.py:20). No content copied.
- main.py after `available` is known (:404-411): if `Engine.OLLAMA in available`: one
  `log.warning("ollama_duty_cycle_not_enforced", reason="daemon_outside_worker")` (fixed fields, no ref).
  Status: not in status.json (keep scope small; ADR text instead).
Tests: MockTransport chat reply with `prompt_eval_count = max_prompt_tokens + 1` -> error line with
`prompt_tokens`, no `completion` key; `== max_prompt_tokens` -> normal line. caplog/structlog capture:
warning emitted once with `--engines ollama`, not with mlx/fake.
If the orchestrator prefers the existing acceptance (ADR-0009:165) for item 10, drop the ollama.py part
and keep only the warning; the AC is met either way.

### Step 5: ack consistency (N2) + docs
Files: P/worker/control.py, P/worker/process.py, T/test_worker_control.py, docs/adr/0009-offline-sandboxed-inference-worker.md,
docs/adr/0006-cooperative-capacity-governor.md (one sentence), docs/SPEC.md (FR-13 Ollama bullet, if step 4 lands).
- `parse_ack(reply, *, issued, at, prev: WorkerAck | None = None, expect_paused: tuple[int, bool] | None = None)`:
  ValueError if `prev is not None and gen < prev.gen`, or if `expect_paused == (gen, p)` and `paused != p`.
  process.py:639 passes `prev=self._ack` and `expect_paused=(c.gen, c.paused)` for `c = self._ctl`
  (or `(0, False)` when `_ctl is None`). Gen 0 always means unpaused (worker default, process.py:398).
- Tests (pure): backwards ack rejected; mismatched paused for the gen in force rejected; an older gen
  (not the one in force) with any `paused` accepted; adversary payload added to the parametrised
  test_malformed_ack_or_usage_is_a_protocol_violation (T/test_worker_control.py:284-312), e.g.
  `{"tick": 1, "ack": 0, "paused": true}` with no ctl issued.
- ADR-0009 amendments (residual-risk list, :131-133 and :165-172):
  - sandbox-off grandchildren: still not reaped (dev only), but every watchdog fire now shuts the pipe
    socket down, so they cannot wedge the control side;
  - Ollama release: total cap RELEASE_TOTAL_S, at most 2 outstanding release threads, one release per
    hard kill, control side always releases after a graceful unload;
  - PROMPT_TOO_LONG: best effort via `prompt_eval_count` (or keep "not detectable" if step 4 drops it);
  - Ollama duty: paced per request, not metered, startup warning;
  - MPSGraph cache (TASK-4.4): writes to `com.apple.MetalPerformanceShadersGraph` stay denied and are
    accepted: a second shared writable cache would widen the cross-worker poisoning surface already
    accepted for `com.apple.metal` (:119-121); cost is stderr noise and no MPSGraph cache reuse, so
    bench numbers may be slightly pessimistic (note in docs/bench/m3-apple-m3-pro.md).
  - Deferred: lenient ModelEntry variant (TASK-6.3), see sub-task below.
- ADR-0006: one line that acks are additionally checked for monotonicity and `paused` of the gen in force.
Invariants: acks stay advisory; enforcement unchanged (footprint, priority, GPU guard).

## Risks / open questions
- R1 `_shutdown_quietly` relies on `Pipe(duplex=True)` being a socketpair (CPython POSIX). If someone
  switches to `duplex=False` (os.pipe), shutdown is a no-op (ENOTSOCK suppressed) and only the kill
  remains; add an assertion-style unit test that the handle's conn is a socket.
- R2 Request-write bound = call timeout (load: 1800 s). A wedged dev-only worker blocks the loader thread
  that long unless interrupted; the loop's stop path interrupts it with a kill only (no socket shutdown without the call lock, accepted in ADR-0009: dev-only, sandbox off, the call's own deadline then shuts the socket down). Alternative:
  a fixed 60 s cap for the write; not proposed (new constant, no evidence a healthy worker needs less).
- R3 Always releasing after a graceful unload adds one loopback POST per model switch/idle unload
  (ms when healthy). Accepted.
- R4 Abandoned release threads: max 2, daemon, loopback only.
- R5 Step 4 PROMPT_TOO_LONG changes observable output for over-cap Ollama prompts (was a truncated
  completion, becomes an error line). That matches FR-6 for the other engines; SPEC FR-13 text must change.
- No ADR needed beyond amendments to ADR-0009 (and one line in ADR-0006); nothing deviates from the paper.

## Proposed sub-task (orchestrator decides; not run)
backlog task create "Lenient ModelEntry response variant for executors (from TASK-6.3)" -p TASK-12 \
  -d "When the first new ModelEntry field is added, executors must accept it: add a lenient (extra=ignore) response variant used in Assignment, keep the server's registry strict." -l executor,api
(Or close that note as deferred-by-trigger in TASK-12's notes; it is not a hardening item.)
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
TASK-4.4 (M3 bench): the worker sandbox denies writes to the com.apple.MetalPerformanceShadersGraph cache dir. Runs succeed, but stderr is noisy and there is no MPSGraph cache reuse, so bench numbers may be slightly pessimistic. Evaluate a narrowly scoped write allow for that cache dir (ADR-0009), or accept it explicitly.

Narrowed (TASK-5.3): ctl sends are now bounded by a kill watchdog (CTL_SEND_TIMEOUT_S). Still open: _call's initial request send (up to 64 MiB, under _lock/_send_lock) has no time limit; a non-reading worker blocks the call thread until kill/interrupt. Fix with the same kill watchdog.

TASK-5.3 final audit: (a) _call takes _send_lock and sends the request with no timeout; with the sandbox off (DEV) a worker can fork a child that holds the pipe open and stops reading, wedging _send_lock then _lock. Fix: acquire(timeout=CTL_SEND_TIMEOUT_S+1) -> WorkerCrashed, and the same kill watchdog on the _call write. (b) Ack 'paused' is not checked against the delivered generation's state and acks may go backwards; keep delivered gen->paused, reject mismatch and ack < previous. Acks are advisory (ADR-0006); enforcement is control-side.

From TASK-5.5 review: WorkerHandle.ensure_model treats a warm worker as reusable when model_id matches (worker/process.py:675), ignoring revision; the loop now compares (model_id, revision). If the catalogue bumps a model's revision under the same id, a warm worker keeps the old weights. Compare revision (or the cache path) in ensure_model too.

From TASK-5.6 round 3: Ollama is paced per request only (the daemon owns the GPU queue). Warn at startup / in status when the Ollama engine is enabled and duty < 1 (or force concurrency 1).

From TASK-6.3: ModelEntry nested in Assignment stays extra=forbid (server validates the model registry strictly), so a future new ModelEntry field would break older executors; add a lenient response variant when the first such field is added.

Done (branch fix/task-12-worker-ipc-hardening): one deadline thread per _call, bounded request write and send-lock wait, socket shutdown on every watchdog fire, reply racing a fire is void (WorkerTimeout); result validation inside _call (stats/probe/run); ensure_model honours the revision; Ollama release has a total 3 s cap, max 2 threads, one release per hard kill, always after a graceful unload; startup warning when Ollama is enabled; ack monotonic + paused-in-force check (ADR-0006). Accepted in ADR-0009: Ollama PROMPT_TOO_LONG not detectable (existing acceptance, SPEC FR-13 unchanged), MPSGraph cache write stays denied, sandbox-off grandchildren not reaped (but cannot wedge the pipe). Deferred: lenient ModelEntry variant (TASK-6.3) until the first new ModelEntry field is added.
<!-- SECTION:NOTES:END -->
