---
id: TASK-43
title: GPU guard follow-ups from TASK-15 review
status: In Progress
assignee: []
created_date: '2026-10-19 14:36'
updated_date: '2026-10-20 00:15'
labels:
  - security
dependencies: []
ordinal: 83000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
From TASK-15 final review/security: (1) a GPU verdict already taken by the watcher is dropped when work finished in the same wake-up or _infer_done was set before _on_verdict (loop.py ~880, ~1240, ~1327): put it back into _gpu_pending or run _gpu_kill_idle after _complete. (2) Meter kills never count toward quarantine: a worker that churns IOKit clients so every registry walk fails gets back-off only (~3 % avg GPU, ~7 s unmetered bursts); count meter kills at a higher threshold (e.g. 6/h/engine) or when read failures coincide with the worker's client set changing. (3) Optionally persist quarantine expiry in the state dir across executor restarts.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 Taken GPU verdicts are never lost (tests for both paths)
- [x] #2 Meter-failure churn is bounded by quarantine (test + sim)
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
# TASK-43 plan: GPU guard follow-ups from TASK-15

Worktree `/Users/robbertvanhintum/Documents/claude_code/open-minions-task43`, branch `fix/task-43-gpu-guard-followups`.
All paths below are relative to `packages/minion-executor/` unless they start with `docs/` or `CLAUDE.md`.

## Goal and acceptance criteria

The guard (ADR-0006 D17) must never lose a KILL it has decided, and a worker that blinds the meter
must end up quarantined, not just backed off.

- AC#1 "Taken GPU verdicts are never lost (tests for both paths)". Path A: the watcher took the
  verdict but `work` finished in the same wake-up (`loop.py:880-882` breaks, the watch result is
  never read; the `finally` at `loop.py:957` only sees `_gpu_pending`, which `_watch` already
  cleared at `loop.py:1245-1247`). Path B: `_infer_done` was set before `_on_verdict` ran
  (`loop.py:1332-1334` returns None and the main loop discards the watch, `loop.py:908`). Related
  holes found while reading: `_on_verdict` interrupts the worker and then returns early
  (`loop.py:1340-1341` when `_infer_ok`, `loop.py:1345-1346` for a raced success) **before** the
  GPU bookkeeping at `loop.py:1357-1362`, so that kill is never counted either. The stopper branch
  (`loop.py:883-894`) drops a GPU verdict that completed in the same wake-up.
  Maps to FR-50 (gpu_duty_cycle enforcement, status note SPEC.md:260), NF-8, invariant 6.
- AC#2 "Meter-failure churn is bounded by quarantine (test + sim)". Today `_note_gpu_kill(...,
  quarantine=not meter)` (`loop.py:1027`, `loop.py:1360`, early return `loop.py:1104-1105`) means
  meter kills feed only the back-off. Maps to FR-50, FR-55 (presence `gpu_violation`), FR-56
  (`gpu_quarantined` in status.json), SPEC §8 / S-10 (compromised worker), invariant 6.
- Optional item (3), persisting the quarantine across restarts: **recommendation: defer** (see the
  Deferred section; a sub-task command is given).
- Round-4 notes (a)-(d) from the task notes are planned as Step 3 (and (b) as part of Step 1).

Invariant 7 is what makes all of this safe: inference is in the worker subprocess and
upload/complete are done by the control process (`loop.py:1630-1656` never touch the worker after
`_infer_done` is set at `loop.py:1759`), so killing the worker after inference never loses a
result and never stops the control loop.

## Step 1: never drop a taken GPU verdict (loop.py, about 120 changed lines incl. tests)

Files: `src/minion_executor/loop.py`, `tests/test_loop.py`.

1. Add `_GPU_VERDICTS = frozenset({_Verdict.GPU_DUTY, _Verdict.GPU_METER})` next to `_Verdict`
   (`loop.py:105-115`).
2. One bookkeeping helper, used by every kill path (replaces the duplicated code at
   `loop.py:1021-1028` and `loop.py:1357-1362`):
   ```python
   def _record_gpu_kill(self, v: _Verdict, engine: Engine | None, pid: int | None) -> None:
       """Bookkeeping of one GPU kill on any path: status reason, killed pid, back-off streak,
       per-engine quarantine points; drops a pending verdict and re-baselines the guard."""
       meter = v is _Verdict.GPU_METER
       self._gpu_violation = (self._gpu_reason or ("meter" if meter else "over_duty"), self._clock())
       self._gpu_killed_pid = pid
       self._note_gpu_kill(
           engine, meter=meter
       )  # Step 2 changes the signature; in Step 1 keep quarantine=not meter
       if self._gpu is not None:
           self._gpu.stop()
       self._gpu_pid = None
       self._gpu_pending = None
       self._gpu_kick.clear()
       self._status_kick.set()
   ```
   `_gpu_kill_idle(self, v, *, engine: Engine | None = None)` (`loop.py:1009-1029`): keep the
   `pid is None or pid == self._gpu_killed_pid` early return, then
   `_record_gpu_kill(v, engine if engine is not None else (self._loaded.engine if self._loaded else None), pid)`
   and `await self._kill_worker(pid)`. Subtask-related callers pass `engine=a.model.engine`
   (after a `WorkerCrashed`, `_loaded` is None and the kill would otherwise not be charged to an
   engine).
3. `_take_gpu_pending(self, acq: UUID) -> _Verdict | None`: returns and clears `_gpu_pending` (and
   the kick) when it is tagged with `acq`, else None (stale tags are left to the existing
   handling).
4. `_flush_gpu_verdict(self, a: Assignment, watch: asyncio.Task[_Verdict] | None) -> None`
   (async): `v = self._take_gpu_pending(a.acquirement_id)`; if None and `watch` is done, not
   cancelled, without exception and `watch.result() in _GPU_VERDICTS`, `v = watch.result()`; if
   `v` is not None, `await self._gpu_kill_idle(v, engine=a.model.engine)`. Never raises (wrap in
   `contextlib.suppress(Exception)` like `loop.py:965`).
5. `_handle`, path A: right after the `while not work.done()` loop exits normally
   (`loop.py:878-917`, i.e. before `try: result = work.result()` at `loop.py:918`):
   `await self._flush_gpu_verdict(a, watch if watch in pending else None)`.
   `watch in pending` is False once `_on_verdict` already handled it (`loop.py:908`), so a verdict
   is never acted on twice. This kills the worker **before** `_complete` (which can take up to
   `complete_shed_budget_s`); the result still stands (invariant 5 untouched: nothing is
   re-uploaded, the attempt key is unchanged).
6. `_on_verdict` (`loop.py:1322-1367`):
   - path B: replace the plain ignore at `loop.py:1332-1334` with: if `v in _GPU_VERDICTS` and
     `self._gpu is not None`: log `gpu_verdict_after_inference`, `await self._gpu_kill_idle(v,
     engine=a.model.engine)`, return None (upload carries on). Non-GPU verdicts stay ignored.
   - capture `pid = self._worker.pid` **before** the interrupt at `loop.py:1337`, and move the GPU
     bookkeeping (`loop.py:1357-1362`) to directly after the interrupt / cache release, as
     `if self._gpu is not None and v in _GPU_VERDICTS: self._record_gpu_kill(v, a.model.engine, pid)`.
     Then the `_infer_ok` early return (`loop.py:1340-1341`) and the raced-success `_complete`
     (`loop.py:1345-1346`) no longer skip it. The rest (release code `v.value`) is unchanged.
7. Stopper branch (`loop.py:883-894`): after the hard kill, if `watch.done()` with a GPU verdict
   (or `_take_gpu_pending(a.acquirement_id)` returns one), call
   `self._record_gpu_kill(v, a.model.engine, pid_before_kill)` (no second kill: `kill()` already
   ran). Only bookkeeping; it matters for honesty of the counters and for item (3) if that is ever
   done.
8. Keep the `finally` backstop (`loop.py:957-966`); it still covers returns from inside the loop
   (`_on_signal`, `_report_failure`) and a verdict injected during `_complete`.
9. Round-4 (b), killed-pid reuse: `_gpu_killed_pid` (`loop.py:292`) is never cleared, so a respawned
   worker that reuses the killed pid could never be idle-killed (`loop.py:1018`). Clear it where
   the loop knows a live worker exists after a load: `self._gpu_killed_pid = None` right after
   `self._loaded = a.model` (`loop.py:1726`). Every spawn the loop causes goes through
   `ensure_model` there (a dead handle is replaced inside `ensure_model`; `_execute` waits for
   `_kill_worker` via `_await_unload` at `loop.py:1713` first, so the clear cannot race a kill).
   No `Worker` protocol change. (A spawn-generation key would be more general but needs a protocol
   field in every fake; not worth it.)

Tests (unit, `tests/test_loop.py`, near `loop.py`-GPU tests at `tests/test_loop.py:4132`), all
with `_gpu_loop(...)` (`tests/test_loop.py:3731`) and `worker.gpu_busy = None` so the real sampler
never produces a verdict; the stubs below make the timing deterministic (asyncio FIFO scheduling):
- `test_gpu_verdict_taken_when_work_finishes_in_the_same_wakeup_is_not_lost` (path A): replace
  `loop._execute` with a stub that `await go.wait()`, sets `loop._infer_done`, returns a
  `CompleteRequest`; replace `loop._watch` with a stub that `await go.wait()` and returns
  `_Verdict.GPU_DUTY` (set `loop._gpu_reason = "over_duty"`). Both tasks finish in the same loop
  iteration after `go.set()`, so `_handle` wakes with both done. Wrap `loop._complete` to record
  `worker.interrupts` on entry. Assert: subtask completed, interrupts recorded at `_complete` entry
  == 1 (killed before complete), `len(loop._gpu_streak) == 1`, the MLX kill counted once,
  `loop._gpu_violation` set, `_gpu_pending is None`. Must fail on current main (verdict dropped,
  interrupts 0).
- `test_gpu_verdict_after_inference_kills_and_counts_while_the_upload_proceeds` (path B): stub
  `_execute` sets `_infer_ok` and `_infer_done`, then awaits `upload_gate`; stub `_watch` awaits
  `loop._infer_done.wait()` and returns `GPU_DUTY`. After `until(lambda: worker.interrupts == 1)`
  set `upload_gate`. Assert completed, interrupts == 1, streak 1 (no double count from the
  post-loop flush), no release.
- `test_gpu_pending_not_taken_before_inference_ended_is_killed_before_complete`: stub `_watch`
  never returns; stub `_execute` sets `_infer_done`, then sets
  `loop._gpu_pending = (a.acquirement_id, GPU_DUTY)` and returns. Assert interrupts == 1 at
  `_complete` entry (today it happens only in the `finally`, after complete).
- `test_gpu_verdict_racing_the_inference_end_is_still_counted`: `worker.interrupt` side effect sets
  `loop._infer_ok = True` while the `_execute` stub is still pending (the `loop.py:1340` early
  return); assert `_gpu_streak` len 1, `_gpu_killed_pid == 4242`, interrupts == 1, then the
  subtask completes.
- `test_gpu_verdict_with_a_hard_stop_in_the_same_wakeup_is_counted`: `_hard_stop` and the watch
  stub complete together; assert release `STOP` and streak 1.
- `test_gpu_killed_pid_rearms_after_a_respawn`: `_gpu_kill_idle(GPU_DUTY)` on pid 4242, run one
  subtask (ensure_model), `_gpu_kill_idle(GPU_DUTY)` again on the same fake pid 4242 -> interrupts
  2, streak 2. Keep `test_gpu_kill_of_an_already_killed_pid_is_not_repeated`
  (`tests/test_loop.py:4149`) green (no ensure_model in between).
- Existing tests to keep green unchanged: `tests/test_loop.py:3978` (stale tag dropped), `:4132`
  (backstop), `:4076`, `:4102`.

Invariants: 3/5 untouched (no extra server writes; a kept result is completed exactly as before,
a killed one is released as before). 6 strengthened (only verdicts the guard already decided are
executed; no new KILL decisions, so no new honest false positives). 7: kills go through
`interrupt()` in a thread; the control loop survives.

## Step 2: meter kills count toward quarantine (loop.py, about 100 lines incl. tests + sim)

Decision: **weighted points, one sliding hour per engine: a duty/clients kill = 2 points, a meter
kill = 1 point, quarantine at >= 6 points** (= 3 duty kills as today, or 6 meter kills, or a mix).
Rejected: correlating read failures with the worker's client-set churn.

Reasoning (honest-worker false positives first):
- Where honest meter kills come from: (i) host-wide IORegistry churn by *other* processes that
  invalidates every one of 3 walks in each of 3 consecutive reads (about 6 s; `gpu_meter.py:417-425`,
  `_WALK_ATTEMPTS = 3`); (ii) an IOKit read that overruns the sample interval under host load
  (`loop.py:1048-1054`); (iii) a control-process stall (`_gpu_stalled`, `loop.py:1148-1157`). None
  of them is attributable to the worker, which is why meter kills were excluded. A threshold of 3
  (same as duty) would let one transient storm (login, an Xcode build spawning Metal processes)
  quarantine an honest executor for an hour.
- At 6/h the condition has to persist through the escalating GPU back-off (10+20+40+80+160 s x
  U(0.5,1) >= 155 s, so at least about 2.5 min of repeated failures). At that point this Mac
  cannot be metered: invariant 6 says fail closed (do not run unmetered GPU work), and the
  alternative is a flapping executor that releases a subtask every few minutes (each release also
  burns the server-side `MINION_RELEASE_LIMIT` budget of that subtask). Quarantine converts the
  flapping into idling, which is the better outcome even for an honest owner.
- Correlation was rejected because a failed read carries no trustworthy client set (the walk was
  invalidated; that is exactly why it fails closed). Using partial walks would change the meter's
  error contract (`gpu_meter.py:417-425`, `IokitGpuReader`, `Worker.gpu_clients`,
  `_sample_gpu`, `_judge_gpu`), and detection would depend on registry ordering and on how fast
  the rogue closes clients, so no bound could be claimed in the ADR. The threshold gives a hard,
  testable bound.
- Bound: at most 6 meter kills per engine per sliding hour. With about 7.5 s unmetered per life
  (3 jittered 2 s samples), that is <= ~45 GPU-s per engine per hour (about 1.25 %), down from
  about 3 % with back-off only. Two metered engines (mlx + hf) double it; document that.

Changes (`src/minion_executor/loop.py`):
- Constants (`loop.py:135-136`): `GPU_QUARANTINE_POINTS = 6`, `GPU_DUTY_KILL_POINTS = 2`,
  `GPU_METER_KILL_POINTS = 1`; drop `GPU_KILLS_QUARANTINE` (update the 3 tests that use it, if
  any, or keep it as `GPU_QUARANTINE_POINTS // GPU_DUTY_KILL_POINTS` for the log text).
- State (`loop.py:296`): `self._gpu_kills: dict[Engine, list[tuple[float, int]]]` (clock, points).
  Simpler alternative that keeps `tests/test_loop.py:4123` untouched: keep `_gpu_kills` (duty
  kill times) and add `_gpu_meter_kills: dict[Engine, list[float]]`; points =
  `2 * len(duty) + len(meter)` within the hour. Implementer: pick the second (smaller diff).
- `_note_gpu_kill(self, engine, *, meter: bool = False)` (`loop.py:1097-1111`): always extends the
  streak; with an engine, appends to the duty or meter list (pruned to the hour); logs
  `gpu_engine_quarantined` (engine, `until_s`, `points`) on the transition into quarantine. Unknown
  engine stays back-off only (`loop.py:1104`).
- `_quarantine_points(engine, now) -> int` and `_quarantine_until(engine, now) -> float | None`
  (clock time at which the points drop below 6: walk the kills oldest-first, drop until the sum is
  < 6, the last dropped kill + `GPU_QUARANTINE_S`). `_quarantined()` (`loop.py:1132-1139`) uses
  `_quarantine_points >= GPU_QUARANTINE_POINTS` over the union of both dicts' keys.
  `_acquirable_engines` (`loop.py:1141-1146`) and the status (`loop.py:1418`) need no other change.
- Callers: `_record_gpu_kill` (Step 1) passes `meter=v is _Verdict.GPU_METER` (replaces
  `quarantine=not meter`).

Tests:
- Rewrite `test_gpu_meter_kill_backs_off_but_never_quarantines` (`tests/test_loop.py:4084`) as
  `test_gpu_meter_kills_quarantine_at_six_per_hour`: 5 meter kills (fresh fake pid each) ->
  still acquirable; the 6th -> `_acquirable_engines() == []`, `gpu_quarantined == ["mlx"]`; +3600 s
  -> lifted.
- `test_gpu_mixed_kills_quarantine_by_points`: 2 duty + 2 meter -> quarantined; 2 duty + 1 meter ->
  not.
- `test_gpu_quarantine_until_is_when_points_drop_below_threshold` (pure helper, fake clock).
- Keep `tests/test_loop.py:4005` (3 duty kills quarantine) and `:4115` (all quarantined -> presence
  `gpu_violation`, no acquire) green.
- Loop-level wiring: `test_gpu_meter_churn_ends_in_quarantine`: `_gpu_loop(reader=broken)`
  (pattern of `tests/test_loop.py:3782`) with 10 assignments, `backoff=(0.001, 0.01)` and a tiny
  `failure_pause_s`; run until `loop._acquirable_engines() == []`; assert exactly 6
  `GPU_METER_UNAVAILABLE` releases, then a presence with `paused_reason == "gpu_violation"` and no
  further acquire. Needs Step 1 (b) so repeated kills of fake pid 4242 are not deduped.
- Sim (AC#2 "sim"): `test_gpu_meter_churn_sim_is_bounded_by_quarantine` (fake clock, no real
  time): 4 simulated hours; each life costs 8.0 s unmetered (3 x 2 s x 1.25 + 0.5 s kill);
  `_note_gpu_kill(MLX, meter=True)`; `await loop._gpu_kill_backoff()` with `_sleep_fn` advancing
  the clock and `random.uniform` monkeypatched to its lower bound (fastest churn = worst case);
  while quarantined, advance 30 s (the loop's wait). Assert (1) at most 6 kills in any 3600 s
  window, (2) unmetered share <= 6 * 8 / 3600 + 0.002, and (3) the same sim with quarantine
  disabled (monkeypatch `GPU_QUARANTINE_POINTS` to a huge value) gives a share at least 2x higher
  (documents the gain over back-off only).

Invariants: 6 strengthened (meter-blind churn is now bounded, not just slowed). 1/2/3 untouched
(acquire simply omits a quarantined engine, `loop.py:636-642`). Honest false positives: discussed
above; a quarantine only removes work, it never runs anything outside the budget.

## Step 3: guard refinements from round 4 (gpu_guard.py, about 80 lines incl. tests)

Files: `src/minion_executor/capacity/gpu_guard.py`, `tests/test_gpu_guard.py`.

- (a) idle -> busy edge split. Today the mirror edge gets the busy limit for the whole sample
  (`gpu_guard.py:35-36`), so at duty 1.0 (limit 1.1) a rogue that burns only inside idle gaps
  shorter than a sample is never charged. Record the idle span, not just the busy edge: in
  `set_idle` (`gpu_guard.py:145-152`), on `idle -> busy` store
  `self._quiet_span = (self._started, now)` (`_started` is the quiet start set on entering idle,
  `gpu_guard.py:152`). In `_judge`, busy branch (`gpu_guard.py:285-286`): overlap
  `q = min(span_end, now) - max(span_start, prev_t, now - dt)`; if `q > 0`:
  `allow = cfg.margin * q + limit * (dt - q)`. Only the idle part is charged at the margin; the
  busy tail of the *previous* subtask that falls in the same sample keeps the busy limit (charging
  `[prev_t, edge)` as the note literally says would charge that tail against the margin and
  reintroduce the honest 1.8 s-subtask false positive that `test_honest_full_duty_worker_with_short_subtasks_is_never_killed`
  guards). Reset `_quiet_span` in `start()` for a new pid and in `stop()`.
  Honest-worker check: an honest worker uses no GPU while idle, so busy <= limit x (busy part)
  still holds; the only effect is less drain credit for idle time inside a busy sample, the same
  credit idle samples already get.
  Tests: extend `_edge_sim` (`tests/test_gpu_guard.py:557`) with `gap_rate: float = 0.0`;
  `test_gap_burning_rogue_at_full_duty_is_killed` (`_edge_sim(1.0, 1.0, 1.8, 0.8, 1800,
  gap_rate=1.0)` killed, bound to be set from the run, e.g. <= 600 s; must not be killed on the
  pre-change code); keep `tests/test_gpu_guard.py:601`, `:608`, `:614`, `:500`, `:516` green and add
  an honest 8 h variant with 0.8 s gaps at duty 1.0 and 0.75.
- (c) drain after a quiet-mode kill: **wording only** (no behaviour change). The quiet KILL is
  share-based and immediate, so the bucket only matters for the next busy period; draining at the
  margin would also change D17's measured churn numbers. Fix the docstring
  (`gpu_guard.py:38-39`, "a gap without a worker drains at the last busy-mode limit, also after a
  quiet-mode kill") and the ADR sentence.
- (d) `test_honest_paced_worker_with_short_subtasks_is_never_killed` (`tests/test_gpu_guard.py:608`)
  passes on pre-fix code. Make it discriminating: temporarily disable the straddle branch
  (`gpu_guard.py:287-295`) and raise `rate` (e.g. 0.8-0.85 at duty 0.75) until the test fails
  without it and passes with it; if no parameter set does, mark it "coverage only" in its
  docstring. Do not commit the disabled branch.

Invariants: 6 (tighter at duty 1.0, no new honest kill per the sims), 7 untouched. The guard stays
pure (no clock, no I/O).

## Step 4: docs (docs-keeper)

- `docs/adr/0006-cooperative-capacity-governor.md:352` D17: amend in place ("amendment, TASK-43"):
  verdict handling (a decided KILL is always executed and counted: before `complete` when the
  subtask's work already ended, at once after inference); quarantine points (duty/clients 2, meter
  1, threshold 6 per engine per hour, bound about 45 GPU-s/h/engine for meter churn); idle span
  split; killed-pid re-arm after a load; the drain wording; in Residual risks (`:367`) keep "State
  is per run" and point to the deferred sub-task. No new ADR: this amends our own decision, it does
  not deviate from the paper.
- `docs/SPEC.md:260` and `docs/ARCHITECTURE.md:560`: "3 kills of one engine within an hour" ->
  "6 points within an hour (a duty or clients kill counts 2, a meter kill 1)".
- `CLAUDE.md:71` ("3 kills/h quarantine an engine"): same wording change. Flag to the orchestrator:
  CLAUDE.md edits need the user's go-ahead.
- Backlog notes on TASK-43 (and the round-4 items ticked).

## Deferred: (3) persisted quarantine expiry (recommend a separate task)

Why defer: not in the acceptance criteria; the worker cannot cause a restart (sandbox denies
signals to other processes, process-fork/exec and file writes outside its tmp dir,
`worker/sandbox.py:8-25`, `:203-205`), so only an owner restart or a control-process crash resets
the state, both rate-limited by launchd; security measured restart churn at 0.33 / 0.42 share
against the 0.4 / 0.6 limits (ADR-0006 `:367`). Persisting adds a new state file, parsing and
wall-clock semantics (monotonic time does not survive a restart), and an owner-visible behaviour
change (a quarantine that survives `service` restarts needs a status hint and a reset story).

Design for the sub-task (so it is cheap later): `gpu_quarantine.json` in the state dir, written
with `atomic_write_private` (`capacity/safefile.py:121`, 0600, vetted dir, no symlink follow) on
every GPU kill, read once at `run` start with `read_private_file` (`capacity/safefile.py:85`, 4 KiB
cap); content: per engine a list of `{at: UTC timestamp, points}` within the last hour plus the
streak times; on load convert to monotonic offsets, drop entries older than 1 h and clamp entries
in the future to now (a clock moved back cannot extend a quarantine past 1 h from start); an
unsafe file or dir follows the existing state-dir rules (exit 2 like the other state files); an
invalid file is logged, ignored and rewritten. Tests: round trip, 0600, symlink refused, clock
skew clamp, a restart keeps the quarantine, the worker sandbox cannot write it (macos marker).

```
backlog task create "Persist GPU quarantine and kill streak across executor restarts" -p TASK-43 \
  -d "Follow-up of TASK-43 item 3: gpu_quarantine.json in the state dir (0600, safefile rules), UTC entries converted to monotonic on load, future entries clamped, invalid file ignored+rewritten, unsafe -> exit 2; ADR-0006 D17 residual risk 'State is per run' updated." \
  --ac "Quarantine and streak survive a restart within the hour" \
  --ac "Clock moved back cannot extend a quarantine beyond 1 h" \
  --ac "Unsafe file/dir handled like status.json/control.json" -l security
```

## Risks and open questions

- Step 1 kills the worker after inference in more cases. Safe by invariant 7, but the next subtask
  pays a model reload. Only when the guard already decided a KILL, so honest workers are not
  affected.
- The path A/B tests stub `_watch`/`_execute`; they test `_handle`'s handling, not the sampler. The
  existing real-sampler tests (`tests/test_loop.py:3740`, `:3885`, `:4061`) keep covering the
  end-to-end path.
- Step 2's points mix duty and meter kills: an honest executor with one legitimate duty kill and
  four host-caused meter kills in an hour is quarantined. Accepted (same fail-closed argument); an
  alternative is two separate thresholds (3 duty OR 6 meter), which a mixed rogue could exploit
  for up to 2 + 5 kills/h.
- Step 3 (a) changes guard math; the 8 h honest sims are the guard rails. If any honest sim needs
  loosened bounds, stop and report instead of loosening.
- No `status.json` schema change (`gpu_quarantined` stays a list of engine names).
- No real-GPU (`gpu` marker) evidence is needed for Steps 1-2 (control logic); Step 3 (a) could
  get one `bench --budget-check` row at full as a sanity check, but it is not required by the
  convention (no engine performance claim).
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Round-4 review follow-ups: (a) idle->busy edge not split: at duty 1.0 a rogue burning only in idle gaps < ~1 s is never charged (stays within 100 % budget) - record the busy edge in set_idle(False,t) and charge margin for [prev_t, edge). (b) _gpu_killed_pid never cleared: key on spawn generation or clear on spawn (pid reuse). (c) after a quiet-mode kill the gap drains at the last busy limit, not the margin - tighten wording or drain at margin. (d) test_honest_paced_worker_with_short_subtasks_is_never_killed passes on pre-fix code too (coverage only).
<!-- SECTION:NOTES:END -->
