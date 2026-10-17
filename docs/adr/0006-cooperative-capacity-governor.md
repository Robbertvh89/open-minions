# ADR-0006: Cooperative capacity governor

- Status: **Accepted** (written in TASK-5.1, wired in TASK-5.5, measured in TASK-5.6)
- Date: 2026-10-16
- Deciders: project owner
- Deviates from paper: **no** (the paper has no partial allocation; this refines ARCHITECTURE §3.6
  and SPEC §4.6, and changes three of their details, marked **Deviation** below).

## Context

macOS has no GPU quota and Metal has no public queue priority. SPEC §4.6 (FR-50..FR-56) asks the
executor to stay within the owner's budget and to yield to the user (invariant 6). M1 shipped static
profiles only. M4 (TASK-5) adds the live governor. This ADR records the decisions that shape it,
so the sub-tasks (5.1..5.6) share one design. Decisions D1..D14 are from the TASK-5 plan.

## Decision

**D1 Governor placement.** An asyncio task in the control process, polling signals every
`MINION_EXECUTOR_SIGNAL_POLL_S` (2 s, range 0.5..4: detection plus one engine step must stay under NF-9's 5 s) in `asyncio.to_thread`. The decision function
`governor.decide(snapshot, config, owner_ctl, now, prev_state)` is pure. **Deviation** from
ARCHITECTURE §3.6 ("polled + notifications"): no NSWorkspace / dispatch notifications. Polling bounds
detection at the poll interval, which leaves headroom under NF-9 (5 s) and keeps one code path.

**D2 Signal sources.** HID idle via Quartz `CGEventSourceSecondsSinceLastEventType`; thermal state
and Low Power Mode via `NSProcessInfo`; power source and battery via IOKit `IOPS*`; memory pressure via
`sysctl kern.memorystatus_vm_pressure_level` (1 normal, 2 warn, 4 critical). New base dependency
`pyobjc-framework-Quartz`, darwin only. Off macOS: `NullSignals`; dev/test only: `FileSignals`
(`MINION_EXECUTOR_FAKE_SIGNALS`, requires `MINION_EXECUTOR_DEV=1`). On macOS a source that cannot be
read at startup is exit 2 (fail closed). Mid-run, 3 consecutive failures pause with
`signals_unavailable`.

**D3 Live worker control.** The existing duplex pipe carries `{"op":"ctl", gen, duty, paused, trim}`.
Latest-wins; each `run` message also carries it. The worker applies it at the next checkpoint (also
mid pacer-sleep) and acks with a tick-shaped message, so liveness and the GPU share come from the same
channel. Outside a run the worker applies ctl silently, never replies. Inference stays in the worker
(invariant 7); the control side only sends ctl.
*Implemented in TASK-5.3* (`worker/control.py`, `WorkerHandle.control/last_ack/gpu_share`): `control()`
assigns the generation, stores the state (re-sent with every `run`, so a respawned worker is never out
of date) and writes under a send lock, never under the call lock. The ack is a tick,
`{"tick", "ack": gen, "paused", "busy_s", "wall_s"}`; every field of a ctl message, ack and usage is
validated strictly on both sides (a bad ack, an ack for a generation never issued, NaN or a negative
time is a protocol violation: worker killed, `WorkerCrashed`). A run that starts paused does no work
until resumed. While paused the worker blocks in the checkpoint polling the pipe and keeps ticking, so
the inactivity timeout never fires. The pacer sleep is a `conn.poll(timeout)`, so a pause or duty change
cuts it short and the owed sleep is re-derived. Pause time counts as neither busy nor wall time, and a
late wake-up is credited against the next sleep (capped at 0.5 s, so a stall is repaid as a short
catch-up, not a burst), which keeps `busy_s / wall_s` on the target on a loaded machine.
The worker is the party being limited, so its acks, `paused` and `busy_s / wall_s` are **advisory**
(strictly validated, and `busy_s > wall_s` is a protocol violation, but a lying worker is not
detectable from them). Enforcement is control-side: footprint sampling (D7), the OS priority check
(D8, `priority_ok`) and kill. `control()` is bounded end to end: the pipe write lock is waited for at most
`CTL_SEND_TIMEOUT_S` and the write itself runs under a watchdog of the same length; a worker that has
stopped reading its control channel is killed (never left with a partial frame followed by another),
the in-flight call fails `WorkerCrashed`. If `control()` could not take the write lock (a large `run`
frame is being written), the newest state is kept (`_sent_gen` / `_unsent`) and re-sent by the in-flight
call between replies, so the latest decision is never lost. In-flight Ollama requests cannot be
suspended: a pause stops *new* requests (the runner submits lazily, at most `max_batch_size`
outstanding, with a checkpoint before each) and the requests already sent finish during the pause.

*Pacing requires synchronous GPU completion (TASK-5.6 NF-8 finding).* The pacer measures busy time as
wall time between checkpoints. mlx-lm pipelines the next token with `mx.async_eval`, so at a
checkpoint the GPU still holds queued work that runs through the pacer's sleep (measured: balanced
duty 0.5 gave 74-76 % GPU share on qwen2.5-0.5b, larger batches worse). Runners therefore drain the
GPU before the checkpoint that ends a pacing quantum (`mx.synchronize` incl. mlx-lm's generation
stream; HF: `torch.mps.synchronize()`). At duty 1.0 nothing is synced and async pipelining stays on.

*Quantum pacing (TASK-5.6 round 3).* Syncing at every step at duty < 1 serialises CPU graph build and
GPU compute (no pipelining), so the paced share fell to about duty x G/(G+C): measured light 14-15 %
(expected 23 %), balanced 27-31 % (expected 40 %), i.e. the owner's granted duty was wasted. The pacer
therefore accumulates busy time across steps and only ends a *quantum* when it reaches `QUANTUM_S`
(50 ms): that step syncs the GPU first (so the sync counts as busy), then sleeps
`busy x (1 - d) / d` (oversleep credit capped at 0.5 s) and resets the accumulator. Between quanta
there is neither a sync nor a sleep, so pipelining works inside a quantum and the GPU holds at most one
quantum of queued work (<= the quantum plus one step) when the sleep starts: invariant 6 and NF-8(a)
(share <= duty + 10 pp) hold with a bounded overshoot. Runners ask `StepControl.will_sleep()` before
the checkpoint and sync only when it is True (controls without it sync every step, the safe side).
Pause and control-message reading still happen at every checkpoint (NF-9 reaction is unchanged). A
live duty drop (1.0 -> 0.5) makes the next step a quantum end regardless of accumulated time:
`will_sleep()` applies pending control messages first. A message arriving between `will_sleep()` and
`checkpoint()` is applied at the checkpoint and takes effect one step later: the pacer latches the
`will_sleep()` answer, so that step does not sleep unsynced (a duty drop forces the next one). Ollama is not
controllable: the daemon owns the GPU queue, so its duty is best-effort (request-level pacing only).

**D4 Budget per batch.** Every `run` carries the current `Budget`. `Runner.set_budget` re-applies
memory/wired limits and batch caps; `Runner.trim` frees allocator caches. The loop re-slices records
with the current `max_batch_size` before each batch (FR-54 "next batch").
*Implemented in TASK-5.3* at the worker level: `WorkerHandle.run(..., budget=)` sends it, the worker
calls `set_budget` only when it differs from the one in force and re-applies CPU priority; all six
runners implement `set_budget`/`trim` (MLX: memory, cache and wired limits; HF: MPS memory fraction;
Ollama and fake: the fan-out cap, the daemon owns the memory). The duty the worker paces at is
`min(control duty, Budget.gpu_duty_cycle)`: the governor can make a batch stricter than its budget,
never looser (invariant 6; enforced worker-side on every ctl and at run start). The loop passes the
budget per batch from TASK-5.5.

**D5 Pause semantics (FR-53).** Pause applies at once: no new acquires; the worker pauses at its next
checkpoint. **Resume needs the condition clear for 10 s** (`RESUME_AFTER_S`). Profile escalation keeps
the 60 s hold of ARCHITECTURE §3.6; de-escalation is immediate. If the governor stays paused for 60 s,
the in-flight subtask is interrupted and released. Memory pressure `warning` pauses and trims;
`critical` interrupts, releases and unloads at once. **Deviation** from ARCHITECTURE ("escalate only
after 60 s"): without the 10 s resume window a 5 s thermal blip would always end in a release at 60 s.
**No ramp after resume** (decided in TASK-5.4/5.5): the escalation timer keeps running while paused,
and the applied budget stays the held per-dimension minimum through a pause. The normal 60 s timer is
the only ramp, so a pause longer than 60 s resumes straight at the target budget. No work ran in
between and every de-escalation was already applied at once, so there is nothing to ramp towards.
*Wired in TASK-5.5* (`capacity/runtime.py`, `loop.py`): the pause gates the acquire loop and the model
load / next batch; a watcher task beside each subtask sees `release_now` and `pause_expired` (both
level-triggered, so the actions are idempotent) and interrupts + releases. A pause that ends inside
60 s simply lets the same attempt continue (the worker blocked in its checkpoint, heartbeats kept
going).
Reason priority for the single `paused_reason`: `user_paused` > `control_invalid` > `memory_pressure` >
`thermal` > `low_power_mode` > `low_battery` > `on_battery` > `signals_unavailable` > `in_call`. `control_invalid`
is the fail-closed pause for a present-but-unusable `control.json` (D11). On battery with
`allow_on_battery` and an unknown percentage also pauses (`low_battery`). Automatic profile changes
compare the resolved budgets per dimension (memory, duty, batch, CPU priority): no higher anywhere
applies at once; anything else (no lower anywhere, or mixed) is held 60 s. While held, the budget is
the per-dimension minimum of the current and target budgets (invariant 6): dimensions that go down
drop at once, those that go up wait; `Decision.profile` keeps the current profile name until the
hold ends, then the full target applies. The comparison is against the budget actually in force
(`GovState.applied`), so a dimension lowered by a hold is not raised again before its own 60 s,
even if the target is the current profile (no snap-back).

**D6 Downgrade (FR-54, ADR-0009).** When `budget.memory_gb` drops below the loaded model's
`memory_gb`, the loop finishes the current batch (bounded by 60 s), releases the subtask (executor-side
code `BUDGET_DOWNGRADE`, a release, not a failure), unloads the worker (Ollama `keep_alive 0`, then
kill), and frees the cache pin. An idle warm worker whose model no longer fits is unloaded at once.
*Implemented in TASK-5.5*: the batch loop compares the loaded model with `decision.budget` before every
batch (and before the model load); a model over budget ends the subtask at that boundary with
`BUDGET_DOWNGRADE` (release, `worker.unload()`, cache unpin). If the running batch has not ended
`MINION_EXECUTOR_DOWNGRADE_BATCH_S` (60) after the drop, the watcher interrupts the worker. The idle
check also unloads a warm worker under memory pressure (`trim`), without waiting for `idle_unload_s`.
The idle back-off sleep wakes on governor changes, so this happens within one poll.

**D7 Memory guard (NF-8 safety net).** `mx.set_memory_limit` is a soft guideline in MLX and RSS also
holds the Python heap, so the control side samples the worker footprint (darwin `proc_pid_rusage`,
linux `/proc/<pid>/statm` resident set; `worker/footprint.py`, TASK-5.3) every poll, through
`WorkerHandle.footprint_bytes()`, which samples first and then checks the child is still alive (an
unreaped child keeps its pid, so a recycled pid is never mistaken for the worker). On macOS this is
`phys_footprint` from the `rusage_info_v0` flavor: the oldest flavor, present on every supported
macOS, and the same counter Activity Monitor charges (compressed and GPU-wired memory included, which
RSS misses). More than `memory_gb * 1.10` on 2 consecutive samples
interrupts the worker and fails the subtask as **retryable `OOM`**: another Mac with a bigger budget
may fit it, and `max_attempts` bounds the retries. *Implemented in TASK-5.5* in the per-subtask
watcher (every `MINION_EXECUTOR_SIGNAL_POLL_S`; `None` = no live worker resets the count). The guard is
never suspended. It measures against the budget the *running batch* started with (a drop that
still fits the model, say full to light, only takes effect at the next batch boundary, so the worker
legitimately sits between the two): `max(batch budget, live budget)` for as long as the live budget is
below the batch's. OOM fails only when the footprint exceeds 1.1x the budget the batch started with.
If the batch is still running `downgrade_batch_s` after the drop and the footprint is over 1.1x the
live budget, that is the owner's policy, not the subtask's fault: the subtask is released
(`BUDGET_DOWNGRADE`, no attempt burnt), not failed. A downgrade is bounded by D6. The Ollama daemon's memory is outside the worker
and stays the documented caveat of ADR-0009.

**D8 CPU priority.** `Budget.cpu_priority` (`background|low|normal`, optional on the wire so older
clients are still accepted; the executor always sets it). In the worker: `background` is the Darwin
background QoS (`setpriority(PRIO_DARWIN_PROCESS, 0, PRIO_DARWIN_BG)`, what `taskpolicy -b` does) plus
nice 15; `low` is nice 10; `normal` is 0. Raising priority is impossible unprivileged, so it takes
effect at the next worker spawn (conservative; invariant 6 safe). `OMP_NUM_THREADS=2` for non-normal.
*Implemented in TASK-5.3* (`capacity/priority.py`): the worker applies the priority of each run's budget
after reading back what the OS holds. Raising niceness works live (it only ever goes up); the Darwin
background QoS is reversible both ways; lowering nice (to a higher priority) is `EACCES`, so that part
waits for a respawn. No sandbox rule is needed: the profile is `(allow default)` plus denials, none of
which cover `setpriority`/`nice` on the process itself (tested inside the Seatbelt sandbox).
At spawn the niceness and `OMP_NUM_THREADS=2` come from the latest `cpu_priority` the handle has seen
(`ensure_model` / `run` budget; the constructor's `nice` only until a budget names one): `OMP_NUM_THREADS=2`
whenever that priority is `low` or `background` (an absent one counts as `low`).
Enforcement is control-side: `WorkerHandle.priority_ok()` reads the worker's nice (and, on macOS, its
background flag from `proc_bsdinfo`; `getpriority(PRIO_DARWIN_PROCESS, other_pid)` reads 0 for any pid
but our own) and returns False if the worker is less throttled than the budget in force. The governor
wires it in TASK-5.5: the watcher polls it with the footprint; a False after the grace window
(60 s, about one batch, counted from the subtask start and from each CPU priority change, because the
worker applies a new priority only when its next run starts) interrupts the worker and releases the
subtask (`PRIORITY_VIOLATION`, with the normal failure back-off so a faulty host cannot churn).
The fetch subprocess nice derives from the same value (TASK-5.1: `nice_for`).
Rollout order: upgrade the server before the executors. `Budget` is `extra=forbid`, so an old
server answers 422 to a new executor's `cpu_priority`.

**D9 Configuration (FR-51/52/53).** `~/.config/minion/executor.toml` (or `MINION_EXECUTOR_CONFIG`),
parsed with `tomllib` into a Pydantic model with `extra=forbid`: `active_profile`, `idle_profile`,
`idle_after_min`, `allow_on_battery`, `min_battery_percent`, `on_call` (`auto|light|pause|ignore`,
default `auto`, D16), `[profiles.<name>]` partial overrides,
`[profiles.custom]` (all dimensions required), `[[schedule]]` (profile, `HH:MM` start/end, optional
days). The file is refused (exit 2) if it is not owned by the user or is group/world-writable, or if
its directory is. The 75 % RAM cap always applies, to every profile including `custom`. Schedules use
local wall-clock time, a documented exception to the UTC rule because they are a human calendar.

**D10 `run --profile`.** Giving `--profile light|balanced|full|custom` pins that profile: no adaptive
switching, auto-pause still applies. **Omitting it means adaptive** (FR-52, defaults `light` while
active, `full` while idle on AC). **Deviation** from SPEC FR-51, which called `balanced` the default:
`balanced` becomes the pinned choice owners opt into, not the implicit one. An adaptive run starts on
`active_profile` and switches live since TASK-5.5.

**D11 Owner control (FR-56).** `control.json` / `status.json` files in a 0700 state dir
(`MINION_EXECUTOR_STATE_DIR`), written atomically. The directory is created 0700; an existing one must be owned by the user and
not group/world-writable (it is not chmod-ed). Files are refused if not owned by the user or writable
by others. Works with launchd and the later menu-bar app; no socket. **Implemented in TASK-5.4:**
`minion-executor pause [--for 30m|1h|2h30m]`, `resume` and `profile <name|auto>` do a read-modify-write
of `control.json` (strict schema, `extra=forbid`; tmp file + fsync + rename, 0600, state dir created
0700). All file operations are relative to one vetted directory fd and never follow symlinks. A
`control.json` that is present but not ours, group/world-writable, oversized, not regular, unreadable,
invalid or of a newer version is **unusable**: the governor fails closed (paused `control_invalid`,
automatic profile) until it is fixed or replaced by `resume`; a missing file or directory is just
absent. The reader returns a reason and does not log (5.5 logs once per change). `profile <x>` on an
unusable file keeps the pause (with a warning) instead of dropping it silently. An owner `resume` lifts the owner pause at once, the 10 s
clear window applies to signal pauses only. `decide` takes `total_gb`, `local_now` and an optional
`--profile` pin as arguments so it stays free of I/O and clocks.
**Wired in TASK-5.5** (`capacity/runtime.py` `GovernorRuntime`, `capacity/status.py`): `run` first
requires the state dir to be creatable/safe (exit 2 otherwise) and the signal source readable (exit 2),
then the runtime polls every `MINION_EXECUTOR_SIGNAL_POLL_S` with one source object for the whole run
(the one-read-in-flight bound is per source) through `read_with_timeout`, reads `control.json` on one
daemon thread under a timeout (at most one read in flight; while it hangs the file counts as unusable
and no further thread starts), and seeds `decide` from `initial_state` (a cold
`GovState()` has no applied budget and would skip the hold). Until its first poll finishes, and after
an unexpected error in a poll, the decision is paused `signals_unavailable` (fail closed); errors
lasting `pause_release_after_s` set `pause_expired`, so an in-flight subtask is released. A published
decision older than `max(10 s, 3 x poll)` (the polling task is stuck) reads as that same fail-closed
decision, and if the governor task ends the loop stops acquiring and `run` exits 1. The loop
takes the budget from `Decision.budget` only (a hold lowers it below `budget_for(profile)`). A signal
read problem and an unusable control file are logged once per change, not per poll.
`status.json` (atomic, 0600, same dir) is rewritten on every decision change, on subtask start/finish
and every 10 s, with a `stopped` marker on exit; `minion-executor status` prints it and exits 1 when
the file is missing, `stopped`, older than 30 s or its pid is gone (2 on an unsafe dir). It holds ids
and numbers only (profile and source, budget, "held below nominal" when the budget in force is under
the profile's nominal one, candidate, paused reason, current subtask id/attempt/progress, GPU share,
coarse signals, today's subtasks/records of this process; typed fields, control characters are
stripped when printing); never a payload, URL, acquirement id or token. One executor per state dir:
`run` takes an exclusive `flock` on `executor.lock` (0600) in it and a second one exits 2; only the
lock holder writes the `stopped` marker.

**After inference (TASK-5.5 fix round).** Once the batches are done the watcher can no longer end the
subtask: a verdict (pause expired, memory critical, downgrade timeout, a crashed watcher) is moot, the
result exists and upload / complete proceed. It then only unloads the idle worker for memory
enforcement. A verdict that races the work finishing completes the result. A watcher that crashes
while inferring is itself a verdict (`WATCHER_FAILED`: interrupt and release). The D8 grace window
restarts when a batch starts with another CPU priority, not on a decision change. Presence and every
acquire/heartbeat carry the profile, so the server can infer from adaptive profile changes that the
owner is away from the keyboard; pin a profile (`run --profile`, `profile <name>`) to avoid that
(SPEC S-11).

**D12 Presence (FR-55).** `POST /api/v1/executor/presence {budget, paused_reason?}` returns 204 and
updates the caller's own executor row (profile, budget, paused_reason, `last_seen_at`). It carries no
acquirement id: it never touches a subtask (invariant 3), and bounded fields keep payloads out (S-5).
`acquire` clears `paused_reason`. The executor sends it on entering a pause and every 60 s while
paused. No migration: the columns already exist. *Sent by the loop since TASK-5.5*: on entering a pause,
whenever the pause reason or budget changes while paused, and every
`MINION_EXECUTOR_PRESENCE_INTERVAL_S` (60, max 110). A failed ping is retried after 10 s; 401/403 end
the run like a refused acquire. While paused the loop does not acquire.

**D13 Graceful stop (FR-17).** First SIGTERM/SIGINT stops acquiring; the in-flight subtask may finish
within `MINION_EXECUTOR_STOP_GRACE_S` (30). A second signal or the deadline interrupts and releases.

**D14 Never block sleep.** No `caffeinate`, no IOPM assertions in product code.

**D16 Call-aware governor (amendment, TASK-6.10, FR-53 optional).** `SignalSnapshot.call_active` is
true when a process runs audio input or a camera is running. Microphone: input-only detection via
CoreAudio process objects (`kAudioHardwarePropertyProcessObjectList` + `kAudioProcessPropertyIsRunningInput`,
macOS 14+, through ctypes), falling back to the default input device's
`kAudioDevicePropertyDeviceIsRunningSomewhere` when the process list is unavailable. The fallback has
false positives (output on a combined headset/USB device, an always-open microphone app). An
always-open microphone app (dictation, noise suppression) sets `IsRunningInput` on the process-object
path too, so under `auto` it keeps an adaptive executor at light for as long as it runs; use
`on_call = "ignore"` in that case. Cameras:
CoreMediaIO `IsRunningSomewhere` per device. These are public property reads; they open no device, so
there is no TCC prompt and no indicator. Device names/UIDs are never read. Residual risk: loading
CoreMediaIO may load admin-installed third-party DAL plug-ins into the control process.
The probe runs on its own daemon thread (`AsyncCallProbe`), apart from the safety read: one read in
flight, a 0.5 s hang limit; the 15 s max age only caps a hung read's old value. A hung, failed or
stale read gives `None` (unknown), treated as not in a call, and can never cause
`signals_unavailable`. `MacSignals.read` starts the probe read, takes the safety reads, then waits
for the rest of the 0.5 s limit for the completion event, so a new call state is seen on the very next poll (NF-9); a read already hung is not
waited for. The probe is built lazily and skipped (no thread, no CoreMediaIO load, `call_active`
`None`) when `on_call = "ignore"`: `GovernorRuntime` tells the source each poll
(`set_want_call_state`). `call_probe_unavailable` is logged once when the probe yields `None`,
including a first read that never settles.
`on_call` in `executor.toml`: `auto` (default) caps to light only while the profile is adaptive (not
pinned by `run --profile`, not owner-chosen with `minion-executor profile`); `light` caps always;
`pause` pauses with `in_call`, the lowest-priority reason, with the usual 10 s resume hysteresis;
`ignore` disables it. While the cap is in force the decision and budget report plain `light` (numbers:
per-dimension minimum of light, the target and the budget in force, so a lower dimension stays lower),
so on the wire it is an ordinary drop to light, not a "full with light numbers" fingerprint. The cap
applies at once and clears the escalation candidate; afterwards the budget rises after the 60 s hold
(owner/pinned choices return at once). The dev signal file accepts an optional `call_active`.
An explicit owner/`--profile` choice under `on_call = "light"` is capped against light and its
target only, so it follows the owner at once in both directions. `status.json` (local, 0600) carries
`call_active` and `call_capped` (set by the governor when the cap applied) and renders
`[light: in call]`; neither is sent to the server.
Privacy: the drop to light is visible to the server (SPEC S-11). Pinning a profile hides calls under
`auto`, but `on_call = "light"` on a pinned executor reveals them the same way. The light "cover" only
hides calls if light is otherwise reachable (true by default); if the owner sets active and idle
profiles both above light, light appears only in calls: pin a profile or use `on_call = "ignore"`.
 `pause` mode reveals `paused_reason = in_call`, which operators and owners choose knowingly.
Not verified here: real microphone/camera activation (we deliberately do not start them).

### Gap 1: how NF-8 is measured

NF-8 says "GPU busy share within ±10 pp of `gpu_duty_cycle`". Read literally this fails for any
workload that is partly CPU-bound (tokenization, I/O): the pacer can only add idle time, so the busy
share can be below `duty` however well the governor works. We therefore split the requirement:

1. **Hard bound (the invariant-6 check):** busy share ≤ `gpu_duty_cycle` + 10 pp. Exceeding the
   budget is the violation.
2. **Accuracy:** busy share within ±10 pp of `gpu_duty_cycle × unpaced_share`, where `unpaced_share`
   is the busy share of the same workload with the pacer off (duty 1.0). This checks the control law
   without penalizing CPU-bound phases the pacer cannot compress.

Memory: worker footprint ≤ `memory_gb` × 1.10, enforced by D7 and measured in TASK-5.6.

**How it is measured (TASK-5.6).** `minion-executor bench --budget-check` runs, per profile, the real
worker path (sandboxed worker, runner) and compares three sources: the worker's own GPU time, from
`ioreg -r -l -w 0 -c AGXDeviceUserClient` (the worker pid's summed `accumulatedGPUTime` delta over a
phase window divided by its wall time, sampled ~2 Hz in the bench process; the sandboxed worker
cannot exec; the first 1.5 s of a phase are dropped). The device-wide `Device Utilization %` (IOAccelerator)
is reported but not judged: on a Mac in use it moves with other apps, the display and DVFS (a first
run showed an idle device at 30-43 % and an unpaced load only +33 pp, so a baseline-subtracted
device figure failed `light` at 32 % against 20 % expected). The worker
footprint sampled at 10 Hz from the control side (`phys_footprint`, D7); and the pacer's own
`busy_s / wall_s` (advisory: the worker is the party being limited). Phases: idle (device info), unpaced
(duty 1.0, giving `unpaced_share`; skipped at duty 1.0, where there is no accuracy verdict), a 2 s
gap, paced at the profile duty. `unavailable` rows are not judged; an `error` row fails the check. The verdicts are the two checks above
plus memory, computed by the pure `nf8_verdict`. NF-9 reaction time is asserted in CI (20 trials, fake
engine, `FileSignals`, `tests/test_governor_reaction.py`) and measured on a Mac with the production poll
(docs/bench/governor-measurements.md); NF-4 with `bench --cpu-check`.

### Gap 2: how NF-7 is measured

NF-7 (no visible degradation in a call; UI frame drops and app-launch time within 10 % of baseline)
needs people. We add a **synthetic proxy**, `minion-executor bench --foreground-check` (TASK-6.9), and
say plainly what it is: it measures **GPU scheduling latency**, not compositor frame drops. A small
child process (not an inference worker: no task data, no network) issues a fixed fp16 matmul, sized
to about 5 ms on the idle GPU, on an absolute 30 fps schedule and records completion minus
*scheduled* start; a frame misses when that exceeds 33.3 ms, and slots skipped because it fell
behind count as misses. App-launch time is the wall time of N warm relaunches of Calculator via
`osascript` (includes osascript startup; never done if Calculator is running before any launch, and
only PIDs started by us are signalled; "unavailable" on a TCC/osascript error, and then the row is
judged on frames only and marked partial; available in one window only is an error row).
The probe child runs `mlx` outside the Seatbelt sandbox (it handles no task data, its environment
is scrubbed, and the bench is run by the Mac's owner only).

Per (model, profile, repeat): a fresh worker, warm-up, a **baseline** window (worker loaded, idle),
then a **loaded** window with the worker running back-to-back batches at the profile budget. Thresholds
(per profile, median over repeats): frame p95 ratio loaded/baseline <= 1.10, miss-rate increase
<= 1.0 pp, launch p50 ratio <= 1.10. Only `light` and `balanced` are judged; `full` is informational.
A loaded window where the worker used < 2 % of the GPU is an error row, so an idle worker cannot pass.
This is a stand-in for the compositor and a call app; the probe and the worker share one GPU with
separate command queues. It does not replace the human study
([docs/usability/nf7-video-call-protocol.md](../usability/nf7-video-call-protocol.md)), which stays
open. Procedure: docs/bench/governor-measurements.md.

## Measurements

> M3 Pro (36 GB, macOS 27.0.1), TASK-5.6: docs/bench/m4-governor-apple-m3-pro.{json,md} (NF-8, NF-9 CI) and docs/bench/m4-governor-cpu-apple-m3-pro.json (NF-4). The first per-process run found a pacing bug (mlx-lm async_eval work running during the pacer sleep: qwen balanced 74–76 % at duty 50 %), fixed by quantum pacing; see the .md for the history.

| Check | Result | Verdict |
|---|---|---|
| NF-8 hard bound (share <= duty + 10 pp), light / balanced / full | qwen2.5-0.5b-mlx 21–22 % / 31–35 % / 77–78 %; minilm-mlx 12–13 % / 20–22 % / 54–55 % (per-process GPU time, 3 repeats) | pass (18/18 rows) |
| NF-8 accuracy (+-10 pp of duty x unpaced share) | qwen light 21–22 vs 22–23 %, balanced 31–35 vs 39–41 %; minilm light 12–13 vs 10 %, balanced 20–22 vs 28–29 %; errs low | pass (12/12 paced rows) |
| NF-8 memory (footprint <= 1.1 x memory_gb) | peak 0.45–2.09 GB vs 9.0 / 14.4 / 25.2 GB | pass |
| NF-9 CI, 20 trials, poll 0.25 s: max | 0.433 s (on battery 5 %); p50 0.10–0.25 s per event | pass (< 5 s) |
| NF-9 Mac, production poll 2 s: memory pressure / user return / thermal+power (FileSignals) | not run on this Mac (needs a person at the keyboard and induced swap); opt-in procedure in docs/bench/governor-measurements.md; bound by poll (≤ 4 s cap) + one step + ack | open (opt-in) |
| NF-4 control-process CPU, idle / paused (60 s) | 0.134 % / 0.162 % (`bench --cpu-check`); whole `run` process tree (control + worker, `ps` procedure) not recorded here | pass for the control process; whole tree open |
| Q5 (adaptive duty from total GPU utilization): answered by the data above? | Partly: device-wide utilization is 31–39 % on an idle desktop and moves with DVFS, so a policy keyed on total GPU utilization would need a per-process signal (AGX accumulatedGPUTime works) and its own experiment; Q5 stays open | |

## Consequences

- SPEC FR-51/FR-52 default wording changes (balanced is no longer the implicit default); NF-8 is
  restated as the two checks above (SPEC NF-8 carries the restated text since TASK-5.6).
- A pause shorter than 10 s of clear signals never resumes early; a flapping signal costs at most
  one release per 60 s of pause.
- One new darwin-only dependency (D2); Linux CI stays green through `NullSignals` / `FileSignals`.
- Reaction to signals is bounded by poll interval + one engine step (NF-9); TASK-5.6 measured it (see Measurements).
- Open: Q5 adaptive total-GPU mode stays off; the Measurements section records the data for it.
