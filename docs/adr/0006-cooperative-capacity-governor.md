# ADR-0006: Cooperative capacity governor

- Status: **Draft** (written in TASK-5.1; finalized with measurements in TASK-5.6)
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
`MINION_EXECUTOR_SIGNAL_POLL_S` (2 s, range 0.5..5) in `asyncio.to_thread`. The decision function
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
Reason priority for the single `paused_reason`: `user_paused` > `control_invalid` > `memory_pressure` >
`thermal` > `low_power_mode` > `low_battery` > `on_battery` > `signals_unavailable`. `control_invalid`
is the fail-closed pause for a present-but-unusable `control.json` (D11). On battery with
`allow_on_battery` and an unknown percentage also pauses (`low_battery`). Automatic profile changes
compare the resolved budgets per dimension (memory, duty, batch, CPU priority): no higher anywhere
applies at once; anything else (no lower anywhere, or mixed) is held 60 s. While held, the budget is
the per-dimension minimum of the current and target budgets (invariant 6): dimensions that go down
drop at once, those that go up wait; `Decision.profile` keeps the current profile name until the
hold ends, then the full target applies.

**D6 Downgrade (FR-54, ADR-0009).** When `budget.memory_gb` drops below the loaded model's
`memory_gb`, the loop finishes the current batch (bounded by 60 s), releases the subtask (executor-side
code `BUDGET_DOWNGRADE`, a release, not a failure), unloads the worker (Ollama `keep_alive 0`, then
kill), and frees the cache pin. An idle warm worker whose model no longer fits is unloaded at once.

**D7 Memory guard (NF-8 safety net).** `mx.set_memory_limit` is a soft guideline in MLX and RSS also
holds the Python heap, so the control side samples the worker footprint (darwin `proc_pid_rusage`,
linux `/proc/<pid>/statm` resident set; `worker/footprint.py`, TASK-5.3) every poll, through
`WorkerHandle.footprint_bytes()`, which samples first and then checks the child is still alive (an
unreaped child keeps its pid, so a recycled pid is never mistaken for the worker). On macOS this is
`phys_footprint` from the `rusage_info_v0` flavor: the oldest flavor, present on every supported
macOS, and the same counter Activity Monitor charges (compressed and GPU-wired memory included, which
RSS misses). More than `memory_gb * 1.10` on 2 consecutive samples
interrupts the worker and fails the subtask as **retryable `OOM`**: another Mac with a bigger budget
may fit it, and `max_attempts` bounds the retries. The Ollama daemon's memory is outside the worker
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
wires it in TASK-5.5, with a grace window after a budget change.
The fetch subprocess nice derives from the same value (TASK-5.1: `nice_for`).
Rollout order: upgrade the server before the executors. `Budget` is `extra=forbid`, so an old
server answers 422 to a new executor's `cpu_priority`.

**D9 Configuration (FR-51/52/53).** `~/.config/minion/executor.toml` (or `MINION_EXECUTOR_CONFIG`),
parsed with `tomllib` into a Pydantic model with `extra=forbid`: `active_profile`, `idle_profile`,
`idle_after_min`, `allow_on_battery`, `min_battery_percent`, `[profiles.<name>]` partial overrides,
`[profiles.custom]` (all dimensions required), `[[schedule]]` (profile, `HH:MM` start/end, optional
days). The file is refused (exit 2) if it is not owned by the user or is group/world-writable, or if
its directory is. The 75 % RAM cap always applies, to every profile including `custom`. Schedules use
local wall-clock time, a documented exception to the UTC rule because they are a human calendar.

**D10 `run --profile`.** Giving `--profile light|balanced|full|custom` pins that profile: no adaptive
switching, auto-pause still applies. **Omitting it means adaptive** (FR-52, defaults `light` while
active, `full` while idle on AC). **Deviation** from SPEC FR-51, which called `balanced` the default:
`balanced` becomes the pinned choice owners opt into, not the implicit one. Until the governor is wired
(TASK-5.5) an adaptive run starts on `active_profile`.

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
clear window applies to signal pauses only. `signals.py`, `governor.py` and `control.py` exist and are
unit-tested but are wired into the run loop in TASK-5.5; `decide` takes `total_gb`, `local_now` and an
optional `--profile` pin as arguments so it stays free of I/O and clocks.

**D12 Presence (FR-55).** `POST /api/v1/executor/presence {budget, paused_reason?}` returns 204 and
updates the caller's own executor row (profile, budget, paused_reason, `last_seen_at`). It carries no
acquirement id: it never touches a subtask (invariant 3), and bounded fields keep payloads out (S-5).
`acquire` clears `paused_reason`. The executor sends it on entering a pause and every 60 s while
paused. No migration: the columns already exist.

**D13 Graceful stop (FR-17).** First SIGTERM/SIGINT stops acquiring; the in-flight subtask may finish
within `MINION_EXECUTOR_STOP_GRACE_S` (30). A second signal or the deadline interrupts and releases.

**D14 Never block sleep.** No `caffeinate`, no IOPM assertions in product code.

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

## Consequences

- SPEC FR-51/FR-52 default wording changes (balanced is no longer the implicit default); NF-8 is
  restated as the two checks above (TASK-5.6 updates the SPEC text with the data).
- A pause shorter than 10 s of clear signals never resumes early; a flapping signal costs at most
  one release per 60 s of pause.
- One new darwin-only dependency (D2); Linux CI stays green through `NullSignals` / `FileSignals`.
- Reaction to signals is bounded by poll interval + one engine step (NF-9); TASK-5.6 measures it.
- Open: Q5 adaptive total-GPU mode stays off; TASK-5.6 records the data for it.
