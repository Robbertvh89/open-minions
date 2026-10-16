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

**D4 Budget per batch.** Every `run` carries the current `Budget`. `Runner.set_budget` re-applies
memory/wired limits and batch caps; `Runner.trim` frees allocator caches. The loop re-slices records
with the current `max_batch_size` before each batch (FR-54 "next batch").

**D5 Pause semantics (FR-53).** Pause applies at once: no new acquires; the worker pauses at its next
checkpoint. **Resume needs the condition clear for 10 s** (`RESUME_AFTER_S`). Profile escalation keeps
the 60 s hold of ARCHITECTURE §3.6; de-escalation is immediate. If the governor stays paused for 60 s,
the in-flight subtask is interrupted and released. Memory pressure `warning` pauses and trims;
`critical` interrupts, releases and unloads at once. **Deviation** from ARCHITECTURE ("escalate only
after 60 s"): without the 10 s resume window a 5 s thermal blip would always end in a release at 60 s.
Reason priority for the single `paused_reason`: `user_paused` > `memory_pressure` > `thermal` >
`low_power_mode` > `low_battery` > `on_battery` > `signals_unavailable`.

**D6 Downgrade (FR-54, ADR-0009).** When `budget.memory_gb` drops below the loaded model's
`memory_gb`, the loop finishes the current batch (bounded by 60 s), releases the subtask (executor-side
code `BUDGET_DOWNGRADE`, a release, not a failure), unloads the worker (Ollama `keep_alive 0`, then
kill), and frees the cache pin. An idle warm worker whose model no longer fits is unloaded at once.

**D7 Memory guard (NF-8 safety net).** `mx.set_memory_limit` is a soft guideline in MLX and RSS also
holds the Python heap, so the control side samples the worker footprint (darwin `proc_pid_rusage`,
linux `/proc/<pid>/status`) every poll. More than `memory_gb * 1.10` on 2 consecutive samples
interrupts the worker and fails the subtask as **retryable `OOM`**: another Mac with a bigger budget
may fit it, and `max_attempts` bounds the retries. The Ollama daemon's memory is outside the worker
and stays the documented caveat of ADR-0009.

**D8 CPU priority.** `Budget.cpu_priority` (`background|low|normal`, optional on the wire so older
clients are still accepted; the executor always sets it). In the worker: `background` is the Darwin
background QoS (`setpriority(PRIO_DARWIN_PROCESS, 0, PRIO_DARWIN_BG)`, what `taskpolicy -b` does) plus
nice 15; `low` is nice 10; `normal` is 0. Raising priority is impossible unprivileged, so it takes
effect at the next worker spawn (conservative; invariant 6 safe). `OMP_NUM_THREADS=2` for non-normal.
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
(`MINION_EXECUTOR_STATE_DIR`), written atomically, ignored if not owned by the user or writable by
others. Works with launchd and the later menu-bar app; no socket.

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
