---
id: TASK-5
title: 'M4: Capacity governor'
status: In Progress
assignee: []
created_date: '2026-10-15 14:02'
updated_date: '2026-10-16 18:23'
labels:
  - milestone
  - M4
dependencies: []
ordinal: 5000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Profiles, macOS signals, duty-cycle throttle, auto-pause, budget reporting, downgrade-release. SPEC §4.6, ARCHITECTURE §3.6.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 NF-8 budget adherence measured
- [ ] #2 NF-9 reaction <5s measured
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
# TASK-5 (M4) Capacity governor: plan

## 1. Goal and acceptance criteria (restated)
The executor enforces the owner's capacity budget live. It reads macOS signals (HID idle time, power
source and battery, Low Power Mode, thermal state, memory pressure) and owner commands. A pure
governor turns those inputs into a budget, a pause decision and a release decision. The control loop
then applies the result:
- duty cycle and pause at the next generation step;
- batch size, memory limits and CPU priority at the next batch;
- downgrade-release and unload when the memory budget drops below the loaded model;
- the state is reported on acquire, heartbeat and presence.

The M1 carry-overs also land here:
- FR-17: a 30 s grace period on stop.
- FR-51: `executor.toml` overrides and a `custom` profile.
- `cpu_priority` as a Budget field, with background QoS (`taskpolicy -b` equivalent).

AC (parent):
- #1 NF-8 measured: GPU busy share within +-10 pp of `gpu_duty_cycle`, worker RSS <= `memory_gb` + 10 %.
- #2 NF-9 measured: the governor reacts to memory-pressure, thermal and user-return events in < 5 s (pause new steps).

SPEC mapping:
- FR-17 graceful stop -> 5.2.
- FR-50/51 dimensions and profiles, toml, custom, cpu_priority -> 5.1 and 5.3.
- FR-52 adaptive switching and schedules -> 5.4 (pure) and 5.5 (wired).
- FR-53 auto-pause -> 5.4 and 5.5.
- FR-54 mid-subtask changes and downgrade -> 5.3 and 5.5.
- FR-55 reporting and presence -> 5.1 and 5.5.
- FR-56 owner CLI (`profile`, `pause`, `resume`, `status`) -> 5.4 and 5.5.
- FR-18 -> all.
- NF-4 idle CPU < 1 % (checked again with polling on) -> 5.6.
- NF-8 and NF-9 -> 5.6.
- S-5 (no payload in status or presence) -> 5.1 and 5.5.
- ARCH §3.6 levers 1-4 -> 5.3 and 5.5.
- ADR-0009 (`ollama.py:172`, "a budget downgrade (M4) will reuse the same path") -> 5.5.

Out of scope (do not absorb):
- TASK-9 (Range-signed GET, liveness-by-progress), TASK-10 (pools), TASK-11 (lease renewal), TASK-12 (IPC/Ollama hardening).
- FR-42 fleet view and Grafana panels (M5); the menu-bar app (M6).
- FR-53 "foreground apps" (optional in SPEC): proposed follow-up, below.
- Q5 adaptive total-GPU mode: stays off; 5.6 records data for it.

## 2. Touchpoints (current code)

**Executor control loop**, `packages/minion-executor/src/minion_executor/loop.py`:
- The budget is fixed at construction, `self._budget` :164. It is read at:
  - acquire :244;
  - admission :293;
  - the Heartbeater budget :312;
  - batch slicing :528;
  - `ensure_model` :547-550.
- Stop path :318-335 kills and releases at once; `_complete` checks `stopping` :427.
- Idle unload :273-286.

**Worker**, `worker/process.py`:
- Forced env :65-70.
- `_Ctl` (StepControl, static pacer, ticks) :129-159.
- `worker_main`: nice :189-191; budget fixed at load :229; `_Ctl` built from the load budget :245.
- `WorkerHandle.__init__` (nice) :298-333; spawn args :356-371.
- `_call` tick handling :432-438; `run` :527-551; `stats` :553.
- `unload`/`_graceful_reset` (Ollama keep_alive 0, then kill) :576-602; `interrupt` :626.

**Pacer**, `worker/throttle.py:8-38`: `DutyCyclePacer`, duty is immutable.

**Profiles**, `capacity/profiles.py`:
- `ProfileSpec.nice` :20; `PROFILES` :23-27.
- `parse_profile` rejects custom :39-40.
- `budget_for` :44-53 (no cpu_priority).

**Heartbeater**, `heartbeat.py`: the budget is fixed :62-73; `pause_fn` exists but is unused :66/:90.

**CLI**, `main.py`:
- `_run`: `budget_for` :161; worker nice :201; fetch nice :222; signal handlers :226-227.
- `--profile` :238-240, parsed :266; `status` stub :282-286; bench nice :361/:372/:383.

**Settings**, `settings.py:15-54` (MINION_EXECUTOR_*).

**Runners**:
- Protocol: `runners/base.py:58-77` (StepControl, Runner).
- `mlx_common.py:43-65`: `apply_memory_limits`, `clamp_wired_limit`.
- `mlx_completion.py`: batch caps :126-127; checkpoints :113/:146.
- `hf.py`: memory fraction :78, `empty_cache` :102.
- Runner classes: FakeRunner (`fake.py:168`), HfEmbeddingRunner (`hf.py:111`), HfCompletionRunner (`hf.py:215`), MlxCompletionRunner (`mlx_completion.py:36`), MlxEmbeddingRunner (`mlx_embedding.py:166`), OllamaRunner (`ollama.py:115`).

**Common**, `packages/minion-common/src/minion_common/`:
- `schemas.py`: `Budget` :52-58; `AcquireRequest` :188-190; `HeartbeatRequest` :240-245.
- `enums.py`: `CapacityProfile` :53-59.
- `client.py`: ExecutorClient :133-160 (no `presence`).

**Server**, `packages/minion-server/src/minion_server/`:
- `api/executor.py:88-116`.
- `repositories/executors.py`: `touch` :111-126 (does not clear paused_reason); `report_state` :130-149.
- `services/acquisition.py:42-48`; `services/heartbeat.py:55-61`.
- `db/models.py:193-198`: profile, budget jsonb and paused_reason already exist, so no migration is needed.

**Tests and config**:
- `tests/test_cli.py:31-35` asserts that custom is rejected (must be flipped).
- `tests/test_e2e.py:245-279` (SIGTERM releases at once; needs the grace env).
- `tests/test_runner_worker.py:131-170` (pacer).
- Root `pyproject.toml`: mypy overrides :57-66, markers :73-79; `conftest.py:41-67` (skip logic).
- CI (`.github/workflows/ci.yml`) is ubuntu only and runs all tests, including integration, with docker.

## 3. Key decisions (ADR-0006 "Cooperative capacity governor", drafted in 5.1, finalized with data in 5.6)

**D1 Governor placement.**
- It is an asyncio task in the control process.
- Signals are polled every `MINION_EXECUTOR_SIGNAL_POLL_S` (2.0, range 0.5..4) in `asyncio.to_thread`.
- No NSWorkspace or dispatch notifications: polling bounds detection at 2 s, which leaves NF-9 headroom. This deviates from ARCH §3.6 "+ notifications".
- `governor.py` is pure: `decide(snapshot, config, owner_ctl, now, prev_state) -> Decision`.

**D2 Signal sources (`capacity/signals.py`).**
- HID idle: `Quartz.CGEventSourceSecondsSinceLastEventType(kCGEventSourceStateHIDSystemState, kCGAnyInputEventType)`.
- Thermal state and Low Power Mode: `Foundation.NSProcessInfo.processInfo().thermalState()` / `.isLowPowerModeEnabled()`.
- Power source and battery %: IOKit `IOPSCopyPowerSourcesInfo`, `IOPSCopyPowerSourcesList`, `IOPSGetPowerSourceDescription`, bound with `objc.loadBundleFunctions`.
- Memory pressure: `sysctlbyname("kern.memorystatus_vm_pressure_level")` through ctypes (1 normal, 2 warn, 4 critical). It is verified readable unprivileged on this Mac.
- New base dependencies: `pyobjc-framework-Quartz` (pulls Cocoa and core), `sys_platform=='darwin'` only.
- mypy overrides for `Quartz`, `Foundation`, `objc`: `ignore_missing_imports`, `follow_imports=skip`.
- Off macOS: `NullSignals` (AC, nominal, user active).
- Dev and test only: `FileSignals` reads a JSON snapshot from `MINION_EXECUTOR_FAKE_SIGNALS`. It needs `MINION_EXECUTOR_DEV=1`, otherwise exit 2. This is the seam that makes signals fakeable in CI and on the Mac.
- On macOS, a source that cannot be read at startup gives exit 2 (fail closed, invariant 6). Mid-run, 3 consecutive read failures pause with reason `signals_unavailable`; one failure keeps the last value.

**D3 Live worker control channel.**
- The existing duplex Pipe carries `{"op":"ctl","gen":n,"duty":d,"paused":bool,"trim":bool}`.
- `WorkerHandle.control(state)` sends it lock-free under a new `_send_lock`, shared with `_call`'s send. It is latest-wins: the handle stores the state, and each spawn and each `run` message carries it, so a fresh worker is never out of date.
- Inside a run, `_Ctl.checkpoint` polls `conn.poll(0)`. The pacer sleep becomes `conn.poll(timeout)`, so a pause or duty change interrupts a pacer sleep immediately.
- While paused, the worker blocks in checkpoint and keeps sending ticks (no inactivity timeout).
- The worker acknowledges immediately with a tick-shaped message `{"tick": n, "ack": gen, "paused": bool, "busy_s": x, "wall_s": y}`. It has the "tick" key, so `_call` treats it as liveness; the control side records `ack_gen` and `ack_at`, and uses busy_s and wall_s for the GPU share.
- Outside a run, `worker_main` applies ctl silently and never replies (an unsolicited reply would desync `_call`).
- Messages are sent only on change, so the pipe buffer cannot fill (TASK-12 `_send` watchdog stays separate).
- Reaction bound: poll (<= 2 s) + to_thread send (ms) + at most one engine step. Worst case is one prefill chunk; 5.6 measures it.

**D4 Budget per batch.**
- Every `run` message carries the current Budget.
- On change the worker calls the new `Runner.set_budget(budget)`, which re-applies:
  - MLX `set_memory_limit`, cache limit and wired limit;
  - the HF memory fraction;
  - the batch caps (Ollama max_workers too).
- The new `Runner.trim()` frees allocator caches: MLX `mx.clear_cache`, HF `torch.mps.empty_cache`, no-op for Ollama and fake.
- The loop re-slices records with the current `max_batch_size` before each batch. This covers FR-54 "next batch".

**D5 Pause semantics (FR-53).**
- Pause takes effect immediately: no new acquires, and the worker pauses at its next checkpoint.
- Leaving a pause needs the condition to be clear for `RESUME_AFTER_S` = 10 s.
- Profile escalation keeps ARCH's 60 s; de-escalation is immediate.
- If the governor stays PAUSED for >= `PAUSE_RELEASE_AFTER_S` = 60 s, the in-flight subtask is interrupted (worker killed) and released.
- Memory pressure `warning` gives pause + trim. `critical` gives `release_now`: interrupt, release and unload at once.
- The 10 s resume window is a deviation from ARCH ("escalate only after 60 s"). Without it, a 5 s thermal blip would always release the subtask at 60 s. Record in ADR-0006.
- Reason priority for the single `paused_reason`: `user_paused` > `memory_pressure` > `thermal` > `low_power_mode` > `low_battery` > `on_battery` > `signals_unavailable`. A `PausedReason` StrEnum goes in `minion_common.enums`; the wire field stays a pattern string.

**D6 Downgrade (FR-54, ADR-0009).**
- When `budget.memory_gb < a.model.memory_gb`, the loop finishes the current batch at the batch boundary, bounded by 60 s; after that it interrupts.
- It then releases (log code `BUDGET_DOWNGRADE`, executor-side only) and calls `worker.unload()` (`_graceful_reset`: Ollama keep_alive 0, then always kill), then `cache.release_in_use()`.
- An idle warm worker whose model exceeds the new budget is unloaded at once, without waiting for `idle_unload_s`.

**D7 Memory guard (NF-8 safety net).**
- `mx.set_memory_limit` is a soft guideline in MLX, and RSS also includes the Python heap.
- So the control side samples the worker footprint (`worker/footprint.py`) every poll:
  - darwin: `proc_pid_rusage` RUSAGE_INFO_V4 `max(ri_phys_footprint, ri_resident_size)`;
  - linux: `/proc/<pid>/status` VmRSS.
- More than `memory_gb * 1.10` on 2 consecutive samples: interrupt the worker and `fail(retryable, OOM)`. Retryable means another Mac with a bigger budget may fit, and max_attempts bounds it.
- The Ollama daemon memory is outside the worker and stays the documented caveat (ADR-0009).

**D8 CPU priority.**
- `Budget.cpu_priority: CpuPriority | None = None` (`background|low|normal`). The default is None so older clients are not misreported; the executor always sets it.
- In the worker:
  - background = `os.setpriority(PRIO_DARWIN_PROCESS=4, 0, PRIO_DARWIN_BG=0x1000)`, the same call `taskpolicy -b` makes, plus `os.nice` up to 15 (the Linux fallback);
  - low = nice 10;
  - normal = 0.
- Under `spawn` there is no exec wrapper, so the in-process call is the only option.
- Live changes:
  - more nice or BG on: applied live at the next run;
  - BG off: live;
  - lowering nice: impossible unprivileged, so it is applied at the next worker spawn (conservative, invariant 6 safe).
- `OMP_NUM_THREADS=2` for background and low (ARCH §3.6 lever 4) is set per spawn.
- The fetch subprocess nice is derived from cpu_priority.

**D9 Config (FR-51/52/53).**
- File: `~/.config/minion/executor.toml`, overridable with `MINION_EXECUTOR_CONFIG`. Parsed with `tomllib` into Pydantic `ExecutorConfig` (extra=forbid).
- Fields:
  - `active_profile` (light), `idle_profile` (full), `idle_after_min` (10);
  - `allow_on_battery` (false), `min_battery_percent` (30);
  - `[profiles.<name>]` partial overrides of built-ins; `[profiles.custom]` needs all of memory_fraction|memory_gb, gpu_duty_cycle, max_batch_size, cpu_priority;
  - `[[schedule]]` with profile, start "HH:MM", end "HH:MM", days?.
- Schedules use local wall-clock time, a documented exception to the UTC rule because they are a human calendar.
- The 75 % RAM cap always applies.
- The file is refused (exit 2) if it is not owned by the user or is group/world-writable.

**D10 `run --profile`.**
- If given, it pins that profile: no adaptive switching, auto-pause still applies.
- If omitted, it is adaptive (FR-52 defaults light/full).
- This conflicts with FR-51's "balanced (default)". Proposal: balanced is the default for `adaptive = false`. SPEC fix in 5.1 docs plus ADR-0006. Needs orchestrator confirmation.

**D11 Owner control (FR-56) via files, not a socket.**
- `control.json` and `status.json` live in `MINION_EXECUTOR_STATE_DIR` (default `~/Library/Application Support/open-minions`, mode 0700, files 0600, atomic replace).
- `pause [--for 1h]`, `resume`, `profile <name|auto>` write `control.json`; the governor polls it with the signals.
- The loop writes `status.json` on change and every 10 s: budget, paused_reason, profile, current subtask ids, GPU share, today's subtasks and records (local date).
- Control files not owned by the user, or writable by others, are ignored with a warning.
- Simplest option that works with launchd and the later menu-bar app (M6).

**D12 Presence (FR-55).**
- `POST /api/v1/executor/presence {budget, paused_reason?}` returns 204 and reuses `executors_repo.report_state`.
- Sent on entering pause and every `MINION_EXECUTOR_PRESENCE_INTERVAL_S` (60) while paused. 60 s keeps the executor in ARCH §7's "polled < 2 min" window.
- Acquire's `touch` sets `paused_reason = NULL`.
- No migration (columns exist, `models.py:193-198`).
- No server rate limit; it matches acquire, and per-IP limits stay a proxy concern.

**D13 FR-17 grace.**
- The first SIGTERM/SIGINT means no new acquires; the in-flight subtask may finish and complete within `MINION_EXECUTOR_STOP_GRACE_S` (30, range 0..300; 0 = the M1 behaviour).
- At the deadline, or on a second signal: interrupt, release, exit 0.
- `_complete` retries continue until the hard deadline.
- launchd `ExitTimeOut` must be >= grace + 10 s; note this for the M5 plist.

**D14 Never block sleep.** No `caffeinate`, no IOPM assertions in product code. A macos test asserts that `pmset -g assertions` lists no assertion from executor or worker pids during a run. `caffeinate -u` is used only by the NF-9 measurement test.

## 4. Sub-tasks
Order and dependencies:
- 5.1 first; 5.3 depends on 5.1 (cpu_priority).
- 5.2 is independent, but merge it before 5.5 (both touch `loop._handle`).
- 5.4 is independent of 5.2 and 5.3.
- 5.5 needs 5.1-5.4; 5.6 needs 5.5.
- Each sub-task is one branch/PR.

### TASK-5.1 M4.1 Contracts, presence endpoint, executor.toml and custom profile
AC:
- #1 `Budget.cpu_priority` (`background|low|normal`, optional) is sent by the executor on register, acquire and heartbeat; requests without it are still accepted.
- #2 `POST /executor/presence` returns 204 and updates the executor row's profile, budget, paused_reason and last_seen_at. Revoked or invalid token gives 401; a bad paused_reason gives 422. Acquire clears paused_reason. `ExecutorClient.presence` exists.
- #3 `executor.toml` (or `MINION_EXECUTOR_CONFIG`) is loaded and validated: overrides, custom, active/idle, idle_after_min, battery rules, schedules. Unknown keys, bad values or unsafe permissions exit 2; the 75 % cap is always enforced.
- #4 `run --profile custom` works when defined. Worker and fetch nice derive from cpu_priority. The test_cli custom-rejection test is flipped.
- #5 ADR-0006 draft (D1-D14) and the SPEC FR-51/52 default wording (D10) are proposed.

Steps:
- **S1 common + server** (~200 lines incl. tests).
  - `minion_common/enums.py`: `CpuPriority`, `PausedReason`.
  - `schemas.py`: `Budget.cpu_priority: CpuPriority | None = None`; `PresenceRequest(budget: Budget, paused_reason: str | None = Field(None, max_length=64, pattern="^[a-z_]+$"))`.
  - `client.py`: `async def presence(self, req: PresenceRequest) -> None`.
  - Server: `api/executor.py` route `@router.post("/presence", status_code=204)`; new `services/presence.py` (thin, a txn calling `report_state`); `repositories/executors.py touch` adds `paused_reason=None`.
  - Tests:
    - unit: schema round trip;
    - integration: presence updates row; revoked 401; 422 pattern; acquire clears paused_reason; heartbeat budget with cpu_priority stored.
- **S2 executor config + profiles** (~250 lines).
  - New `capacity/config.py`: `class ProfileOverride`, `class ScheduleEntry`, `class ExecutorConfig(BaseModel)`, `def load_config(path: Path | None) -> ExecutorConfig` (with the owner/permission check), `def default_config_path() -> Path`.
  - `capacity/profiles.py`:
    - `ProfileSpec(memory_fraction | memory_gb, gpu_duty_cycle, max_batch_size, cpu_priority)` replaces nice;
    - `def resolve_profiles(cfg) -> dict[CapacityProfile, ProfileSpec]`;
    - `budget_for(profile, total_gb=None, specs=PROFILES)` sets cpu_priority;
    - `def nice_for(p: CpuPriority) -> int`;
    - `parse_profile(name, specs)` accepts custom when defined.
  - `settings.py`: `config_path: Path | None`.
  - `main.py`: load config before `restrict_environ`; use specs and nice_for at :161/:201/:222/:361-383.
  - Tests: `test_config.py` (valid, unknown key, perms, custom incomplete, cap, schedule parse); test_cli updated.
- **S3 docs**: ADR-0006 draft, SPEC §4.6 status, `.env.example` (`MINION_EXECUTOR_CONFIG`).

Invariants:
- 1: presence is REST.
- 3: presence has no acquirement and mutates only the caller's own executor row (principal), never a subtask.
- 4/S-5: bounded fields only.
- 6: cap enforced, unsafe config refused.

### TASK-5.2 M4.2 FR-17 graceful stop with grace period
AC:
- #1 On SIGTERM/SIGINT the executor stops acquiring. An in-flight subtask that finishes within `MINION_EXECUTOR_STOP_GRACE_S` (default 30, 0..300) is completed. Otherwise the worker is killed at the deadline and the subtask released. Exit 0.
- #2 A second signal stops immediately (kill + release). Grace 0 behaves as M1.
- #3 e2e: SIGTERM during a short subtask ends SUCCEEDED; during a long one, RELEASED within grace + 5 s.

Steps:
- **S1** (~150 lines).
  - `settings.py`: `stop_grace_s: float = Field(30, ge=0, le=300)`.
  - `loop.py`:
    - `stop()` becomes first call: set `_stop` and schedule `_hard_stop.set` after grace via `loop.call_later`; second call: set `_hard_stop` now;
    - `_handle` waits on {work, beat, hard_stopper} instead of the stopper (:318-335);
    - `_complete` (:427) checks `_hard_stop`;
    - backoff `_pause` keeps waking on `_stop`.
  - `main.py:226-227`: unchanged handler; pass grace to the loop ctor.
  - Unit tests in `test_loop.py`: finishes in grace, then complete and no release; exceeds grace, then interrupt + release at the deadline (fake clock or short grace); double stop; grace 0.
- **S2**:
  - `test_e2e.py:245` sets `MINION_EXECUTOR_STOP_GRACE_S=1` for the long-subtask case and adds a short-subtask completes-on-SIGTERM case.
  - Docs: SPEC FR-17 status, ARCH §2.2 Stop bullet :179-181, CLAUDE.md run line.

Invariants:
- 3: release uses the current acquirement_id.
- 5: an interrupted attempt's output is never completed.
- 6: bounded grace, worker killed at the deadline.

### TASK-5.3 M4.3 Live worker control: governor-driven pacer, pause, per-batch budget, CPU priority, footprint
AC:
- #1 `WorkerHandle.control(WorkerCtl)` changes duty and pause of a running batch at the next checkpoint (also mid pacer-sleep). The worker acks the applied generation within one step. Ticks continue while paused, so no inactivity timeout.
- #2 Every `run` carries the current Budget. `Runner.set_budget` re-applies memory/wired limits and batch caps (mlx, hf, ollama, fake); `Runner.trim()` frees caches. The gpu-marked tests show `mx` limits updated.
- #3 `cpu_priority` is applied in the worker (D8), including inside the Seatbelt sandbox (macos test: worker `getpriority` and PRIO_DARWIN_BG state as expected after a live change).
- #4 `worker/footprint.py`: `process_footprint_bytes(pid) -> int | None` (darwin proc_pid_rusage, linux /proc).
- #5 The pacer reports busy_s/wall_s. A CI test with fake runner `sleep_steps` measures duty within +-3 pp of target at 0.3/0.5/1.0 (control-law check).

Steps:
- **S1 IPC** (~250 lines).
  - `throttle.py`: `DutyCyclePacer.set_duty(d)`, accumulated `busy_s` and `wall_s`; injectable `wait(seconds) -> bool` instead of sleep, so a ctl message ends the wait early.
  - `process.py`:
    - `@dataclass(frozen=True) class WorkerCtl(gen: int, duty: float, paused: bool, trim: bool = False)`;
    - `_Ctl` polls conn for `op=="ctl"`, applies it, blocks while paused (sending ticks every tick_s), and sends the ack tick;
    - `worker_main` handles ctl outside runs silently; `run` op reads `msg["budget"]` and `msg["ctl"]`;
    - `WorkerHandle.control(ctl)` is lock-free with `_send_lock`; `last_ack() -> tuple[int, float] | None`; `gpu_share()` from ack/tick busy and wall;
    - `run(batch, params, on_progress, budget=None)` sends the budget and current ctl;
    - spawn passes the initial ctl.
  - Update the `Worker` protocol in `loop.py:106-119` (add `control`; `run` gets `budget`).
  - Tests in `test_runner_worker.py`: pause mid-batch stops progress, then resume; duty change applied; ctl while idle produces no stray reply; ack gen; paused > step_timeout gives no crash.
- **S2 runners** (~150 lines).
  - `base.py` Runner protocol: `def set_budget(self, budget: Budget) -> None`, `def trim(self) -> None`.
  - Implement in the six runner classes (MLX: `apply_memory_limits` + `clamp_wired_limit` + `self._budget`; HF: memory fraction + `self._budget`; Ollama/fake: store).
  - `worker_main` calls `set_budget` when the run budget differs, and `trim` on a ctl with trim.
  - Tests: fake unit; gpu tests for mlx/hf limits.
- **S3 CPU priority and footprint** (~150 lines).
  - `capacity/profiles.nice_for` plus a new `worker/priority.py` (`apply_cpu_priority(p, current) -> applied`, darwin BG via `os.setpriority(4, 0, 0x1000)`).
  - Called at worker start before the sandbox (replaces the nice arg at :189) and on budget change in `run`.
  - `FORCED_WORKER_ENV` and engine env: `OMP_NUM_THREADS=2` for non-normal.
  - `worker/footprint.py`.
  - Tests: unit (mapping, monotonic nice), macos sandbox test (`test_sandbox_macos.py` pattern), footprint on linux and darwin.

Invariants:
- 6: pause and duty are live, memory limits per batch.
- 7: all engine calls stay in the worker; the control side only sends ctl.

Risks:
- Sandbox might deny setpriority after confinement. Fallback: apply at spawn only, with escalation and de-escalation at the next spawn; document.
- PRIO_DARWIN_BG may throttle Metal heavily. That is intended for `background`; quantify in 5.6.

### TASK-5.4 M4.4 macOS signals, pure governor, owner control commands
AC:
- #1 `signals.py` returns `SignalSnapshot(hid_idle_s, on_ac, battery_percent, low_power_mode, thermal: Thermal, memory_pressure: MemPressure, taken_at)` from real macOS APIs (D2). Live macos test reads plausible values. `NullSignals` off macOS; `FileSignals` only with DEV=1. Pure mapping helpers are unit-tested on Linux.
- #2 `governor.decide(...)` is pure and implements:
  - FR-52: active/idle on AC, schedules, owner profile override;
  - FR-53: pause reasons and priority, warning = pause+trim, critical = release_now, battery rules, Low Power Mode, `signals_unavailable`;
  - hysteresis (D5): de-escalate now, escalate after 60 s, resume after 10 s clear, pause-release after 60 s.
  Table-driven tests with a fake clock cover every transition.
- #3 `minion-executor pause [--for DURATION]`, `resume`, `profile <light|balanced|full|custom|auto>` write `control.json` atomically (0600, in a 0700 state dir); unsafe files are ignored. CLI tests.
- #4 New deps are locked (`uv.lock`), CI on Linux stays green (mypy overrides).

Steps:
- **S1 signals** (~220 lines).
  - `capacity/signals.py`: `class SignalSource(Protocol): def read(self) -> SignalSnapshot`; `MacSignals` (lazy pyobjc imports inside), `NullSignals`, `FileSignals`; `def default_source(settings) -> SignalSource`.
  - Pure helpers: `thermal_from_ns(int)`, `pressure_from_sysctl(int)`, `power_from_iops(dict)`.
  - `minion-executor/pyproject.toml` deps; root `pyproject.toml` mypy overrides (`Quartz`, `Quartz.*`, `Foundation`, `objc`).
  - Tests: `test_signals.py` (pure), `test_signals_macos.py` (macos marker).
- **S2 governor** (~250 lines).
  - `capacity/governor.py`: `@dataclass(frozen=True) class Decision(profile, budget: Budget, paused_reason: PausedReason | None, release_now: bool, trim: bool)`; `GovState` (candidate-since timestamps, paused-since, last escalation); `def decide(snap, cfg, specs, owner: OwnerControl | None, now: datetime, local_now: datetime, state: GovState) -> tuple[Decision, GovState]`. No I/O, no clocks.
  - Tests: `test_governor.py`, table-driven.
- **S3 owner control** (~150 lines).
  - `capacity/control.py`: `OwnerControl(paused_until: datetime | None, paused: bool, profile: CapacityProfile | None)`; `read_control(dir)`, `write_control(dir, ctl)` (atomic, perms), `state_dir()`.
  - `main.py` commands `pause`, `resume`, `profile`; `--for` parses `30m|1h|2h30m`.
  - Tests: `test_control.py`, `test_cli.py`.

Invariants:
- 6: fail closed on unreadable signals; never sleep-blocking (pure reads only).
- S-5: no payload anywhere.
- Security: control and config files are owner-only.

### TASK-5.5 M4.5 Governor in the executor loop: auto-pause, downgrade-release, reporting, status
AC:
- #1 A governor task runs beside the loop (poll 2 s). Acquire, heartbeat and presence always carry the current budget and paused_reason. While paused there is no acquire, and presence is sent on entering pause and every `MINION_EXECUTOR_PRESENCE_INTERVAL_S` (60). Paused CPU stays < 1 % (NF-4).
- #2 Mid-subtask:
  - duty and pause are pushed to the worker immediately; batch size, memory limits and CPU priority apply at the next batch;
  - pause for >= 60 s, or memory critical: interrupt the worker and release;
  - resume within 60 s continues the same attempt.
- #3 FR-54 downgrade:
  - when memory_gb < the loaded model, finish the batch (bounded 60 s), release, unload (graceful reset incl. Ollama keep_alive 0), unpin the cache;
  - an idle warm worker above budget is unloaded at once.
- #4 Memory guard (D7): worker footprint > memory_gb x 1.1 on two samples: interrupt and `fail(retryable, OOM)`.
- #5 `status.json` plus `minion-executor status` (budget, profile, paused_reason, current subtask ids, GPU share, today's contribution; exit 1 if no executor is running).
- #6 Integration e2e (Linux CI, FileSignals, real server and worker): pause, then no acquire plus a presence row with paused_reason; pause mid-subtask, then resume and SUCCEEDED; pause > 60 s (shortened via ctor arg in tests), then RELEASED; downgrade, then RELEASED and the worker unloaded. macos test: no PM assertions held (D14).

Steps:
- **S1 runtime glue** (~250 lines).
  - New `capacity/runtime.py`: `class GovernorRuntime`:
    - owns the source, cfg, specs, state, current Decision;
    - `async def run(self, stop: asyncio.Event)` polls signals and control;
    - `on_change` callbacks; `budget` / `paused_reason` / `decision` properties;
    - `async wait_unpaused(timeout) -> bool`.
  - `loop.py`:
    - ctor takes `governor: GovernorRuntime` (a static `FixedGovernor` for tests and bench);
    - replace `self._budget` reads (:244, :293, :312, :528, :547-550);
    - when paused, skip acquire and send presence (new `_presence_tick`);
    - Heartbeater gets `budget_fn` + `pause_fn` (`heartbeat.py:62-95`).
  - `main.py _run`: build config, specs, signal source, runtime (`--profile` = pinned); start the runtime task.
  - Unit tests with fakes.
- **S2 mid-subtask policy** (~250 lines).
  - In `loop._handle` add a governor watcher task beside work, beat and hard_stopper:
    - on change: `to_thread(worker.control, ctl)`;
    - `release_now` or paused >= 60 s: interrupt + release (outcome FAILED-free `_Outcome.PAUSED`, no failure back-off);
    - memory guard sampling `footprint` each poll.
  - `_run_batches` re-reads the budget per slice and checks the downgrade (D6) before each batch; `_maybe_unload` also unloads when the loaded model > budget.
  - Unit tests in `test_loop.py` with fake worker and fake governor.
- **S3 status** (~120 lines).
  - `capacity/control.py write_status`/`read_status`; the loop updates on change and every 10 s.
  - `main.py status` replaces the stub (:282-286).
  - Tests.
- **S4 e2e** (test-engineer).
  - `tests/test_governor_e2e.py` (integration) using `MINION_EXECUTOR_FAKE_SIGNALS` + fake runner `sleep_steps`.
  - `tests/test_no_sleep_assertion_macos.py`.

Invariants:
- 2/3: release only by the current owner via acquirement_id; a 409 on release is "already handled".
- 5: interrupted attempts are never completed.
- 6: this sub-task is the core of it.
- 7: inference stays in the worker.

Risk: release cap (`MINION_RELEASE_LIMIT`=10) vs downgrade churn (TASK-3 risk). Hysteresis plus admission keep it low; keep the default and log `BUDGET_DOWNGRADE` and `PAUSE_RELEASE` codes for Grafana later.

### TASK-5.6 M4.6 Measure NF-8 and NF-9, ADR-0006, docs
AC:
- #1 `minion-executor bench --budget-check --profile <p> --model <id> [--duration 60]` measures:
  - GPU busy share: ioreg `AGXAccelerator` `PerformanceStatistics."Device Utilization %"` at 1 Hz, minus a 20 s idle-baseline median;
  - worker peak footprint/RSS (10 Hz sampler + `max_rss_bytes`) vs the budget;
  - the pacer's own busy/wall.
  It prints pass/fail per NF-8. M3 Pro results for light, balanced and full x (Qwen-class completion, BERT embedding) are committed as `docs/bench/m4-governor-apple-m3-pro.{json,md}`.
- #2 NF-9:
  - CI test (no server; real worker subprocess, fake runner 100 ms steps, in-memory fake signals, production 2 s poll): 20 trials per event (memory warning, thermal serious, user return) of inject-to-ack (pause or new duty). Assert max < 5 s; report p50/p95/max.
  - On this Mac (opt-in `reaction` marker, real MLX model):
    - memory pressure via `sudo memory_pressure -S -l warn` (simulated) or unprivileged `memory_pressure -l warn` under `timeout 60`;
    - user return via `caffeinate -u -t 1`, or a manual keypress, with T0 = observe time minus HIDIdleTime (exact back-dating);
    - thermal and power via FileSignals (not inducible safely; the real readers are unit- and live-read tested).
  - Results go in the same doc.
- #3 NF-4: idle and paused executor CPU < 1 % over 60 s (`ps -o %cpu` samples) is recorded.
- #4 ADR-0006 is finalized (incl. the NF-8 interpretation below). SPEC §4.6 / FR-17 / FR-56 / NF-8 status, §6.2 presence implemented, Q5 (answered or still open with data), ARCH §2.2/§3.6/§9, CLAUDE.md commands and `.env.example` are synced.

Steps:
- **S1 meters + bench mode** (~250 lines).
  - New `capacity/gpu_meter.py`: `read_gpu_utilization() -> float | None` via `/usr/sbin/ioreg -r -c AGXAccelerator -d 1 -a` and `plistlib`, in the control/bench process only (the worker sandbox forbids exec).
  - `bench.py`: `run_budget_check(...)` + `BudgetCheckResult`; `main.py bench --budget-check`.
  - Unit tests with canned ioreg plist and a fake sampler.
- **S2 NF-9 tests**: `tests/test_governor_reaction.py` (CI); `tests/test_governor_reaction_macos.py` (markers `macos` + `reaction`, opt-in in `conftest.py` like `model`).
- **S3 run on this Mac, commit results** (verifier).
- **S4 ADR-0006 + docs** (docs-keeper).

Measurement setup, this Mac (M3 Pro 36 GB, mlx + hf, no Ollama):
- Close heavy apps; on AC.
- Use bench-models.yaml entries already cached from M3.
- `--profile full|balanced|light`, 60 s steady state each, 3 repetitions; report mean +- sd.
- RSS check also runs with `max_batch_size` at the profile cap.

CI (Linux):
- NF-9 and the pacer-law duty check with the fake runner.
- The GPU share and footprint parsers are unit-tested with canned data.
- The real GPU numbers are Mac-only (documented, attached to the PR as CLAUDE.md requires for performance claims).

## 5. SPEC/ARCH gaps and decisions needing an ADR (ADR-0006)
1. **NF-8 at duty 1.0 and for CPU-bound steps.** The pacer sleeps `t(1-d)/d` on wall time, including CPU and tokenizer time, so the measured GPU share is about `d * s1` (s1 = unpaced share), which can be < d - 10 pp for small models.
   - Proposed reading: the hard bound is share <= d + 10 pp; the +-10 pp check is against `d * s1`; both are reported.
   - Needs SPEC wording. Alternative: measure GPU-synchronized step time (ARCH §3.6 says `mx.synchronize`), which needs a runner-side timing hook. Decide after the 5.6 data.
2. **NF-8 "RSS" on unified memory.** Use `max(phys_footprint, resident)`. The MLX memory limit is soft, hence the control-side guard (D7). The Ollama daemon is not covered (ADR-0009 caveat).
3. **FR-51 "balanced (default)" vs FR-52 adaptive defaults** (D10). Proposal: omitting `--profile` means adaptive. Confirm.
4. **Pause resume hysteresis 10 s vs ARCH 60 s escalation** (D5), so blips do not trigger the 60 s release.
5. **Presence interval unspecified**: 60 s (D12). **Paused-reason vocabulary**: enum in common (D5).
6. **Polling instead of notifications** for signals (D1).
7. **Schedules in local time**, an exception to the UTC rule (D9).
8. **CPU priority**: in-process `setpriority(PRIO_DARWIN_BG)` instead of a `taskpolicy -b` exec (spawn), and nice escalation only at the next spawn (D8).
9. **Owner control via files in a state dir** (D11); CLI `profile auto` added.
10. **FR-54 "finish the current batch"** is bounded at 60 s; memory critical interrupts mid-batch (D5/D6).
11. **launchd ExitTimeOut >= grace + 10 s** for the M5 plist (D13).
12. **Q5** stays open unless the 5.6 data answers it.

## 6. Risks
- IOKit power functions through `objc.loadBundleFunctions` may need signature tweaks. Fallback: parse `pmset -g batt` every 10 s (power changes are not part of NF-9).
- `caffeinate -u` may not reset CGEventSource HID idle. Fallback: a manual keypress with back-dated T0 (still exact).
- A long prefill step (8 x 8k-token prompts) can exceed the 5 s budget for "pause new steps". 5.6 measures the worst case. If it fails, cap the prefill chunk with `prefill_step_size` in the mlx runner via `set_budget` (follow-up).
- Pacer pause and TASK-9(a) "liveness only on progress": a paused worker does not progress. When TASK-9(a) lands it must exempt paused acks; note this on TASK-9.
- New pyobjc dependency size and lock churn. Darwin-only markers keep Linux CI unaffected.

## 7. Proposed follow-ups (not created; orchestrator decides)
- `backlog task create "FR-53 optional: foreground-app rules (zoom.us, screen sharing) -> light or pause" -l executor,M4-followup`
- `backlog task create "Q5 adaptive duty from total GPU utilization (ioreg) if NF-7/NF-8 data needs it" -l executor`

## 8. Sub-task create commands (do not run; orchestrator decides)
```
backlog task create "M4.1 Contracts, presence endpoint, executor.toml and custom profile" -p task-5 -l M4 --ac "Budget.cpu_priority sent on register/acquire/heartbeat; old clients accepted" --ac "POST /executor/presence 204 updates profile/budget/paused_reason/last_seen_at; 401 revoked; 422 bad reason; acquire clears paused_reason" --ac "executor.toml validated (overrides, custom, active/idle, battery, schedules); unknown keys/perms/bad values exit 2; 75% cap" --ac "run --profile custom works; nice derived from cpu_priority" --ac "ADR-0006 draft and SPEC FR-51/52 default wording proposed"
backlog task create "M4.2 FR-17 graceful stop with 30 s grace" -p task-5 -l M4 --ac "SIGTERM: no new acquires; in-flight finishes within grace -> completed, else killed and released at deadline; exit 0" --ac "Second signal stops immediately; grace 0 = M1 behaviour" --ac "e2e: short subtask SUCCEEDED on SIGTERM; long subtask RELEASED within grace+5 s"
backlog task create "M4.3 Live worker control: governor-driven pacer, pause, per-batch budget, CPU priority, footprint" -p task-5 -l M4 --ac "WorkerHandle.control changes duty/pause at next checkpoint incl. mid pacer-sleep; ack per generation; no timeout while paused" --ac "run carries Budget; Runner.set_budget/trim in all runners; gpu tests show mlx limits updated" --ac "cpu_priority applied (PRIO_DARWIN_BG/nice) incl. inside sandbox" --ac "footprint sampler darwin/linux" --ac "pacer busy/wall within +-3 pp of target in CI"
backlog task create "M4.4 macOS signals, pure governor, owner control commands" -p task-5 -l M4 --ac "MacSignals reads HID idle, power/battery, Low Power Mode, thermal, memory pressure; NullSignals off macOS; FileSignals dev-only" --ac "governor.decide pure: FR-52, FR-53, hysteresis, pause-release timer, signals_unavailable; table tests" --ac "pause [--for], resume, profile <name|auto> write control.json atomically 0600" --ac "deps locked; Linux CI green"
backlog task create "M4.5 Governor in the executor loop: auto-pause, downgrade-release, reporting, status" -p task-5 -l M4 --ac "acquire/heartbeat/presence carry live budget+paused_reason; paused -> no acquire, presence on enter + every 60 s" --ac "mid-subtask duty/pause pushed at once; pause>=60 s or memory critical -> interrupt+release; resume continues attempt" --ac "FR-54 downgrade: finish batch (<=60 s), release, unload (Ollama keep_alive 0), unpin cache; idle worker above budget unloaded" --ac "memory guard: footprint > 1.1x budget twice -> fail retryable OOM" --ac "status.json + minion-executor status" --ac "integration e2e with FileSignals for pause/resume/release/downgrade; macos no-PM-assertion test"
backlog task create "M4.6 Measure NF-8 and NF-9, ADR-0006, docs" -p task-5 -l M4 --ac "bench --budget-check: GPU share (ioreg, baseline-subtracted) and worker peak footprint vs budget; M3 Pro results committed" --ac "NF-9: CI 20-trial reaction test max < 5 s; Mac opt-in measurements (memory_pressure, user return, FileSignals thermal/power) documented" --ac "NF-4 idle/paused CPU < 1 % recorded" --ac "ADR-0006 final; SPEC/ARCH/CLAUDE.md/.env.example synced"
```
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Carry-over from M1 (TASK-2.3): (1) FR-17 grace period — SIGTERM currently kills worker and releases immediately; spec wants finish-or-release within 30 s grace; (2) FR-51 executor.toml overrides + custom profile not built (parse_profile rejects custom); (3) CPU priority only via nice — add taskpolicy / Budget field.

Decisions (orchestrator, autonomous delivery; record in ADR-0006): D10 omitting --profile = adaptive (FR-52), pinned profiles via --profile; Gap 1 NF-8 hard bound share <= duty+10pp, ±10pp checked vs duty x unpaced share; D5 resume after 10 s clear; D7 memory guard fails as retryable OOM.

The plan remark that the pipe buffer cannot fill is superseded by the bounded ctl send in TASK-5.3 (kill watchdog; ctl is not written while a non-run op is in flight).
<!-- SECTION:NOTES:END -->
