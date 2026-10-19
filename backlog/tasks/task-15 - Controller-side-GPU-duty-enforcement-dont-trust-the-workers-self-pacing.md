---
id: TASK-15
title: Controller-side GPU duty enforcement (don't trust the worker's self-pacing)
status: In Progress
assignee: []
created_date: '2026-10-17 01:56'
updated_date: '2026-10-19 10:14'
labels:
  - executor
  - security
dependencies: []
ordinal: 31000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Duty enforcement relies on the worker pacing itself and reporting busy/wall; a compromised worker (e.g. weights-parser exploit) could skip pacing. Enforce from the control process: SIGSTOP/SIGCONT duty cycling or task_policy/QoS clamp, verified against the per-process ioreg accumulatedGPUTime meter (capacity/gpu_meter.py). From TASK-5.6 security audit.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Control process detects worker GPU share above duty+10pp over a window and throttles or kills it
- [ ] #2 Works without worker cooperation
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
## Goal
The control process enforces the owner's `gpu_duty_cycle` on the worker without trusting the worker: it measures the worker pid's real GPU time itself, throttles it with SIGSTOP/SIGCONT, and kills it if throttling does not work. Today the worker paces itself (`worker/throttle.py`) and reports `busy_s/wall_s`, which ADR-0006 D3 calls advisory (`worker/control.py:11-12`).
- AC#1: share > duty + 10 pp over a window, then throttle, then kill. Maps to invariant 6, FR-50 (gpu_duty_cycle), FR-54, NF-8(a), ADR-0006 D3/D7/D8 pattern.
- AC#2: no worker cooperation. Maps to S-10 and ADR-0009: meter readings come from the kernel's IOKit registry, SIGSTOP cannot be caught, and the sandbox already blocks foreign signals and fork (`worker/sandbox.py:199-205`).

## Spike results (2026-10-18, M3 Pro, mlx 0.32.4; scratch scripts, nothing in the repo)
- **Meter cost.** `ioreg -r -l -c AGXDeviceUserClient` uses 14.4 ms of child CPU per call, which is 0.7 % of a core at 2 s. An in-process IOKit read via ctypes (IOAccelerator, then child iterator, then IOObjectConformsTo("AGXDeviceUserClient"), then the CF properties IOUserClientCreator / AppUsage[].accumulatedGPUTime, keyed by IORegistryEntryGetRegistryEntryID) costs **0.95 ms of CPU** (146 clients). It finds the same pids as ioreg, with values within 3.5 ms. IOServiceGetMatchingServices("AGXDeviceUserClient") finds nothing because user clients are not registered services, so the code must walk the accelerator's children. Summing per pid without per-client positive deltas gives negative shares, so per-client tracking is mandatory (the same rule as `process_gpu_share`).
- **SIGSTOP and Metal.** Tested with mlx 2048^2 matmuls, both pipelined (async_eval, like mlx-lm) and a "deep queue" (200 queued ops): 50/50 ms gating, 500/500 ms gating, 900/100 ms gating, 5 s stops. No Metal errors, rc 0, all runs finished. Gating 50/50 halved the share (0.55 -> 0.28). GPU work that ran *during* stops was 7-107 ms in total for pipelined, but about 160-200 ms per stop for the deep queue: committed command buffers drain while the process is stopped. So gating stops new submissions only, and a deep-queue rogue still needs the kill rung.
- **QoS clamp.** `setpriority(PRIO_DARWIN_GPU, pid, DENY)` returns EPERM without privileges. There is no public GPU priority API, so it is not an option.

## Design (choices made, alternatives rejected)
- **Meter.** In-process IOKit reader in the control process, about 1 ms per read. The worker pid's clients are tracked with per-client positive deltas, and each read is checked against the live child (sample, then is_alive, like `footprint_bytes`, `worker/process.py:872`). ioreg stays for the bench.
- **Guard** (pure state machine). Sliding window W = 10 s, judged once the span is >= 8 s. Limit = allowed + 0.10, where allowed = the max duty in force over [window start - grace 5 s, now] (a duty drop gets ack latency, the D7 pattern). While the governor is paused, after the grace, allowed = 0 (limit 0.10). Duty 1.0 makes the limit >= 1.1, so the guard never acts.
  - over the limit -> GATE: stop_s = clamp(busy_w/allowed - wall_w, 0, 0.75 x guard interval), which brings the window back to the duty, not just to the limit.
  - over the limit continuously for 30 s -> KILL.
  - paused and over the limit -> KILL at once (no gating to 0).
  - more than 4 distinct client ids for the worker pid -> KILL (evasion by opening/closing clients between samples; honest mlx = 1 client).
  - 3 consecutive meter errors during a batch -> release GPU_METER_UNAVAILABLE (fail closed, like D2).
  - Sample times jittered +-20 %.
- **Enforcement ladder.** Rung 1 (SIGSTOP/SIGCONT gating) is safe per the spike, needs no cooperation and keeps an honest-but-mis-pacing worker (the TASK-5.6 async_eval bug) within budget without losing work. Rung 2 (kill) = `interrupt()` + release `GPU_DUTY_VIOLATION`, treated as a local fault (failure back-off, like PRIORITY_VIOLATION), bounded server-side by MINION_RELEASE_LIMIT.
  - Rejected: an IPC pause tier (the cooperation we distrust; ctl already carries duty).
  - Rejected: budget downgrade (an uncooperative worker ignores it).
  - Rejected: a QoS clamp (EPERM).
- **Honest worker under gating.** The pacer counts stopped time as busy and sleeps more afterwards, so gating only ever errs on the safe side. Gating only starts above the limit. Measured honest shares are well below it (qwen balanced 31-35 % vs a 60 % limit).
- **Idle warm worker** (no run in flight): checked on each idle loop pass (every 10-30 s) with allowed = 0. A violation unloads the worker and applies the failure back-off.
- **Off darwin / no AGX.** The reader is None and the guard is inert (CI on Linux, fake engine). On darwin, if the startup probe fails while mlx/hf is advertised, `run` exits 2 (fail closed, like D2 signals). With only fake/ollama advertised it logs and stays inert.
- **Ollama.** The daemon's GPU time is not the worker's, so the guard sees about 0. This is the existing ADR-0009 caveat (documented, no code).
- **Cost.** About 1 ms per 2 s while working (0.05 %), and about 1 ms per idle pass. NF-4 is unaffected; to be measured anyway.

## Steps
### 1. In-process IOKit GPU client reader (about 200 lines)
- `capacity/gpu_meter.py`: add `read_gpu_clients_iokit() -> dict[int, dict[str, int]]`, the same shape as `read_gpu_clients` (`gpu_meter.py:117`), with cid = f"0x{registry_id:x}" (the same id ioreg prints).
- ctypes against IOKit and CoreFoundation, the signals.py style (`capacity/signals.py:18`).
  - **Check every CF type ID before use** (CFGetTypeID vs CFString/CFArray/CFDictionary/CFNumber): a wrong type crashes the control process.
  - Release every io_object/CF ref.
  - Cap the clients iterated (e.g. 100k) and the string buffers.
  - Reuse `_CREATOR` for "pid N, name".
- Raise `GpuMeterError` when there is no IOAccelerator, no AGX clients, or the library is missing.
- `default_gpu_reader() -> Callable[[], dict[int, dict[str, int]]] | None` returns None off darwin.
- Update the module docstring (it currently says "never by the product loop", `gpu_meter.py:16`).
- Tests:
  - unit: creator parse, off-darwin None, CF error paths via an injected fake.
  - `macos` test (skipped without AGX): parity with the ioreg parser on a live pid set; 200 reads leak no ports (task port count stable).

### 2. Pure guard `capacity/gpu_guard.py` (about 150 code + 200 test lines)
```python
@dataclass(frozen=True)
class GpuGuardConfig:
    window_s: float = 10.0; min_span_frac: float = 0.8; margin: float = 0.10
    grace_s: float = 5.0; kill_after_s: float = 30.0; max_stop_frac: float = 0.75
    max_clients: int = 4; max_meter_errors: int = 3
class GpuAction(enum.Enum): OK = "ok"; GATE = "gate"; KILL = "kill"
@dataclass(frozen=True)
class GpuVerdict: action: GpuAction; share: float | None; limit: float | None; stop_s: float = 0.0; reason: str = ""  # reason: over_duty|paused|clients|meter
class GpuDutyGuard:
    def __init__(self, cfg: GpuGuardConfig = GpuGuardConfig()) -> None: ...
    def start(self, pid: int, now: float, *, idle: bool = False) -> None  # fresh window; client set kept per pid
    def note_ctl(self, duty: float, paused: bool, now: float) -> None
    def observe(self, now: float, clients: Mapping[str, int] | None, *, interval_s: float) -> GpuVerdict
    def observe_error(self, now: float) -> GpuVerdict
```
- Per-client positive deltas (same rules as `process_gpu_share`, `gpu_meter.py:208`).
- clients None means no live worker: reset, OK.
- Unit tests with a fake clock and synthetic counters:
  - honest at or below duty -> OK
  - over -> GATE with the formula
  - persistent 30 s -> KILL
  - duty drop mid-window: no false positive within grace + window
  - paused rogue -> KILL after grace
  - churn > 4 ids -> KILL
  - counter reset -> no negative
  - duty 1.0 -> never acts
  - idle mode limit 0.10
  - 3 errors -> KILL(meter)
  - short span -> OK

### 3. Wire detection + kill into the loop (AC#1 "kills", AC#2) (about 250 lines incl. tests)
- `worker/process.py`: add `WorkerHandle.gpu_clients(reader) -> dict[str, int] | None` (sample, then verify alive, like `footprint_bytes` :872; pid :513).
- `loop.py`:
  - `Worker` Protocol (:171-186): add `gpu_clients`.
  - ExecutorLoop ctor (:230-300): add `gpu_reader=None, gpu_guard: GpuGuardConfig | None = None`.
  - `_push_control` (:889) calls `guard.note_ctl`.
  - `_sample_worker` (:908) also returns the GPU clients (same to_thread).
  - `_watch` (:914-1013): `guard.start(pid)` when the first batch begins (`_batch_budget` set, so load is excluded); jitter `next_sample`.
  - `_Verdict` (:104-112): KILL -> new `GPU_DUTY = "GPU_DUTY_VIOLATION"`, meter -> `GPU_METER = "GPU_METER_UNAVAILABLE"`. Both go in the `local` back-off set in `_on_verdict` (:1063).
  - Idle: `_enforce_idle_budget` (:741) also runs the idle guard -> `_unload_idle("gpu_duty_violation")` + `_back_off_after_failure()`.
  - Structured logs (numbers only, S-5).
  - `_status_snapshot` (:1075): prefer the measured share for `gpu_share` when available (the worker figure stays as a fallback).
- `main.py:314-345`: build the reader, probe at startup (exit 2 rule above), pass reader + config.
- Update the fakes in `tests/test_loop.py:193` (FakeWorker) and the e2e fakes.
- Unit tests (fake reader/counters):
  - rogue over the limit -> interrupt + release code GPU_DUTY_VIOLATION + back-off
  - honest -> completes
  - duty drop mid-batch -> no kill
  - paused rogue killed before the 60 s pause_expired
  - idle rogue unloaded
  - reader None -> no-op
  - 3 meter errors -> GPU_METER_UNAVAILABLE
  - verdict after inference done ignored (existing rule)
- Invariants: 6 (budget enforced without cooperation), 7 (still a subprocess; the kill reclaims memory), 3/5 (release only, nothing uploaded on a verdict).

### 4. SIGSTOP/SIGCONT duty gating (rung 1) (about 250 lines incl. tests)
- `worker/process.py`:
  - add `suspend() -> bool` / `resume() -> None` on WorkerHandle: os.kill(pid, SIGSTOP/SIGCONT) only while `proc.is_alive()`, under a new `_sig_lock`; `_suspended` flag.
  - `interrupt` (:986), `kill` (:960) and `_reset` (:1004) take `_sig_lock` around kill+join, so no signal goes to a reaped pid.
  - `close` (:1037) and `_graceful_reset` (:941) SIGCONT first; otherwise a stopped worker costs the 10 s / 5 s timeout.
  - Pids are recycled sequentially; the residual race of an `is_alive()` reap in another thread is documented.
- `loop.py`: GATE -> `_gate(stop_s)`: suspend in a thread, `await gov.wait_change(version, stop_s)` (wakes early on any governor change, so NF-9 holds), then **resume in `finally`**, synchronously (one syscall, also on cancel/verdict). Log `gpu_guard_gate`, rate-limited.
- Stop length: <= 0.75 x guard interval (<= 1.5 s), far below `step_timeout_s` (300 s) and `CTL_SEND_TIMEOUT_S` (ctl frames fit the pipe buffer).
- Orphan safety:
  - `macos` test: a control subprocess suspends its worker and is SIGKILLed; assert the worker is gone within 5 s. XNU's orphaned-pgrp SIGHUP+SIGCONT should handle this, and then the worker's `_watch_parent` (:133) exits.
  - If the test fails: `os.setpgid(0, 0)` at the top of `worker_main` (:280), before confine. Its own pgrp becomes orphaned when the control dies. Side effect: the worker no longer receives terminal SIGINT (fine, the control drives shutdown).
- Unit tests (fake engine, real WorkerHandle, linux + darwin):
  - while suspended no ticks (`ps -o stat=` shows T / /proc stat)
  - resume -> correct output
  - kill/close while suspended return in < 2 s
  - loop: GATE calls suspend then resume, resume on cancel and on verdict

### 5. GPU tests, measurements, docs
- `runners/fake.py:213`: add a test hook `gpu_burn_s` (test_hooks only, imports mlx like `_mlx_matmul` :150): an unpaced matmul loop that never calls `ctl.checkpoint`.
- `tests/test_gpu_guard_gpu.py` (marks `gpu`, `macos`): real IOKit reader + sandboxed WorkerHandle + loop at light (duty 0.3):
  - (a) rogue: gating seen, then KILL + release GPU_DUTY_VIOLATION within 10 + 30 + 10 s
  - (b) paced hook (checkpoint each step): no gate/kill over 40 s
  - (c) 20 s of 10 Hz gating of pipelined mlx: same checksum as ungated, no Metal error
  - Optionally a `model`-marked qwen run under gating.
- `bench_budget.py:318` `_fill_shares`: add `max_window_share` (10 s sliding, from the existing samples) and `gpu_clients` (distinct ids) per row.
- Measurements to attach to the PR:
  - `bench --budget-check` qwen2.5-0.5b-mlx + minilm-mlx at light/balanced/full x3: max 10 s window share < duty + 10 pp with margin (sets W / margin), client count (sets max_clients)
  - `bench --cpu-check` + the `ps` whole-tree procedure with a warm idle worker (NF-4)
  - (c) above on the M3 Pro
- Docs (docs-keeper):
  - **ADR-0006 amendment D17** "Control-side GPU duty guard": meter, ladder, thresholds, SIGSTOP findings, EPERM QoS finding, churn and Ollama residuals; update the D3 "enforcement is control-side" list. No new ADR: it is a governor amendment, not a paper deviation.
  - SPEC FR-50 table enforcement column + FR-54 status, NF-8 note (the run-time guard enforces (a)), S-10 sentence.
  - ARCHITECTURE §3.6 lever 2 + Tests.
  - docs/bench/governor-measurements.md section.
  - CLAUDE.md: no new env vars (constants), so no change unless one is added.

## Risks / open questions
1. **Client-churn evasion.** Clients opened and closed between samples are invisible. Mitigations: the id count cap plus jitter; a client living fraction f of each 2 s interval is caught with probability about f per interval. Spike follow-up: try a per-task cumulative counter (`task_info(TASK_POWER_INFO_V2).gpu_energy.task_gpu_utilisation` via `task_name_for_pid`); if a name port allows it, it closes the hole.
2. **CPU-signalled MTLSharedEvent waits** in mlx/torch (cpu stream -> gpu) could hit a GPU channel timeout during a stop. Not seen with matmuls or 5 s stops. Stops stay <= 1.5 s; test 5(c) and an optional `model` run with mlx-lm/HF check it.
3. **Deep-queue rogue.** Committed work drains during stops (spike: about 30 % of stop time), so gating is partial; kill bounds the exposure to about 10 s detection + 30 s. Option: an early kill when GPU accrued during a stop > 50 % of the stop.
4. **Honest windows near the limit** (prefill bursts, short batches). Must come out of the `max_window_share` data; if needed, widen W (20-30 s) and keep the margin at NF-8's 10 pp. Never raise the margin above NF-8.
5. **Orphaned stopped worker if the control is SIGKILLed** mid-stop. Covered by the step 4 test and the setpgid fallback.
6. **Ollama** is unmetered by design (ADR-0009 caveat). **HF/MPS under SIGSTOP** is not spiked; covered by an optional `gpu` HF test.
7. **Scope.** Steps 1-3 alone satisfy both ACs (kill). Step 4 can be split off if the orchestrator wants a smaller PR:
   `backlog task create "SIGSTOP/SIGCONT duty gating of the worker (GPU guard rung 1)" -p TASK-15 -l executor,security`
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Implemented (commits 2ccbeb3..a354205): capacity/gpu_meter.py in-process IOKit reader (per-client deltas, ~1 ms/read); capacity/gpu_guard.py pure guard (10 s window, duty + 10 pp, KILL after 30 s over, at once when paused/idle, >4 clients per pid KILL, 3 meter errors fail closed); loop.py one continuous guard per worker pid with +-20 % jitter; KILL -> interrupt + release GPU_DUTY_VIOLATION / GPU_METER_UNAVAILABLE (local back-off; idle warm worker unloaded); main.py startup probe (exit 2 if reader exists and first read fails; disabled off darwin / fake engine with one log line); status.json gpu_violation. Tests: test_gpu_guard.py, test_gpu_guard_gpu.py, test_gpu_meter_iokit.py, test_loop.py. Measurements: docs/bench/m-task15-gpu-guard-apple-m3-pro.{md,json}. Docs: ADR-0006 D17, SPEC 4.6 status, ARCHITECTURE 2.2/3.6, CLAUDE.md commands. Open: TASK-15.1 (SIGSTOP gating), Ollama/HF unmetered, client churn, bench file's short-subtask finding predates be3be7f.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Controller-side GPU duty guard shipped: per-worker-pid IOKit meter and pure guard enforce gpu_duty_cycle + 10 pp over 10 s (kill after 30 s over, at once when paused/idle), kill releases GPU_DUTY_VIOLATION. Measured on M3 Pro: honest worst window 22.9 % (light, limit 40 %) and 45.6 % (balanced, limit 60 %). Throttling (TASK-15.1) and Ollama/HF metering remain open.
<!-- SECTION:FINAL_SUMMARY:END -->
