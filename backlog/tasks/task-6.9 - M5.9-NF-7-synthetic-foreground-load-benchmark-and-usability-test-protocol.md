---
id: TASK-6.9
title: 'M5.9 NF-7: synthetic foreground-load benchmark and usability-test protocol'
status: Done
assignee: []
created_date: '2026-10-17 03:33'
updated_date: '2026-10-17 17:49'
labels:
  - M5
dependencies: []
parent_task_id: TASK-6
ordinal: 40000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
runners/fg_probe.py (MLX): deadline probe in its own spawn subprocess runs a fixed ~5 ms GPU kernel every 33.3 ms and records latency. bench_foreground.py: baseline (worker loaded idle) vs loaded at light/balanced (full info) with a real model; app-launch proxy (osascript launch latency N=20). Pure nf7_proxy_verdict: frame p95 <= 1.10x baseline, deadline-miss +<=1 pp, launch p50 <= 1.10x, median over --repeats. bench --foreground-check. docs/usability/nf7-video-call-protocol.md. Results docs/bench/m5-foreground-<mac>.md. NOT VERIFIABLE HERE: the human study.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 nf7_proxy_verdict unit-tested; CI path runs with a fake probe and fake engine
- [x] #2 Mac results for light and balanced on qwen2.5-0.5b-mlx and minilm-mlx committed (derived figures only)
- [x] #3 If the proxy fails at light or balanced, a follow-up is opened and TASK-15/Q5 flagged; thresholds not loosened
- [x] #4 Protocol doc exists; human study marked NOT VERIFIABLE HERE
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
## TASK-6.9 plan: NF-7 foreground proxy + usability protocol (base: main; 6.10 only touches capacity/)

**Goal.** SPEC NF-7 (docs/SPEC.md:534) asks for no visible/audible call degradation, plus UI frame drops and app-launch time within 10 % of baseline under light/balanced. Add a synthetic, bounded proxy `bench --foreground-check` and a written human protocol. AC1 maps to NF-7 (CI logic), AC2 to NF-7 Mac data, AC3 to SPEC Q5 (SPEC.md:547), TASK-15 and ADR-0006:294, AC4 to NF-7 human part (NOT VERIFIABLE HERE).

**Touchpoints (main).** bench_budget.py:257 `_run_phase`, :219 `_FootprintSampler`, :434 `run_budget_check` (pattern to copy: probe engine, cache.ensure, fresh worker per row, release_in_use); bench.py:183 `synthetic_records`, :267 `host_environment`, :293 `_params`; gpu_meter.py:202 `make_sampler`, :213 `process_gpu_share`, :142 `probe_meters`; main.py:753 `bench`, :802 mutual-exclusion, :881 budget-check branch; worker/process.py:489 spawn ctx, :101 `restrict_environ`; tests/test_bench_budget.py:343-385 (fake engine + `ramp` sampler pattern).

### Step 1: pure metrics and verdict (~200 lines)
- New `minion_executor/bench_foreground.py` (no mlx import). Constants: `FRAME_PERIOD_MS = 1000/30`, `RATIO_LIMIT = 1.10`, `MISS_DELTA_PP = 1.0`, `JUDGED = {light, balanced}`.
- `@dataclass(frozen) FrameStats(p50_ms, p95_ms, miss_rate, frames)` and `frame_stats(latencies_ms: Sequence[float], missed: int) -> FrameStats`. Latency = completion minus *scheduled* start, so queueing behind worker kernels counts. A frame misses its deadline if latency > period; frames skipped because the probe fell behind count as missed.
- `@dataclass PhaseMeasure(frames: FrameStats, launch_p50_s: float | None)`. `Nf7Verdict(frame_p95_ratio, miss_delta_pp, launch_p50_ratio, frame_ok, miss_ok, launch_ok, passed)`. `nf7_proxy_verdict(baseline: PhaseMeasure, loaded: PhaseMeasure) -> Nf7Verdict` (if launch is None, launch_ok is None and the row is judged on frames, marked partial). `aggregate(rows) -> dict[(model, profile), Nf7Verdict]` takes the median of each metric over repeats, then applies the thresholds. `full` rows are informational (verdict computed, never judged).
- Tests (unit), new tests/test_bench_foreground.py: thresholds exactly at 1.10 and +1.0 pp pass, just above fail; zero-baseline guards; percentile on small samples; median over 3 repeats (one bad outlier passes, two fail); full is never judged.

### Step 2: frame probe in runners/ (~200 lines)
- New `runners/fg_probe.py` (mlx stays inside runners/, per CLAUDE.md conventions). `class FrameProbe(Protocol): def calibrate(self) -> int; def run(self, duration_s: float, size: int) -> ProbeResult` with `ProbeResult(latencies_ms: list[float], missed: int)`.
- `MlxFrameProbe` starts its own `mp.get_context("spawn")` child with a scrubbed env (`scrub_env`). The child imports mlx lazily and runs an fp16 matmul `size x size` plus `mx.eval` on an absolute 33.3 ms schedule. `calibrate()` binary-searches the size to about 5 ms median on the idle GPU; the size is fixed per row (baseline and loaded use the same one). Bounds: duration 1..600 s, parent `join(duration+30)` then kill, at most 18,000 frames. Only floats go over the Pipe, no payloads. The probe runs at default nice/QoS, like a foreground app.
- Not an inference worker (invariant 7 is about inference). It handles no data and has no network.
- Tests: `tests/test_fg_probe_gpu.py` (`@pytest.mark.gpu`, 2 s run, calibrated latency within 2..15 ms). Unit test for the schedule/skip math with an injected clock and a fake step.

### Step 3: app-launch proxy (~120 lines, in bench_foreground.py)
- `measure_app_launch(n: int, *, bundle_id="com.apple.calculator", proc_name="Calculator", run=subprocess.run) -> list[float] | None`. Fixed argv, no shell: `/usr/bin/osascript -e 'tell application id "<id>" to launch'`, timed with monotonic, timeout 15 s. Then `pgrep -u <uid> -x Calculator`, SIGTERM that pid, wait up to 5 s until it is gone, and leave a 1 s gap. One warm-up launch is excluded. Returns None (unavailable) if Calculator was already running (the owner's app is never killed), on non-darwin, or on a TCC/osascript error.
- Unit tests with a fake `run`: already running gives None and sends no kill; timeout handling; N samples.

### Step 4: orchestration + CLI (~280 lines)
- `ForegroundCheckConfig(duration_s=60, launches=20, repeats=3, max_tokens=128, seed=0)` with the same bounds style as bench_budget.py:188. `run_foreground_check(cache, worker_factory, entries, budgets, cfg, *, probe: FrameProbe, launcher, sampler_factory=make_sampler, meter_probe=probe_meters, environment=None) -> ForegroundReport`.
- Per (entry, profile, repeat): fresh worker, then ensure_model and a warm-up (excluded). Calibrate (first row only), settle 2 s, then the **baseline** window: probe `duration_s`, then the N launches, with the worker loaded and idle. Next, a `_LoadThread` loops `worker.run(batch, params, budget)` until a stop Event (do not refactor `_run_phase`). Settle 2 s, then the **loaded** window (same probe + launches), then stop.
- The worker GPU share in the loaded probe window comes from `process_gpu_share`. If it is below 0.02, the row becomes error "background load did not run", so a pass cannot come from an idle worker. `render_foreground_table` and `to_json` hold derived figures only (no timestamps, no per-frame arrays).
- main.py: `--foreground-check` and `--launches` flags, added to the mutual-exclusion sum at :802. Reuses `--profiles/--duration/--repeats/--max-tokens/--out`, and the cache/sandbox/`budget_factory` code from the budget-check branch. Exit 1 if any judged aggregate fails, 2 if the probe or GPU meter is unavailable.
- Tests: fake engine end-to-end (`WorkerHandle([Engine.FAKE])`, `FakeFrameProbe` returning scripted latencies, fake launcher, `ramp` sampler). Cases: pass, fail at light, full informational, idle-worker anomaly turns the row into an error. CLI test that it is exclusive with --budget-check and that a bad --launches exits 2.

### Step 5: docs (docs-keeper; ~150 lines)
- New `docs/usability/nf7-video-call-protocol.md`, headed **NOT VERIFIABLE HERE**. It covers N>=8 participants on their own Macs, a 2x2 within-subject design (executor off / light / balanced / off, randomized and blinded), 15-min Teams/Zoom calls with screen share, and the call-aware governor (TASK-6.10) disabled for the study arm or recorded. Measures: Zoom/Teams stats panel (fps, jitter), a WindowServer frame-rate trace via Quartz Debug or `sudo powermetrics` noted, launch time of 3 apps, and a 5-point MOS plus a "noticed degradation" yes/no. Pass rule: NF-7 thresholds, and "noticed" not above the off arm. Consent, no recording of call content, results template.
- docs/bench/governor-measurements.md: new NF-7 section with the command, about 60-min duration, and caveats (the probe is a GPU-latency proxy, not compositor frame drops). ADR-0006: add "Gap 2: how NF-7 is measured" (proxy definition and thresholds) and Measurements rows. SPEC NF-7: point to the proxy and the protocol. CLAUDE.md commands line and the ARCHITECTURE.md:549 M5 line.

### Step 6: Mac measurement (orchestrator, M3 Pro, AC power, Calculator closed, no call running)
```
uv run minion-executor bench --foreground-check --models deploy/bench-models.yaml \
  --model qwen2.5-0.5b-mlx --model minilm-mlx --profiles light,balanced,full \
  --duration 60 --launches 20 --repeats 3 --out docs/bench/m5-foreground-apple-m3-pro.json \
  | tee docs/bench/m5-foreground-apple-m3-pro.md
```
Estimated ~70 min (2 models x 3 profiles x 3 repeats x ~3.8 min). Commit the .json and .md (derived figures only) and the summary in ADR-0006 Measurements.
- **AC3:** if light or balanced fails, keep the thresholds. Open `backlog task create "NF-7 proxy fails at <profile>: <metric>" -p TASK-6`, add a TASK-15 note, and update SPEC Q5 with the data. AC2 stays checked (data committed); NF-7 stays open.

**Risks / open questions.** (1) An MLX probe and an MLX worker share one GPU but run separate command queues. The proxy is a scheduling-latency stand-in for the compositor; say so in docs. (2) osascript `launch` may trigger a TCC Automation prompt on first use; the orchestrator approves it once, otherwise launch is "unavailable" and only frames are judged (partial; record it). (3) The call-aware governor from 6.10 does not engage, since no mic/camera is in use. (4) Thermal drift over 70 min: the baseline is paired per row, which mitigates it. No new dependencies (mlx is already an extra). No invariant is weakened. Invariant 6 is what is being measured. Invariants 1-5 are untouched.
<!-- SECTION:PLAN:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Merged #36: bench --foreground-check (30 fps MLX probe + Calculator relaunch proxy, paired medians, fail-closed exits) and NF-7 video-call protocol (human study NOT VERIFIABLE HERE). M3 Pro: qwen passes light/balanced, minilm fails miss-rate at light (+1.11 pp) and balanced (+1.56 pp); DVFS confounds latency ratios. Thresholds kept; follow-up TASK-24; TASK-15 and SPEC Q5 noted. CI run 39054681404: 1606 passed.
<!-- SECTION:FINAL_SUMMARY:END -->
