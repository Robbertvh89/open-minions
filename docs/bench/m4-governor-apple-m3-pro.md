# M4 governor measurements — Apple M3 Pro (36 GB, macOS 27.0.1)

TASK-5.6. Raw data: [`m4-governor-apple-m3-pro.json`](m4-governor-apple-m3-pro.json) (NF-8) and
[`m4-governor-cpu-apple-m3-pro.json`](m4-governor-cpu-apple-m3-pro.json) (NF-4). Procedure:
[`governor-measurements.md`](governor-measurements.md).

## Setup

- Mac on AC, 100 % battery, no heavy apps; mlx 0.32.4, mlx-lm 0.32.0, Python 3.12.7.
- `minion-executor bench --budget-check --models deploy/bench-models.yaml --model qwen2.5-0.5b-mlx --model minilm-mlx --profiles light,balanced,full --duration 60 --baseline-s 20 --repeats 3`
  at commit 615685a (quantum pacing). 1fc03d5 only changes the pacer's quantum latch for edge cases
  (one step per occurrence); it can only lower the measured share (fewer undrained sleeps) and was not re-measured.
- **Measured share** = the worker process's own GPU time (summed `accumulatedGPUTime` of its
  `AGXDeviceUserClient`s, ns) / wall time. Device utilization is shown for information only: it is
  device-wide and was 31–39 % on an idle desktop (WindowServer and other apps), so it cannot judge a
  per-process budget.
- **expected** = duty × unpaced share (the unpaced share is the same model at duty 1.0).

## NF-8 results (3 repeats each, all rows pass)

| model | profile | rep | status | duty | unpaced | expected | measured | pacer-reported | device idle/unpaced/paced (info) | footprint GB | hard (<= duty+10pp) | accuracy (+-10pp) | memory (<= 1.1x) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| qwen2.5-0.5b-mlx | light | 1 | ok | 30% | 75% | 22% | 22% | 30% | 33/95/71% | 1.66 | pass | pass | pass |
| qwen2.5-0.5b-mlx | light | 2 | ok | 30% | 76% | 23% | 21% | 30% | 34/96/71% | 1.66 | pass | pass | pass |
| qwen2.5-0.5b-mlx | light | 3 | ok | 30% | 76% | 23% | 21% | 30% | 39/96/70% | 1.66 | pass | pass | pass |
| qwen2.5-0.5b-mlx | balanced | 1 | ok | 50% | 79% | 39% | 31% | 50% | 32/99/84% | 1.91 | pass | pass | pass |
| qwen2.5-0.5b-mlx | balanced | 2 | ok | 50% | 81% | 41% | 35% | 50% | 35/99/84% | 1.91 | pass | pass | pass |
| qwen2.5-0.5b-mlx | balanced | 3 | ok | 50% | 80% | 40% | 35% | 50% | 34/99/84% | 1.91 | pass | pass | pass |
| qwen2.5-0.5b-mlx | full | 1 | ok | 100% | - | - | 77% | 100% | 34/-/98% | 2.09 | pass | - | pass |
| qwen2.5-0.5b-mlx | full | 2 | ok | 100% | - | - | 77% | 100% | 34/-/98% | 2.09 | pass | - | pass |
| qwen2.5-0.5b-mlx | full | 3 | ok | 100% | - | - | 78% | 100% | 34/-/98% | 2.05 | pass | - | pass |
| minilm-mlx | light | 1 | ok | 30% | 33% | 10% | 13% | 29% | 33/74/64% | 0.45 | pass | pass | pass |
| minilm-mlx | light | 2 | ok | 30% | 33% | 10% | 12% | 29% | 38/75/58% | 0.45 | pass | pass | pass |
| minilm-mlx | light | 3 | ok | 30% | 32% | 10% | 12% | 29% | 31/74/57% | 0.45 | pass | pass | pass |
| minilm-mlx | balanced | 1 | ok | 50% | 57% | 28% | 20% | 49% | 37/71/74% | 0.46 | pass | pass | pass |
| minilm-mlx | balanced | 2 | ok | 50% | 56% | 28% | 22% | 48% | 32/73/65% | 0.46 | pass | pass | pass |
| minilm-mlx | balanced | 3 | ok | 50% | 57% | 29% | 21% | 48% | 37/72/68% | 0.46 | pass | pass | pass |
| minilm-mlx | full | 1 | ok | 100% | - | - | 54% | 100% | 34/-/75% | 0.61 | pass | - | pass |
| minilm-mlx | full | 2 | ok | 100% | - | - | 55% | 100% | 35/-/76% | 0.61 | pass | - | pass |
| minilm-mlx | full | 3 | ok | 100% | - | - | 54% | 100% | 33/-/76% | 0.61 | pass | - | pass |

- **Hard bound** (share ≤ duty + 10 pp): max measured 22 % at light (30 %), 35 % at balanced (50 %).
- **Accuracy** (±10 pp of duty × unpaced): qwen light 21–22 % vs 22–23 %; qwen balanced 31–35 % vs
  39–41 %; minilm light 12–13 % vs 10 %; minilm balanced 20–22 % vs 28–29 %. Pacing errs low (the GPU
  sync at each quantum end and the final partial-quantum sleep are counted as busy).
- **Memory**: peak worker footprint (from before model load) 0.45–2.09 GB against 9.0 / 14.4 / 25.2 GB.

## How we got here (pacing bug found by this measurement)

1. First run with per-process GPU time, sync-free pacer: qwen balanced measured **74–76 %** at duty
   50 % (hard bound FAIL) while the pacer reported exactly 50 %. Cause: mlx-lm's `BatchGenerator`
   pipelines with `mx.async_eval`, so queued GPU work ran during the pacer's sleep (invariant 6).
2. Sync before every checkpoint (a8cbd76): bound met, but qwen light 14–15 % / balanced 27–31 %,
   because syncing every step serialized CPU graph build and GPU compute.
3. Quantum pacing (615685a): accumulate ~50 ms of busy time, then sync and sleep once. Numbers above.

## NF-9 reaction (CI test, fake engine, FileSignals, poll 0.25 s, 100 ms steps, 4 trials per event)

| event | p50 | max |
|---|---|---|
| thermal serious → pause ack | 0.101 s | 0.143 s |
| memory warning → pause ack | 0.152 s | 0.249 s |
| memory critical → worker killed (release) | 0.188 s | 0.250 s |
| on battery, 5 % → pause ack | 0.168 s | 0.433 s |
| user return → duty drop ack | 0.246 s | 0.289 s |

Max over 20 trials 0.433 s (< 5 s). At the production poll of 2 s the worst case is about poll + one
engine step + ack, well under 5 s (`MINION_EXECUTOR_SIGNAL_POLL_S` is capped at 4 s for this reason).
The Mac-side measurements on real signals (induced `memory_pressure`, user return at the keyboard)
were **not run here**: they need a person at the keyboard and deliberately push this Mac into
swap. The opt-in procedure is in `governor-measurements.md`.

## NF-4 control-process CPU (60 s, production poll 2 s, real MacSignals)

| state | cpu s | cpu % |
|---|---|---|
| idle | 0.080 | 0.134 % |
| paused | 0.097 | 0.162 % |

Both < 1 %. This covers the control process only; the whole `run` process tree (control + worker, `ps` procedure in governor-measurements.md) was not recorded here and stays open.
