# M5 NF-7 foreground-load proxy — Apple M3 Pro (36 GB, macOS 27.0.1)

TASK-6.9. Raw data: [`m5-foreground-apple-m3-pro.json`](m5-foreground-apple-m3-pro.json). Procedure:
[`governor-measurements.md`](governor-measurements.md). Definition: ADR-0006 Gap 2. This is a
**synthetic proxy** (GPU scheduling latency of a small 30 fps probe), not compositor frame drops and
not a video call; the human study ([protocol](../usability/nf7-video-call-protocol.md)) is not run.

## Setup

- Mac on AC, 100 % battery, Calculator closed, no call; normal desktop activity (load average about 3
  at start). mlx 0.32.4, mlx-lm 0.32.0, Python 3.12.7. Probe size 2286 (fp16 matmul, calibrated to
  about 5 ms).
- `caffeinate -i minion-executor bench --foreground-check --models deploy/bench-models.yaml --model qwen2.5-0.5b-mlx --model minilm-mlx --profiles light,balanced,full --duration 60 --launches 20 --repeats 3`
  at commit dad697c. Exit 1 (judged rows fail). About 60 minutes.

## Per-repeat rows (all `ok`)

| model | profile | rep | worker GPU | p95 ms base | p95 ms loaded | miss % base | miss % loaded | launch s base | launch s loaded |
|---|---|---|---|---|---|---|---|---|---|
| qwen2.5-0.5b-mlx | light | 1 | 11% | 28.73 | 30.33 | 0.1 | 1.4 | 0.24 | 0.24 |
| qwen2.5-0.5b-mlx | light | 2 | 12% | 29.65 | 29.72 | 0.1 | 0.9 | 0.24 | 0.23 |
| qwen2.5-0.5b-mlx | light | 3 | 11% | 30.27 | 30.52 | 0.4 | 1.2 | 0.24 | 0.23 |
| qwen2.5-0.5b-mlx | balanced | 1 | 21% | 29.68 | 27.36 | 0.1 | 0.6 | 0.24 | 0.23 |
| qwen2.5-0.5b-mlx | balanced | 2 | 22% | 30.32 | 26.84 | 0.4 | 0.2 | 0.23 | 0.23 |
| qwen2.5-0.5b-mlx | balanced | 3 | 22% | 29.97 | 25.19 | 0.6 | 0.2 | 0.23 | 0.23 |
| qwen2.5-0.5b-mlx | full | 1 | 68% | 30.36 | 11.40 | 0.3 | 0.0 | 0.24 | 0.21 |
| qwen2.5-0.5b-mlx | full | 2 | 68% | 29.81 | 11.60 | 0.5 | 0.0 | 0.24 | 0.21 |
| qwen2.5-0.5b-mlx | full | 3 | 68% | 29.79 | 11.37 | 0.2 | 0.0 | 0.23 | 0.21 |
| minilm-mlx | light | 1 | 6% | 28.87 | 30.36 | 0.2 | 0.9 | 0.23 | 0.24 |
| minilm-mlx | light | 2 | 6% | 30.09 | 31.25 | 0.0 | 2.3 | 0.24 | 0.24 |
| minilm-mlx | light | 3 | 6% | 29.59 | 30.74 | 0.2 | 1.3 | 0.24 | 0.24 |
| minilm-mlx | balanced | 1 | 12% | 28.77 | 29.85 | 0.3 | 1.1 | 0.24 | 0.24 |
| minilm-mlx | balanced | 2 | 12% | 28.77 | 30.54 | 0.1 | 2.4 | 0.24 | 0.24 |
| minilm-mlx | balanced | 3 | 11% | 29.44 | 30.18 | 0.2 | 1.7 | 0.24 | 0.25 |
| minilm-mlx | full | 1 | 48% | 29.30 | 10.77 | 0.2 | 0.0 | 0.24 | 0.20 |
| minilm-mlx | full | 2 | 48% | 29.42 | 10.46 | 0.2 | 0.0 | 0.24 | 0.20 |
| minilm-mlx | full | 3 | 48% | 29.36 | 10.48 | 0.1 | 0.0 | 0.24 | 0.20 |

## Verdicts (median of paired per-repeat figures)

| model | profile | judged | p95 ratio (<= 1.10) | miss delta pp (<= 1.0) | launch ratio (<= 1.10) | verdict |
|---|---|---|---|---|---|---|
| qwen2.5-0.5b-mlx | light | yes | 1.01 | 0.78 | 0.98 | pass |
| qwen2.5-0.5b-mlx | balanced | yes | 0.89 | -0.28 | 0.98 | pass |
| qwen2.5-0.5b-mlx | full | no | 0.38 | -0.28 | 0.91 | info |
| minilm-mlx | light | yes | 1.04 | **1.11** | 1.00 | **FAIL** |
| minilm-mlx | balanced | yes | 1.04 | **1.56** | 1.01 | **FAIL** |
| minilm-mlx | full | no | 0.36 | -0.17 | 0.83 | info |

## Reading the result

1. **The proxy fails at light and balanced for minilm-mlx** on the miss-rate criterion (+1.1 and
   +1.6 pp of 30 fps probe frames over 33.3 ms). qwen2.5-0.5b-mlx passes light (+0.8 pp, close to
   the limit) and balanced. Thresholds are kept as specified (TASK-6.9 AC3); follow-up TASK-24.
2. **GPU frequency scaling (DVFS) confounds the latency ratios.** The probe, calibrated to about
   5 ms, took a median of about 22 ms per frame on the otherwise idle GPU (baseline) and 9.5 ms when
   the worker ran at `full`. The busier the GPU, the higher its clocks, so the probe gets *faster*
   under load: `full` "passes" with a p95 ratio of 0.36–0.38, and the light/balanced p95 and p50
   ratios mix this speed-up with real contention. The p95 and launch ratios therefore cannot show
   harm here and should not be read as evidence of no impact. The miss rate (a frame over 33.3 ms) is
   the least confounded figure, and it is the one that fails.
3. **App launch** (warm relaunch of Calculator via osascript, 20 per window) is unchanged at every
   profile (0.20–0.25 s); it is CPU-bound and the worker runs at background priority.
4. **What this means for NF-7.** The data do not show that light/balanced keep a call smooth, and do
   not show that they don't: the probe is a crude stand-in and the DVFS effect works in the worker's
   favour on latency but not on tail misses. SPEC Q5 (throttle on measured total GPU) stays open;
   this run adds that short, frequent embedding steps (minilm, 6–12 % worker GPU share) hurt the
   probe's tail more than longer completion steps at a higher share (qwen, 11–22 %).
