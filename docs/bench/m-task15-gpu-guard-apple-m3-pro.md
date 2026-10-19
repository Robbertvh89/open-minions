# TASK-15 GPU duty guard defaults — Apple M3 Pro (36 GB, macOS 27.0.1)

TASK-15 step 5. Raw data: [`m-task15-gpu-guard-apple-m3-pro.json`](m-task15-gpu-guard-apple-m3-pro.json)
(`bench --budget-check` output, schema_version 1, with the new per-row fields `max_window_share` and
`gpu_clients`). Purpose: justify the guard defaults in `capacity/gpu_guard.py`: window 10 s, margin
+10 pp (NF-8), kill after 30 s over the limit, `max_clients` 4.

## Setup

- mlx 0.32.4, mlx-lm 0.32.0, Python 3.12.7, Mac on AC, normal desktop activity, nothing else on the GPU.
- `caffeinate -i minion-executor bench --budget-check --models deploy/bench-models.yaml --model qwen2.5-0.5b-mlx --model minilm-mlx --profiles light,balanced,full --duration 40 --baseline-s 5 --repeats 3`
  at commit f1a07d0 plus the `bench_budget` change that adds the two columns. MLX models only (Ollama is not installed).
- `max_window_share`: highest per-process GPU busy share over any sliding 10 s window inside the settled
  paced phase (samples about every 0.5 s, finer than the guard's 2 s, so this is the stricter reading).
  `gpu_clients`: distinct `AGXDeviceUserClient` ids of the worker pid over the whole run (load included).

## Rows

| model | profile | rep | duty | measured (whole phase) | max 10 s window | guard limit (duty + 10 pp) | headroom to limit | GPU clients |
|---|---|---|---|---|---|---|---|---|
| qwen2.5-0.5b-mlx | light | 1 | 30% | 21.4% | 22.1% | 40% | 17.9 pp | 1 |
| qwen2.5-0.5b-mlx | light | 2 | 30% | 21.8% | 22.2% | 40% | 17.8 pp | 1 |
| qwen2.5-0.5b-mlx | light | 3 | 30% | 22.1% | 22.9% | 40% | 17.1 pp | 1 |
| qwen2.5-0.5b-mlx | balanced | 1 | 50% | 38.1% | 40.2% | 60% | 19.8 pp | 1 |
| qwen2.5-0.5b-mlx | balanced | 2 | 50% | 42.8% | 44.9% | 60% | 15.1 pp | 1 |
| qwen2.5-0.5b-mlx | balanced | 3 | 50% | 44.3% | 45.6% | 60% | 14.4 pp | 1 |
| qwen2.5-0.5b-mlx | full | 1 | 100% | 88.2% | 92.2% | none (duty 1.0: guard never acts) | - | 1 |
| qwen2.5-0.5b-mlx | full | 2 | 100% | 97.5% | 98.5% | none (duty 1.0: guard never acts) | - | 1 |
| qwen2.5-0.5b-mlx | full | 3 | 100% | 95.7% | 97.8% | none (duty 1.0: guard never acts) | - | 1 |
| minilm-mlx | light | 1 | 30% | 16.6% | 17.5% | 40% | 22.5 pp | 1 |
| minilm-mlx | light | 2 | 30% | 17.1% | 17.5% | 40% | 22.5 pp | 1 |
| minilm-mlx | light | 3 | 30% | 16.0% | 17.1% | 40% | 22.9 pp | 1 |
| minilm-mlx | balanced | 1 | 50% | 31.0% | 32.7% | 60% | 27.3 pp | 1 |
| minilm-mlx | balanced | 2 | 50% | 30.5% | 31.8% | 60% | 28.2 pp | 1 |
| minilm-mlx | balanced | 3 | 50% | 31.1% | 32.1% | 60% | 27.9 pp | 1 |
| minilm-mlx | full | 1 | 100% | 63.6% | 64.6% | none (duty 1.0: guard never acts) | - | 1 |
| minilm-mlx | full | 2 | 100% | 62.3% | 64.2% | none (duty 1.0: guard never acts) | - | 1 |
| minilm-mlx | full | 3 | 100% | 63.1% | 65.0% | none (duty 1.0: guard never acts) | - | 1 |

All 18 rows pass the NF-8 verdicts (hard, accuracy, memory).

## Summary per profile (worst of the 6 rows)

| profile | duty | worst max 10 s window | limit | headroom | clients |
|---|---|---|---|---|---|
| light | 30% | 22.9% (qwen) | 40% | 17.1 pp | 1 |
| balanced | 50% | 45.6% (qwen) | 60% | 14.4 pp | 1 |
| full | 100% | 98.5% (qwen) | never acts | - | 1 |

## Verdict on the defaults

- **Margin +10 pp, window 10 s: safe for these workloads.** The honest worker stays 14 pp or more below the
  limit at light and balanced; the pacer undershoots its duty (qwen balanced 38-44 % whole-phase against
  50 %), so the nearest honest window is far from the 3 pp danger zone. The closest case is qwen balanced
  (max window 45.6 % against 60 %).
- **`max_clients` 4: safe.** An honest MLX worker holds exactly 1 GPU client (every row, every profile).
- **Not covered by this data:** steady batches only. Short prefill bursts or very short subtasks are not
  shown here; see the next section for a related blind spot. HF/MPS and Ollama workers were not measured.
- Full profile: duty 1.0 gives a limit above 1, so the guard never acts there (by design).

## Real-worker `gpu` tests (`packages/minion-executor/tests/test_gpu_guard_gpu.py`)

- Rogue hook (`gpu_burn_s`, ignores pacing) at light: killed and released `GPU_DUTY_VIOLATION`; first
  over-limit verdict about 10 s after the burn starts, kill 30 s later, total about 49 s including the worker
  spawn. Burn share measured by the guard 87 % at first, decaying to 62 % at the kill (windows still hold the
  early part; no gating exists yet: TASK-15.1).
- Honest real Qwen MLX inference at light, balanced and full for 40 s with subtasks longer than the window:
  never gated, killed or released; every judged share was at most duty + 10 pp.
- **Finding (xfail test `test_rogue_in_short_subtasks_is_killed`):** the guard window is per subtask
  (`_watch` starts with `gpu_state[0] = None`, so `GpuDutyGuard.start` resets the window) and a share is
  judged only after 0.8 x 10 s = 8 s. A worker that burns the GPU in back-to-back subtasks shorter than 8 s is
  never judged (8 subtasks of 3 s each ran at about 70 % busy without a verdict). The same blind spot hides the
  between-subtask gaps unless the idle guard fires. Honest workers are unaffected.
