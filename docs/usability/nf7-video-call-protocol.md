# NF-7 usability protocol: video calls with the executor running

> **NOT VERIFIABLE HERE.** This needs real participants on their own Macs. CI and the synthetic
> proxy (`bench --foreground-check`, ADR-0006 Gap 2) cannot show that a call is not visibly or
> audibly degraded. Until this study is run, the human half of SPEC NF-7 stays open.

## Question

Under the `light` and `balanced` profiles, can a user tell that the executor is working during a
Teams/Zoom video call with screen share? Pass rule (SPEC NF-7): no noticeable degradation, and frame
drops and app-launch time within 10 % of the executor-off baseline.

## Participants and design

- N >= 8 participants, each on their own Apple Silicon Mac, on AC power, doing their normal call.
- Within-subject, four arms per participant in randomized order, **executor off / light / balanced /
  off** (the second "off" checks drift and order effects). Each arm is a 15 min call with camera on
  and screen share for at least 5 min.
- Blinded: a launcher script starts the arm (`minion-executor profile ...` or the executor stopped)
  without telling the participant which one. Keep the executor busy (a queue of completion subtasks
  or `bench` load) so the arm actually exercises the GPU. Check `minion-executor status` shows a
  current subtask.
- The call-aware governor (TASK-6.10, `on_call`) would hide the effect by dropping to `light` or
  pausing. Disable it for the study arms (`on_call = "ignore"` in `executor.toml`) or record its
  state per arm; report both if both were used.

## Measures

| Measure | How |
|---|---|
| Call quality | Zoom/Teams statistics panel: sent/received fps, jitter, packet loss, sampled every minute |
| Screen frame rate | WindowServer frame-rate trace (Quartz Debug frame meter); `sudo powermetrics --samplers gpu_power` for GPU residency noted alongside |
| App-launch time | 3 apps (for example Calculator, Notes, Safari), 5 cold launches each, wall time to first window |
| Perceived quality | 5-point MOS per arm, plus "did you notice any degradation?" yes/no |
| Executor state | `minion-executor status` per arm (profile, budget, `paused_reason`) |

## Pass rule

1. Call fps/jitter, screen frame rate and launch time in `light` and `balanced` are within 10 % of the
   executor-off arms (median over participants).
2. The "noticed degradation" rate in `light` and `balanced` is not above the off arms (paired
   comparison, report the counts; with N=8 no significance claim, only absence of a clear effect).
3. Mean MOS is within 0.3 of the off arms.

A failure at `light` or `balanced` opens a backlog item and feeds SPEC Q5 (whether duty cycling is
enough or total GPU utilization must be throttled) and the default profile choice (TASK-15).

## Consent and data

- Written consent before the study; participants may stop at any time.
- Do not record call content, audio, video or screen contents. Only the numbers above are collected.
- Participant ids are pseudonymous (P1..PN). Store no hostnames, account names or timestamps beyond
  the arm order.

## Results template

| Participant | Chip / RAM | Arm | Order | fps | jitter ms | screen fps | launch s (3 apps) | MOS | noticed |
|---|---|---|---|---|---|---|---|---|---|
| P1 | | off | 1 | | | | | | |

Summary: medians per arm, the three pass-rule checks, the `on_call` setting used, deviations.
