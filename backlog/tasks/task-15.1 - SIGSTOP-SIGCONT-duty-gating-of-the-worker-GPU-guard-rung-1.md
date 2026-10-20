---
id: TASK-15.1
title: SIGSTOP/SIGCONT duty gating of the worker (GPU guard rung 1)
status: To Do
assignee: []
created_date: '2026-10-18 21:07'
updated_date: '2026-10-20 04:26'
labels:
  - executor
  - security
dependencies: []
parent_task_id: TASK-15
ordinal: 79000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Split from TASK-15 plan step 4: suspend/resume the worker under _sig_lock to throttle over-duty GPU use before the kill rung; orphan test (control SIGKILLed while worker stopped), setpgid fallback; gpu test 10 Hz throttle same checksum.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Over-duty worker throttled before kill (gpu test)
- [ ] #2 No stranded stopped worker when control dies (macos test)
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
2026-10-20 DEFERRED (planner analysis, scratchpad task15-1-plan.md): no real case today (honest peaks 22.9 % light / 45.6 % balanced vs limits 40/60; HF 10-32 %); gating conflicts with the GPU-second kill bucket (a gated pipelined rogue never fills it -> never killed/quarantined) and needs an ADR-0006 D17 decision first; ~2 s gating is coarse vs the 50 ms pacer; orphaned stopped worker needs its own process group. Revisit if an honest bench/gpu-test window comes within 5 pp of the limit, honest engines get GPU_DUTY_VIOLATION kills in the field, or NF-7 evidence (TASK-24) shows long prefill bursts hurt. Depends on TASK-12.
<!-- SECTION:NOTES:END -->
