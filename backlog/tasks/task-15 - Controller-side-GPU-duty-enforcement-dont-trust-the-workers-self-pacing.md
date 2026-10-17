---
id: TASK-15
title: Controller-side GPU duty enforcement (don't trust the worker's self-pacing)
status: To Do
assignee: []
created_date: '2026-10-17 01:56'
updated_date: '2026-10-17 17:38'
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

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
2026-10-17 TASK-6.9 NF-7 proxy data (docs/bench/m5-foreground-apple-m3-pro.md): minilm-mlx fails the probe miss-rate criterion at light (+1.11 pp) and balanced (+1.56 pp) while within its NF-8 duty; qwen passes. Relevant to enforcement design: duty share alone does not bound foreground tail latency for short frequent steps. Follow-up TASK-24.
<!-- SECTION:NOTES:END -->
