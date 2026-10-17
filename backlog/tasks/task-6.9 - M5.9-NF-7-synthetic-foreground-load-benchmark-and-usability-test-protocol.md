---
id: TASK-6.9
title: 'M5.9 NF-7: synthetic foreground-load benchmark and usability-test protocol'
status: To Do
assignee: []
created_date: '2026-10-17 03:33'
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
- [ ] #1 nf7_proxy_verdict unit-tested; CI path runs with a fake probe and fake engine
- [ ] #2 Mac results for light and balanced on qwen2.5-0.5b-mlx and minilm-mlx committed (derived figures only)
- [ ] #3 If the proxy fails at light or balanced, a follow-up is opened and TASK-15/Q5 flagged; thresholds not loosened
- [ ] #4 Protocol doc exists; human study marked NOT VERIFIABLE HERE
<!-- AC:END -->
