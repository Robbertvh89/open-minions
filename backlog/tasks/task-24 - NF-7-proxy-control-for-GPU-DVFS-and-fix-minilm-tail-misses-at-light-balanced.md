---
id: TASK-24
title: 'NF-7 proxy: control for GPU DVFS and fix minilm tail misses at light/balanced'
status: To Do
assignee: []
created_date: '2026-10-17 17:38'
labels:
  - governor
dependencies: []
ordinal: 49000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
TASK-6.9 measurement (docs/bench/m5-foreground-apple-m3-pro.md) fails NF-7 proxy for minilm-mlx at light (+1.11 pp misses) and balanced (+1.56 pp); qwen passes. GPU DVFS confounds latency ratios: probe is ~22 ms/frame on idle GPU vs 9.5 ms under full load, so p95/launch ratios cannot show harm. (1) Make the proxy DVFS-robust (e.g. keep GPU clocks comparable in both windows with a constant non-competing warm load, or report probe kernel time separately from queueing delay; re-calibrate per window). (2) Investigate why short frequent embedding steps hurt probe tails more than longer completion steps (quantum pacing granularity, step size, batch size at light/balanced); fix in the governor/throttle if needed. (3) Re-run the measurement; feed SPEC Q5 / TASK-15. Thresholds stay.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Proxy is DVFS-robust: full profile no longer shows a latency ratio well below 1 purely from clock scaling (documented check)
- [ ] #2 Root cause of minilm tail misses at light/balanced identified with data
- [ ] #3 Re-measured on the M3 Pro; light and balanced pass the unchanged thresholds, or ADR-0006 records why not and what owners should do
<!-- AC:END -->
