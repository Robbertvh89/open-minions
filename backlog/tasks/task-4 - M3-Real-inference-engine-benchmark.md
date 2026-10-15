---
id: TASK-4
title: 'M3: Real inference + engine benchmark'
status: In Progress
assignee: []
created_date: '2026-10-15 14:02'
updated_date: '2026-10-15 20:31'
labels:
  - milestone
  - M3
dependencies: []
ordinal: 4000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
MLX completion (BatchGenerator) and embedding runners, Ollama/HF fallbacks, models.yaml admission, minion-executor bench. Confirms ADR-0005.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 bench reports tok/s, records/h, peak memory for mlx vs ollama vs hf on same model
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Carry-over from M1 (TASK-2.3 reviews): (1) Runner protocol needs per-step cancel/pause hook (duty cycling), streaming/progress, memory_in_use() before real engines; (2) worker sandboxing (sandbox-exec profile or separate user) — compromised model code can read Keychain/~/.aws as same user; (3) worker env allowlist must gain proxy/CA (HTTPS_PROXY, SSL_CERT_FILE/REQUESTS_CA_BUNDLE), HF_HUB_*/HF_ENDPOINT/HF_TOKEN, OLLAMA_HOST handling — decide on credential-bearing proxy URLs (TODO(M3) in worker/process.py); (4) default --runner mlx currently exits 2 'unknown runner' — M3 must ship it; (5) separate load/run timeouts sized for large MLX loads; (6) TASK-9 model revision pinning.
<!-- SECTION:NOTES:END -->
