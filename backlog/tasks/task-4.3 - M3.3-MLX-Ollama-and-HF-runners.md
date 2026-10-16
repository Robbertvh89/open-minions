---
id: TASK-4.3
title: 'M3.3: MLX, Ollama and HF runners'
status: In Progress
assignee: []
created_date: '2026-10-15 20:42'
updated_date: '2026-10-16 02:20'
labels:
  - M3
dependencies:
  - TASK-4.2
parent_task_id: TASK-4
priority: high
ordinal: 20000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Plan steps S6-S8 of TASK-4 (D10, D11, D12, D14).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 MLX completion (BatchGenerator) and embedding runners pass gpu+model tests with tiny models; wired limit within budget
- [x] #2 Ollama runner (loopback only, digest pinned) passes MockTransport unit tests in CI
- [x] #3 HF mps runner: bge-small embeddings match MLX (cosine >= 0.99)
- [x] #4 CI stays green without engine extras (markers auto-skip; mypy strict clean)
<!-- AC:END -->
