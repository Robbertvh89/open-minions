---
id: TASK-13
title: Bench Ollama on a Mac that already has it (pin digests)
status: To Do
assignee: []
created_date: '2026-10-16 13:02'
labels: []
dependencies: []
ordinal: 23000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
TASK-4.4 left Ollama unmeasured: it is an opt-in extra outside the sandbox (ADR-0009), not installed on the bench Mac. Pin manifest digests for qwen2.5:0.5b-instruct-fp16 and all-minilm:l6-v2 (ollama show), move the pending: entries in deploy/bench-models.yaml to models:, run bench on a Mac with Ollama, add rows to docs/bench/m3-apple-m3-pro.md.
<!-- SECTION:DESCRIPTION:END -->
