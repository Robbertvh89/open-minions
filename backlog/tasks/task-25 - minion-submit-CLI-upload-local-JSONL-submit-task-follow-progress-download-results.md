---
id: TASK-25
title: >-
  minion submit CLI: upload local JSONL, submit task, follow progress, download
  results
status: To Do
assignee: []
created_date: '2026-10-17 17:54'
labels:
  - cli
dependencies: []
ordinal: 50000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Submitting a task today needs a manual S3 upload plus curl. Add a submitter CLI (in minion-common or a small new console script) that takes a local JSONL file, uploads it to S3 (credentials only on the submitter side; the server never proxies payloads, invariant 4), POSTs /tasks with an API key, polls progress, and downloads/merges outputs. Never logs payload contents.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 minion submit --type embedding|completion --model M data.jsonl -o out/ uploads, submits, waits and downloads; exit non-zero on FAILED/CANCELLED
- [ ] #2 API key from env or Keychain, never printed; insecure URLs refused like the executor CLI
- [ ] #3 Integration test end to end with the fake engine
- [ ] #4 README quick start uses it
<!-- AC:END -->
