---
id: TASK-2.1
title: 'M1.1: API contract + server foundation'
status: To Do
assignee: []
created_date: '2026-10-15 14:26'
labels:
  - M1
dependencies: []
parent_task_id: TASK-2
priority: high
ordinal: 9000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Plan steps A1, A2, B1, B2 of TASK-2: Pydantic API schemas + typed httpx client (https unless localhost) in minion-common; server settings, DB session/lifespan, migration 0002 (executor token_expires_at), shared Postgres+SeaweedFS testcontainers fixtures at repo-root conftest; models.yaml loader, URI validation (bucket + inputs/ outputs/ prefixes), storage.py (boto3 via to_thread, presign with public endpoint), auth deps (API keys file, executor tokens with compare_digest), admin CLI issue-executor-token. See TASK-2 plan + decisions D1-D4.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Schemas in minion-common used by server and client; mypy strict clean
- [ ] #2 Auth: valid/invalid/expired/revoked tokens and API keys covered by tests
- [ ] #3 URI validation rejects other buckets, bad prefixes, .. and //
- [ ] #4 SeaweedFS fixture: presigned GET/PUT round-trip test passes
- [ ] #5 Migration 0002 round-trips; drift test clean
<!-- AC:END -->
