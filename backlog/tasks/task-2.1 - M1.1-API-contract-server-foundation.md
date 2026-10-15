---
id: TASK-2.1
title: 'M1.1: API contract + server foundation'
status: Done
assignee: []
created_date: '2026-10-15 14:26'
updated_date: '2026-10-15 15:07'
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
- [x] #1 Schemas in minion-common used by server and client; mypy strict clean
- [x] #2 Auth: valid/invalid/expired/revoked tokens and API keys covered by tests
- [x] #3 URI validation rejects other buckets, bad prefixes, .. and //
- [x] #4 SeaweedFS fixture: presigned GET/PUT round-trip test passes
- [x] #5 Migration 0002 round-trips; drift test clean
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Plan steps A1, A2, B1, B2 of TASK-2 implemented (D1-D4). Key files: packages/minion-common/src/minion_common/{schemas.py,client.py}; packages/minion-server/src/minion_server/{settings.py,db/session.py,app.py,storage.py,uris.py,auth.py,models_registry.py,admin.py,repositories/executors.py}; migrations/versions/0002_executor_token_expiry.py; root conftest.py (Postgres + SeaweedFS fixtures). Decisions: API keys from MINION_API_KEYS_FILE, sha256 + compare_digest (ADR-0007, Proposed); executor tokens pre-issued via minion-server-admin issue-executor-token (ttl 1..365 d); S3 via boto3 + to_thread, presign against public endpoint. Known gaps (by design, TASK-2.2): S3Storage not yet built in lifespan; no route consumes parse_s3_uri/load_models/current_user yet; no 403 team check yet. Docs synced in this commit: SPEC §6/§7/§8, ARCHITECTURE §2.1/§9, CLAUDE.md commands.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Server foundation and shared API contract landed: typed schemas and httpx client in minion-common, settings/DB lifespan, migration 0002 (token_expires_at), S3 storage with public-endpoint presigning, URI validation, API-key and executor-token auth, admin CLI. Endpoints and the acquisition race test follow in TASK-2.2.
<!-- SECTION:FINAL_SUMMARY:END -->
