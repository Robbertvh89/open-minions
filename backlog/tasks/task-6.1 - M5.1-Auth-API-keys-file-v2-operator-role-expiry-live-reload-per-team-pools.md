---
id: TASK-6.1
title: >-
  M5.1 Auth: API keys file v2 (operator role, expiry, live reload, per-team
  pools)
status: In Progress
assignee: []
created_date: '2026-10-17 03:32'
updated_date: '2026-10-17 05:23'
labels:
  - M5
dependencies: []
parent_task_id: TASK-6
ordinal: 32000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Keys file v2 (auth.py:48-102): per-key operator flag + expires_at; teams: {slug: {default_pool, allowed_pools}} (absorbs TASK-10, S-1). KeyStore reloads on mtime/size change checked every MINION_API_KEYS_RELOAD_S (5); invalid file after change fails closed (401 all, error logged). current_user rejects expired keys; new require_operator dependency. create_task (api/tasks.py:119-165) applies default_pool, rejects pools outside allowed_pools with 403 POOL_NOT_ALLOWED, 422 POOL_REQUIRED when team has allowed_pools but no default. Admin CLI: generate-api-key --operator --expires-days; list-executors (never the hash); rotate-executor-token --executor-id (same id, new hash, printed once). Docs: ADR-0007 amendment (created_by FK won't-do), SPEC S-1/S-8, .env.example.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A key removed from the file is rejected within reload_s + 1 s without restart
- [ ] #2 A malformed rewrite gives 401 for all keys plus an error log; fixing the file restores access
- [ ] #3 An expired key gives 401
- [ ] #4 Task without allowed_pool gets the team default; pool outside the allowed set gives 403; teams without a policy behave as before
- [ ] #5 require_operator gives 403 to non-operator keys
- [ ] #6 rotate-executor-token: old token 401, new works with same executor_id; list-executors never prints token_hash
<!-- AC:END -->
