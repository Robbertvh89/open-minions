---
id: TASK-25
title: >-
  minion submit CLI: upload local JSONL, submit task, follow progress, download
  results
status: Done
assignee: []
created_date: '2026-10-17 17:54'
updated_date: '2026-10-17 19:44'
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
- [x] #1 minion submit --type embedding|completion --model M data.jsonl -o out/ uploads, submits, waits and downloads; exit non-zero on FAILED/CANCELLED
- [x] #2 API key from env or Keychain, never printed; insecure URLs refused like the executor CLI
- [x] #3 Integration test end to end with the fake engine
- [x] #4 README quick start uses it
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
TASK-25 plan (planner, based on main):
- New package packages/minion-cli (module minion_cli, script `minion`); not minion-common (keeps boto3/S3 creds out of executor closure, S-2). Reuses UserClient, require_secure_url, ApiError, TaskCreate/TaskOut/SubtaskPage.
- Step 1 scaffold/settings/secrets/store: CliSettings env_prefix MINION_CLI_ (no env_file): server, team, project="default", s3_endpoint_url, s3_bucket="minion", s3_region, s3_access_key/secret (SecretStr|None, else boto default chain), input_prefix="inputs/", output_prefix="outputs/"; require_secure_url on server and s3 endpoint. API key: $MINION_API_KEY else Keychain item open-minions-api-key via /usr/bin/security (fixed argv, PATH env, timeout 10, darwin only); no --api-key flag. ObjectStore protocol + S3Store (boto3 path-style s3v4, asyncio.to_thread). Unit tests.
- Step 2 flows.py (async, no typer): upload_inputs to {input_prefix}{team}/{project}/{name}/{i:04d}-{basename} (argv order; resume if exactly our keys/sizes; foreign objects -> exit 2); submit (get_task exists -> exit 2 hint fetch; upload; create_task output_uri s3://{bucket}/{output_prefix}{team}/{project}/{name}/; default name slug(stem)-UTC%Y%m%d%H%M%S); wait (2 s x1.5 to 30 s, jitter, 429 Retry-After, 5xx/transport retry up to 10, 401/403/404 fatal; counts only to stderr on change); fetch (page list_subtasks SUCCEEDED; download {out_key}{id:06d}.jsonl -> out/parts via temp+replace; skip same size; results.jsonl ascending id; manifest.json; never print line content; never list _attempts/). Unit tests FakeStore + httpx.MockTransport, payload-sentinel never in output.
- Step 3 typer main.py: submit/status/fetch/cancel. Exit 0 ok, 1 FAILED/CANCELLED, 2 usage/config/auth/4xx, 3 PARTIAL, 4 timeout/unreachable, 130 Ctrl-C (never auto-cancel). CliRunner tests.
- Step 4 integration: move Live/_serve/eventually from minion-executor/tests/test_e2e.py into root conftest.py (pure move); packages/minion-cli/tests/test_e2e_cli.py: 2 files 130+107 records rps 25 -> exit 0, order a0..a129,b0..b106 no dups; refetch idempotent; --no-wait + cancel + fetch --wait -> exit 1; BAD_INPUT -> exit 1.
- Step 5 docs: README quick start, CLAUDE.md, SPEC FR-1 + new S-14 (submitter S3 creds scoped to inputs/{team}/ outputs/{team}/, never server's), ARCHITECTURE §2.5/§3.1, .env.example.
- Risks: same-name concurrent submit (server 409 decides); boto3 checksum vs SeaweedFS (request_checksum_calculation="when_required"); remote DOCKER_HOST -> skip; uv.lock conflict with TASK-6.11 -> branch after 6.11 merges.
<!-- SECTION:PLAN:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Merged #38: packages/minion-cli with minion submit/status/fetch/cancel (exit 0/1/2/3/4/130), S3 creds submitter-side only (S-14), content-verified resume and fetch, exclusive locked output dir, results in record order. CI run 39062777152: 1722 passed; live run against compose stack passed.
<!-- SECTION:FINAL_SUMMARY:END -->
