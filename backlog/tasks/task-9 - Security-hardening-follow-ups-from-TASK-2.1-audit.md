---
id: TASK-9
title: Security hardening follow-ups from TASK-2.1 audit
status: Done
assignee: []
created_date: '2026-10-15 14:41'
updated_date: '2026-10-18 19:47'
labels:
  - security
dependencies: []
ordinal: 12000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Deferred MEDIUM/LOW findings from TASK-2.1 security audit: (1) pin HF model revision (commit SHA) in ModelEntry + models.yaml, pass to loaders (SPEC S-7); (2) presigned GET scoped to subtask byte range (sign Range header) and PUT size cap; (3) allow-list task params keys per task type + size cap (block trust_remote_code etc.).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 ModelEntry requires revision SHA and runners load by revision
- [x] #2 Presigned GET signs Range; oversize PUT rejected
- [x] #3 Unknown task params rejected with 422
<!-- AC:END -->



## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Items #1 (revision pin) and #3 (params allow-list) delivered by TASK-4.1; #2 (Range-signed GET, PUT size cap) remains.

Follow-ups from TASK-4.1 review (not yet implemented): (a) absolute per-batch wall-clock cap, and count a step tick toward liveness only when records_done increases, so a stuck batch cannot be kept alive by ticks; (b) server-side cap on records_per_subtask x max_tokens for completions (reject or split before acquire).

TASK-4.4 (M3 bench): `minion-executor bench` ignores the `run --engines ollama` opt-in. An ollama entry under `models:` in a bench file makes the bench pull into ~/.ollama, outside the model cache cap (ADR-0009). Consider requiring an explicit flag for ollama entries in bench once TASK-13 pins digests.

From TASK-5.1 audit: no per-executor rate limit on any executor endpoint (presence, heartbeat, acquire, register); each is a single-row write behind a valid token, 1 MiB body cap. Add a per-executor minimum interval or proxy-level limit.

macOS ACLs are not checked on the state dir, control.json or executor.toml (mode bits only, TASK-5.4 review): reject extended ACLs (acl_get_file) or document the limitation.

TASK-5.4 review: O_NOFOLLOW covers only the last path component; symlinks in state dir parents are followed (same-user/admin only). Consider an openat component walk.

TASK-5.5 review follow-up: POST /executor/presence has no server-side minimum interval / 429 like heartbeat. A misbehaving or hostile executor can spam it (DB write per call). Add a per-executor min interval and a 429 with Retry-After, and honor it in the executor's presence sender.

Follow-up (TASK-6.2): document an example reverse-proxy per-IP/per-token rate-limit config (nginx limit_req / Caddy) for executor and read endpoints; the in-app limiter is per replica, per executor only.
<!-- SECTION:NOTES:END -->
