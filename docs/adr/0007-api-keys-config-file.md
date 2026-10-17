# ADR-0007: User API keys from a config file, not a database table

- Status: **Accepted** (implemented in TASK-2.1; amended by TASK-6.1, see the Amendment below)
- Date: 2026-10-15
- Deciders: project owner
- Deviates from paper: **no** — internal design choice; the paper does not specify user authentication.

## Context

Users create tasks through the REST API and authenticate with API keys (SPEC §6). SPEC §7 and the
paper fix the data model at 7 tables, so a user/key table would be an 8th table. Q4 (SPEC §11) is
still open: SSO/OIDC versus API keys only for the web UI. Designing a key table now risks throwing it
away in M5 auth hardening. TASK-2 needs working user auth in M1 so that task creation can be
authorized by team.

## Decision

- User API keys are read from a YAML file named by `MINION_API_KEYS_FILE`:
  `keys: [{name, sha256, teams: [...]}]`. The file is validated strictly (unknown fields, duplicate
  names or hashes are rejected) and loaded **once at server startup**.
- Only the SHA-256 hash of a key is stored in the file. `minion-server-admin generate-api-key` prints
  the key once and the YAML entry to paste.
- Lookup hashes the presented key with SHA-256 and compares against every stored hash with
  `hmac.compare_digest`, with no early exit on match. An absent or empty keys file fails closed (401).
- `minion_task.created_by` holds the key's `name` as a string. There is no foreign key to a users table.
- The FK part of TASK-2 security carry-forward (1) is consciously deferred to M5 auth hardening.

## Consequences

- No schema change and no 8th table; the SPEC §7 count holds.
- ~~Revoking a user key requires a restart~~ (superseded by the Amendment: keys are re-read live). Executor tokens are different: they live in the database and are revoked
  immediately (`minion-server-admin revoke-executor-token`).
- Keys can carry `expires_at` (Amendment); rotation means adding a new entry and deleting the old one.
- `created_by` is not referentially checked and can outlive a removed key; audit by name only.
- Migrating to a table (or OIDC) in M5 is a migration plus a change to `current_user` only; the
  route-level contract (`Authorization: Bearer`, team scoping) stays the same.

## Amendment (TASK-6.1, M5): keys file v2

- Entries gain `operator: bool` (default false) and `expires_at` (timezone-aware ISO timestamp;
  an expired key gets `401`). A top-level `teams: {slug: {default_pool, allowed_pools}}` map holds
  per-team pool policy (SPEC S-1). Parsing stays strict (`extra=forbid`); a v1 file is valid unchanged.
- **Live reload replaces "loaded once at startup".** `KeyStore` stats the file at most every
  `MINION_API_KEYS_RELOAD_S` (default 5) on the next request and re-reads it when mtime, size or
  inode changed. The file is read through one file descriptor (fstat then read), must be a regular
  file and at most 1 MiB; symlinked files are allowed because secret mounts use them. A removed key
  is therefore rejected within the reload interval without a restart.
- **A malformed, unreadable or vanished file after startup fails closed**: every key gets `401` and an
  error is logged (path and validation error types only, never file content or key material) until a
  valid file is back. The first load at startup still aborts the server. Fixing the file restores access.
- `require_operator` is a FastAPI dependency (`403 OPERATOR_REQUIRED`). No existing endpoint uses it yet;
  the fleet endpoints of M5 will.
- Task creation applies the team policy: no `allowed_pool` gets `default_pool`; a pool outside
  `allowed_pools` is `403 POOL_NOT_ALLOWED`; `allowed_pools` without a default and no pool given is
  `422 POOL_REQUIRED`. A team with only `default_pool` accepts any explicit pool. No policy = as before.
- `created_by` stays a plain string: the FK is **won't-do** (it would need an 8th table, SPEC section 7).
- Executor tokens: `rotate-executor-token` replaces the hash on the same row (same `executor_id`);
  no schema change. Revoked executors are not rotated.
- Hardening of the live reload: each interval the file (<= 1 MiB, local storage; not NFS/FUSE) is read
  through one fd and its sha256 compared, so same-size in-place rewrites with a restored mtime are
  seen. The file must be replaced **atomically** (write a temp file, then `rename`); a parse result is
  discarded when the content changed while parsing. I/O and permission errors are retried every
  interval; content errors are remembered by hash. The file must be owned by root or the server's euid
  and not group/world-writable, and its resolved directory chain must not be group/world-writable
  (sticky + root-owned excepted); otherwise startup fails and a reload fails closed. Docker/K8s secret
  mounts (root-owned 0444/0644) pass. Each request uses one config snapshot for auth and pool policy.
