# ADR-0007: User API keys from a config file, not a database table

- Status: **Accepted** (implemented in TASK-2.1; `auth.lookup_user` full-scan and startup load verified)
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
- **Revoking a user key requires removing it from the file and restarting the server.** Keys are not
  re-read at runtime. Executor tokens are different: they live in the database and are revoked
  immediately (`minion-server-admin revoke-executor-token`).
- Keys are not individually expirable; rotation means generating a new entry and deleting the old one.
- `created_by` is not referentially checked and can outlive a removed key; audit by name only.
- Migrating to a table (or OIDC) in M5 is a migration plus a change to `current_user` only; the
  route-level contract (`Authorization: Bearer`, team scoping) stays the same.
