---
name: security-audit
description: Security audit checklist for open-minions branches — authn/authz, executor tokens, presigned URL scope, SQL injection, secrets, dependency CVEs, untrusted model code, data hygiene on volunteer laptops, logging, deployment config. Use on PRs touching APIs, auth, S3, executor, dependencies or deploy config, or when asked for a "security audit/review".
---

# Security audit

Threat model: SPEC §8. Executors run on employee laptops with confidential data; the server is the
only DB client and hands out S3 access; models come from the internet.

## 1. Automated scans
```bash
uv run ruff check --select S .                              # bandit rules
uv export --no-hashes --no-dev --no-emit-workspace --format requirements-txt > /tmp/minion-reqs.txt
uvx --python 3.12 pip-audit --strict -r /tmp/minion-reqs.txt     # non-zero exit = finding (CVE or audit error)
git diff main...HEAD | grep -nEi "(secret|password|token|api[_-]?key|BEGIN [A-Z ]*PRIVATE KEY)" || true
```
Dev-only credentials in `deploy/` and `.env.example` are acceptable; anything else is a finding.

## 2. Manual checklist (only what the diff touches)
**AuthN/AuthZ**
- Every user endpoint requires a user API key; every executor endpoint an executor token.
- Tokens: ≥256-bit random, compared in constant time (`hmac.compare_digest`), stored as SHA-256 hash,
  revocable; never logged.
- Executor endpoints check that `acquirement_id` belongs to the calling executor (not just exists).
- Task access scoped by team where applicable; `allowed_pool` enforced in acquire.

**Data path**
- Presigned URLs: single object, method-specific (GET input / PUT output), TTL ≤ 1 h, output key
  attempt-scoped so an executor cannot overwrite other subtasks' outputs.
- Server never streams payloads; no S3 credentials leave the server.
- Byte ranges validated server-side; user-supplied S3 URIs restricted to the configured bucket/prefixes
  (no SSRF to arbitrary endpoints).

**Injection**
- SQL only via bound parameters / SQLAlchemy constructs; no f-strings in SQL.
- No `shell=True`, no `eval/exec`, no `pickle` on untrusted data; subprocess args as lists.
- YAML loaded with `safe_load`.

**Executor / models**
- Model ids restricted to `models.yaml` allow-list; HF `trust_remote_code=False`; pinned revisions.
- Local input/output files deleted after upload; temp dirs `0700`.
- No sleep prevention, no privilege escalation, no root.
- Token stored in Keychain (or env for dev), never in plain-text config.

**Logging & errors**
- No payload text, prompts, completions or embeddings in logs/metrics/errors.
- Error responses don't leak stack traces or SQL.

**Deployment**
- TLS required outside local dev; dev credentials not reused in prod config.
- Containers pinned by tag; no `latest`.

## 3. Report
`path:line: <CRITICAL|HIGH|MEDIUM|LOW>: <issue>. <one-sentence exploit>. <fix>.`
Verdict **PASS** (no CRITICAL/HIGH) or **BLOCK**.
