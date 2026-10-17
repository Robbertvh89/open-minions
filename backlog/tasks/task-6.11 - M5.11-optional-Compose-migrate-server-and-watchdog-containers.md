---
id: TASK-6.11
title: 'M5.11 (optional) Compose: migrate, server and watchdog containers'
status: In Progress
assignee: []
created_date: '2026-10-17 03:33'
updated_date: '2026-10-17 18:08'
labels:
  - M5
dependencies:
  - TASK-6.5
parent_task_id: TASK-6
ordinal: 42000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
deploy/Dockerfile (one image, non-root, read-only rootfs); compose services migrate (alembic upgrade head), server, watchdog with healthchecks and depends_on, per ARCHITECTURE 6. Can slip to M6.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 docker compose -f deploy/compose.yml --profile app up -d brings up postgres, s3, grafana, migrate (one-shot), server and watchdog; server healthy and a fake-engine task completes end to end
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
## Goal
One non-root, read-only-rootfs image (deploy/Dockerfile) built with uv from the workspace lockfile; compose services `migrate` (one-shot `alembic upgrade head`), `server`, `watchdog` with healthchecks and depends_on, so `compose up` + a host executor drives a task to SUCCEEDED and Grafana shows it. Maps to FR-40 (dashboards), FR-41 (/healthz), FR-30..32 (watchdog reclaim), NF-5 (stateless server/watchdog), ARCHITECTURE §6. Invariants 1 and 4 must hold: executor stays on the host, talks only to the server REST API; payloads go host <-> S3 via presigned URLs on the loopback public endpoint.

## Touchpoints
- deploy/compose.yml:1-80 (header says server/watchdog come later, line 3; postgres healthcheck line 20 uses the socket)
- packages/minion-server/src/minion_server/app.py:116-120 (main binds settings.host, default 127.0.0.1 in settings.py:24, so the container needs MINION_HOST=0.0.0.0); app.py:104 /healthz
- settings.py:21 env_file=".env" relative to cwd (keep .env out of the image); settings.py:61 models_file default deploy/models.yaml (relative to WORKDIR /app); settings.py:178 public S3 endpoint must be https or loopback, so http://localhost:${MINION_S3_PORT} works; storage.py:58-64 internal client vs presign signer (internal http://s3:9000, public http://localhost:9000)
- auth.py:153-235 keys file: file + every dir on the path must be owned by 0 or euid and not g/w-writable. Checked live: Docker Desktop VirtioFS shows bind-mounted files as owned by the container uid (10001, mode kept), so this passes on macOS. On Linux, chown the dir to 10001. Mount the DIRECTORY, not the file: an atomic temp+rename replace (the documented reload path) does not reach a single-file bind mount.
- packages/minion-watchdog/src/minion_watchdog/main.py:96-118 run_watchdog loop, settings.py:5 (it has no health signal yet)
- migrations/env.py:13 (URL from ServerSettings), alembic.ini (script_location %(here)s/migrations, prepend_sys_path .)
- .github/workflows/ci.yml:24 runs the full `pytest -v` (integration included, Docker available on ubuntu-latest)

## Steps
1. Image (~130 lines). Create deploy/Dockerfile and a root .dockerignore.
   - Pin both base images by exact version plus digest (resolve with `docker buildx imagetools inspect`): `ghcr.io/astral-sh/uv:0.11.23@sha256:...` (matches the local uv and uv_build<0.12) and `python:3.12.<patch>-slim-bookworm@sha256:...`.
   - Build stage: ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=never UV_PROJECT_ENVIRONMENT=/opt/venv. COPY pyproject.toml, uv.lock and all four packages/*/pyproject.toml (the workspace glob needs the executor's metadata too). Then `uv sync --locked --no-dev --no-install-workspace --package minion-watchdog` (cache mount), then COPY common/server/watchdog src, then `uv sync --locked --no-dev --no-editable --package minion-watchdog`. That installs watchdog + server + common, so all three scripts. The executor and pyobjc are never installed.
   - Runtime stage, same python base: groupadd/useradd uid/gid 10001 with nologin and home /nonexistent. COPY --from=build /opt/venv. WORKDIR /app. COPY alembic.ini, migrations/ and deploy/models.yaml. ENV PATH=/opt/venv/bin:$PATH PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 HOME=/tmp MINION_HOST=0.0.0.0 MINION_PORT=8080. `USER 10001:10001`, EXPOSE 8080, CMD ["minion-server"]. No secrets and no .env in any layer.
   - .dockerignore is an allow-list: `*` first, then re-include `pyproject.toml uv.lock alembic.ini migrations/ packages/*/pyproject.toml packages/minion-{common,server,watchdog}/src/ deploy/models.yaml`, then exclude `**/__pycache__`.
   - Tests:
     - unit packages/minion-server/tests/test_deploy_files.py parses the Dockerfile and .dockerignore. It asserts: every FROM has `@sha256:`; the last USER is numeric and not 0; `uv sync --locked --no-dev` is present; .dockerignore starts with `*` and re-includes neither `.env` nor `deploy/secrets`.
     - integration test_deploy_image.py (`pytest.mark.integration`, skipped if no `docker` CLI): `docker build -f deploy/Dockerfile .`, then `docker run --rm --read-only --tmpfs /tmp <img>` runs `id -u` (not 0), `alembic --help`, `minion-server-admin --help` and `minion-watchdog --help`, or a `python -c` import of the three packages. It also checks that `/app/.env` and the executor package are absent.
2. Watchdog health file (~60 lines).
   - WatchdogSettings.watchdog_health_file: str | None = None. In run_watchdog, touch it (os.utime, or create it 0600) at start and after every successful tick. Skip the touch on a failed tick so the file goes stale under DB backoff. A touch error is logged with the class only and never stops the loop.
   - Health rule for the container: the mtime must be newer than 3*interval + 30 s. Do the check in compose with `python -c` (no new CLI surface).
   - Unit tests in packages/minion-watchdog/tests: touched after a good tick, not touched after a failed one, unset = no file, an unwritable path does not crash the loop.
3. Compose services (~150 lines). Edit deploy/compose.yml.
   - Shared anchor `x-minion-app`: build {context: .., dockerfile: deploy/Dockerfile}, image open-minions-app:dev, `profiles: [app]`, read_only: true, tmpfs /tmp, cap_drop [ALL], security_opt [no-new-privileges:true], user "10001:10001".
   - Explicit environment (no env_file; the root .env has localhost URLs): MINION_DATABASE_URL=postgresql+asyncpg://minion:minion@postgres:5432/minion (dev default, same as POSTGRES_PASSWORD), MINION_S3_ENDPOINT_URL=http://s3:9000, MINION_S3_PUBLIC_ENDPOINT_URL=http://localhost:${MINION_S3_PORT:-9000}, S3 keys minion/minion-secret (dev defaults matching s3.json).
   - migrate: command ["alembic","upgrade","head"], restart "no", depends_on postgres service_healthy.
   - server: depends_on migrate service_completed_successfully and s3-init service_completed_successfully. ports "127.0.0.1:${MINION_SERVER_HOST_PORT:-8080}:8080". MINION_API_KEYS_FILE=/run/minion-secrets/api-keys.yaml, with volume `${MINION_SECRETS_DIR:-./secrets}:/run/minion-secrets:ro`. MINION_UI_SESSION_SECRET=${MINION_UI_SESSION_SECRET:-} (empty = UI off) and MINION_UI_ALLOWED_HOSTS=${MINION_UI_ALLOWED_HOSTS:-}. healthcheck `python -c "import urllib.request,sys; urllib.request.urlopen('http://127.0.0.1:8080/healthz',timeout=2)"`.
   - watchdog: command ["minion-watchdog"], depends_on migrate completed, MINION_WATCHDOG_HEALTH_FILE=/tmp/watchdog.health, mtime healthcheck, start_period 30s.
   - postgres healthcheck: change to `pg_isready -h 127.0.0.1 ...`. During first-volume initdb the temp server listens on the socket only, so the socket check reports healthy too early and migrate would race initdb.
   - Add deploy/secrets/ to .gitignore. Update the header comment.
   - Tests:
     - unit in test_deploy_files.py (yaml): all three app services are read_only, cap_drop ALL, no-new-privileges, non-root user and profile app. server/watchdog depend on migrate service_completed_successfully, and server also on s3-init. Every `ports` entry starts with 127.0.0.1. The server port uses MINION_SERVER_HOST_PORT. server/watchdog have healthchecks. Every secret-ish env (UI secret, metrics token) is a `${...}` interpolation, not a literal.
     - integration: `docker compose -f deploy/compose.yml --profile app config -q` exits 0.
4. Docs (~60 lines, docs-keeper).
   - CLAUDE.md commands: `docker compose -f deploy/compose.yml --profile app up -d --build`, MINION_SERVER_HOST_PORT and the secrets dir.
   - ARCHITECTURE §6: container hardening, the migrate one-shot, the profile, and the Linux chown note. §7 upgrade note: the migrate container replaces `uv run alembic upgrade head`.
   - .env.example: MINION_WATCHDOG_HEALTH_FILE. State that the compose vars (MINION_SERVER_HOST_PORT, MINION_SECRETS_DIR, MINION_UI_SESSION_SECRET) are read from deploy/.env or the shell (compose project dir = deploy/).
   - SPEC FR-41 status line.
5. Live verification (verifier, AC #1):
   - mkdir -m700 deploy/secrets. Run `uv run minion-server-admin generate-api-key --name dev --teams t1`, put the printed entry in deploy/secrets/api-keys.yaml and chmod 600 it.
   - `MINION_SERVER_HOST_PORT=18080 docker compose -f deploy/compose.yml --profile app up -d --build`.
   - `docker compose ps`: migrate exited 0; server and watchdog healthy.
   - `curl -fsS localhost:18080/healthz`.
   - `docker compose logs watchdog` shows the JSON `watchdog_started` line.
   - Issue an executor token with `docker compose exec server minion-server-admin issue-executor-token --owner dev` (or host uv + .env), upload input to localhost:9000 and submit a task.
   - `MINION_EXECUTOR_DEV=1 uv run minion-executor run --server http://localhost:18080 --engines fake` (or mlx). Expect the task to reach SUCCEEDED, with output objects in S3.
   - Grafana at localhost:3000: the minion-tasks dashboard shows the task and attempts.
   - Then `docker compose down` (keep the volume).

## Risks / open questions
- The AC says `docker compose up -d`. The plan puts migrate/server/watchdog behind `profiles: [app]` so that a plain `up -d` keeps today's infra-only flow (native `uv run minion-server` on 8080, which the user's other project sometimes holds). Proposal: reword AC #1 to `--profile app up -d` (or COMPOSE_PROFILES=app in deploy/.env). Orchestrator decides.
- Keys-file ownership on Linux hosts (uid differs from 10001): document the chown. No code change and no weakening of auth.py.
- The CI integration build adds ~1-2 min. Acceptable; the `docker` skip keeps local runs without Docker green.
- No deviation from the paper, so no ADR. The watchdog health file is additive config only.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Docs (step 4): CLAUDE.md Commands (--profile app up line, MINION_SERVER_HOST_PORT, MINION_SECRETS_DIR, migrate replaces manual alembic in container mode, native flow kept); docs/ARCHITECTURE.md §6 (container hardening, startup order, loopback port, secrets mount, Linux chown 10001 note, macOS Docker Desktop as is) and §2.3 (MINION_WATCHDOG_HEALTH_FILE); docs/SPEC.md FR-41 status line; .env.example (watchdog health file, compose vars read from deploy/.env). Key files: CLAUDE.md, .env.example, docs/ARCHITECTURE.md, docs/SPEC.md. Open: compose healthcheck for watchdog reads MINION_WATCHDOG_INTERVAL_S from container env, which compose never sets.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Compose app profile (migrate, server, watchdog) documented in CLAUDE.md, ARCHITECTURE §2.3/§6, SPEC FR-41 and .env.example. Watchdog health file (MINION_WATCHDOG_HEALTH_FILE) documented. The watchdog and its healthcheck share MINION_WATCHDOG_INTERVAL_S (compose passes it, default 60).
<!-- SECTION:FINAL_SUMMARY:END -->
