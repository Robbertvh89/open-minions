# One image for migrate, server and watchdog (ARCHITECTURE §6). The executor is never in it.
# Build from the repo root: docker build -f deploy/Dockerfile .

FROM ghcr.io/astral-sh/uv:0.11.23@sha256:d0a0a753ab981624b49c97abc98821c1c09f4ca69d1ef5cee69c501be3d88479 AS uv

FROM python:3.12.15-slim-bookworm@sha256:34386ef0cb081344d7ec1c103ba398e6e9f64e9ab3a1509accc92a4e24a07258 AS build
COPY --from=uv /uv /usr/local/bin/uv
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=never UV_PROJECT_ENVIRONMENT=/opt/venv
WORKDIR /src
# The workspace glob needs every member's metadata, the executor's and the CLI's included.
COPY pyproject.toml uv.lock ./
COPY packages/minion-common/pyproject.toml packages/minion-common/pyproject.toml
COPY packages/minion-server/pyproject.toml packages/minion-server/pyproject.toml
COPY packages/minion-watchdog/pyproject.toml packages/minion-watchdog/pyproject.toml
COPY packages/minion-executor/pyproject.toml packages/minion-executor/pyproject.toml
COPY packages/minion-cli/pyproject.toml packages/minion-cli/pyproject.toml
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --no-install-workspace --package minion-watchdog
COPY packages/minion-common/src packages/minion-common/src
COPY packages/minion-server/src packages/minion-server/src
COPY packages/minion-watchdog/src packages/minion-watchdog/src
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --no-editable --package minion-watchdog

FROM python:3.12.15-slim-bookworm@sha256:34386ef0cb081344d7ec1c103ba398e6e9f64e9ab3a1509accc92a4e24a07258
RUN groupadd --gid 10001 minion \
    && useradd --uid 10001 --gid 10001 --no-create-home --home-dir /nonexistent \
       --shell /usr/sbin/nologin minion
COPY --from=build /opt/venv /opt/venv
# No setuid/setgid binaries (su, mount, ...): nothing to escalate through.
RUN find / -xdev -perm /6000 -type f -exec chmod a-s {} +
WORKDIR /app
COPY alembic.ini ./
COPY migrations/ migrations/
COPY deploy/models.yaml deploy/models.yaml
ENV PATH=/opt/venv/bin:$PATH \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    HOME=/tmp \
    MINION_HOST=0.0.0.0 \
    MINION_PORT=8080
USER 10001:10001
EXPOSE 8080
CMD ["minion-server"]
