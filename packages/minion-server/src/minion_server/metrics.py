"""Prometheus metrics (SPEC FR-41), opt-in via `MINION_METRICS_ENABLED`.

Labels are low-cardinality and carry no identity: the route *template* (never the concrete URL, so
no ids, team or task names), the HTTP method and status, and the acquire outcome. No executor,
owner, team, key or payload value ever becomes a label or a sample.
"""

import contextlib
import os
import re
import secrets
import stat
import sys
import tempfile
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Request, Response
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Histogram,
    generate_latest,
    multiprocess,
)
from starlette.types import ASGIApp, Message, Receive, Scope, Send

_METHODS = frozenset({"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"})
ACQUIRE_BUCKETS_S = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)
UNMATCHED = "unmatched"  # 404s and other requests that match no route: one bucket, not the URL


MULTIPROC_ENV = "PROMETHEUS_MULTIPROC_DIR"
_METRIC_FILE = re.compile(r"^(counter|histogram|gauge)_.*\.db$")


def prepare_metrics_dir(path: str | None) -> tuple[str, bool]:
    """Return (dir, is_private_temp) for the multiprocess samples, emptied of old metric files.

    None: a fresh private mkdtemp dir (0700). A given path must be a real directory (not a
    symlink) owned by us with no group/other access, else `SystemExit(2)`: the dir holds mmap files
    every worker trusts, so a directory another user can write to would let them forge metrics.
    Only `counter_*.db` / `histogram_*.db` / `gauge_*.db` regular files are unlinked, through a
    directory fd opened with O_NOFOLLOW, so nothing outside the directory is ever touched.
    """
    if path is None:
        return tempfile.mkdtemp(prefix="minion-metrics-"), True
    try:
        st = os.lstat(path)
    except FileNotFoundError:
        Path(path).mkdir(mode=0o700, parents=True)
        st = os.lstat(path)
    problem = None
    if not stat.S_ISDIR(st.st_mode):
        problem = "is not a real directory (symlinks are refused)"
    elif st.st_uid != os.geteuid():
        problem = f"is not owned by uid {os.geteuid()}"
    elif st.st_mode & 0o077:
        problem = f"has mode {stat.S_IMODE(st.st_mode):04o}, expected no group/other access (0700)"
    if problem:
        sys.stderr.write(f"MINION_METRICS_MULTIPROC_DIR {path!r} {problem}\n")
        raise SystemExit(2)
    try:
        fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    except OSError as exc:
        sys.stderr.write(f"MINION_METRICS_MULTIPROC_DIR {path!r} cannot be opened: {exc}\n")
        raise SystemExit(2) from exc
    try:
        fst = os.fstat(fd)
        if (fst.st_ino, fst.st_dev) != (st.st_ino, st.st_dev):
            sys.stderr.write(f"MINION_METRICS_MULTIPROC_DIR {path!r} changed while opening\n")
            raise SystemExit(2)
        with os.scandir(fd) as it:
            names = [
                e.name
                for e in it
                if _METRIC_FILE.match(e.name) and e.is_file(follow_symlinks=False)
            ]
        for name in names:
            with contextlib.suppress(FileNotFoundError):
                os.unlink(name, dir_fd=fd)
    finally:
        os.close(fd)
    return path, False


def mark_worker_dead() -> None:
    """Worker shutdown hook: let prometheus_client drop this pid's live-gauge files. Counter and
    histogram files stay on purpose (their totals must outlive the worker)."""
    if os.environ.get(MULTIPROC_ENV):
        multiprocess.mark_process_dead(os.getpid())  # type: ignore[no-untyped-call]


class Metrics:
    """Counters on a private registry (not the process-global one: apps are built per test).

    With `PROMETHEUS_MULTIPROC_DIR` set (`main()` does this in the parent for workers > 1, before
    the workers start) every sample lives in a per-process mmap file and `render` sums all of them
    (MultiProcessCollector). Only counters and histograms are used; a Gauge added later needs an
    explicit `multiprocess_mode`.
    """

    def __init__(self) -> None:
        self.registry = CollectorRegistry()
        self.requests = Counter(
            "minion_http_requests_total",
            "HTTP requests by route template, method and status.",
            ("route", "method", "status"),
            registry=self.registry,
        )
        self.acquires = Counter(
            "minion_acquire_total",
            "Executor acquire calls by outcome (assigned: got a subtask, empty: no work, "
            "shed: 503 SERVER_BUSY or ACQUIRE_DEADLINE, rate_limited: 429 per-executor "
            "in-flight cap; a raised acquire (error) is timed in minion_acquire_seconds, "
            "not counted here).",
            ("outcome",),
            registry=self.registry,
        )
        self.acquire_seconds = Histogram(
            "minion_acquire_seconds",
            "Server-side acquire duration (selection, race, presign) by outcome.",
            ("outcome",),
            buckets=ACQUIRE_BUCKETS_S,
            registry=self.registry,
        )
        self.executor_auth = Counter(
            "minion_executor_auth_total",
            "Executor token authentications by outcome (cache_hit, negative_hit, db_valid, "
            "db_invalid, expired: cached token past token_expires_at, busy: a pool wait timed out "
            "-> 503).",
            ("outcome",),
            registry=self.registry,
        )
        self.task_settle_failed = Counter(
            "minion_task_settle_failed_total",
            "Post-commit task settles that raised (the watchdog's settle_stuck finishes the task).",
            registry=self.registry,
        )

    def settle_failed(self) -> None:
        self.task_settle_failed.inc()

    def observe_acquire(self, outcome: str, seconds: float) -> None:
        self.acquire_seconds.labels(outcome=outcome).observe(seconds)

    def observe(self, route: str | None, method: str, status: int) -> None:
        self.requests.labels(
            route=route or UNMATCHED,
            method=method if method in _METHODS else "OTHER",
            status=str(status),
        ).inc()

    def render(self) -> bytes:
        directory = os.environ.get(MULTIPROC_ENV)
        if directory:
            registry = CollectorRegistry()
            multiprocess.MultiProcessCollector(registry, path=directory)  # type: ignore[no-untyped-call]
            return generate_latest(registry)
        return generate_latest(self.registry)


def _metrics(scope: Scope) -> Metrics | None:
    app: Any = scope.get("app")
    m = getattr(getattr(app, "state", None), "metrics", None)
    return m if isinstance(m, Metrics) else None


class MetricsMiddleware:
    """Counts every HTTP response by route template (pure ASGI; a no-op while disabled)."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        metrics = _metrics(scope) if scope["type"] == "http" else None
        if metrics is None:
            await self.app(scope, receive, send)
            return
        status = 500  # an exception that escapes before a response start is a 500

        async def counting_send(msg: Message) -> None:
            nonlocal status
            if msg["type"] == "http.response.start":
                status = int(msg["status"])
            await send(msg)

        try:
            await self.app(scope, receive, counting_send)
        finally:
            route = getattr(scope.get("route"), "path", None)
            metrics.observe(route, scope["method"], status)


router = APIRouter()


@router.get("/metrics", include_in_schema=False)
async def metrics_endpoint(request: Request) -> Response:
    """Prometheus exposition. 404 while disabled; with `MINION_METRICS_TOKEN` set, a matching
    `Authorization: Bearer` is required (401 otherwise)."""
    metrics = _metrics(request.scope)
    if metrics is None:
        return Response(status_code=404)
    token = request.app.state.settings.metrics_token
    if token is not None:
        given = request.headers.get("authorization", "")
        want = f"Bearer {token.get_secret_value()}"
        if not secrets.compare_digest(given.encode(), want.encode()):
            return Response(status_code=401, headers={"WWW-Authenticate": "Bearer"})
    return Response(content=metrics.render(), media_type=CONTENT_TYPE_LATEST)
