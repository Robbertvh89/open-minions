"""Prometheus metrics (SPEC FR-41), opt-in via `MINION_METRICS_ENABLED`.

Labels are low-cardinality and carry no identity: the route *template* (never the concrete URL, so
no ids, team or task names), the HTTP method and status, and the acquire outcome. No executor,
owner, team, key or payload value ever becomes a label or a sample.
"""

import secrets
from typing import Any

from fastapi import APIRouter, Request, Response
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Histogram,
    generate_latest,
)
from starlette.types import ASGIApp, Message, Receive, Scope, Send

_METHODS = frozenset({"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"})
ACQUIRE_BUCKETS_S = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)
UNMATCHED = "unmatched"  # 404s and other requests that match no route: one bucket, not the URL


class Metrics:
    """Counters on a private registry (not the process-global one: apps are built per test)."""

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
            "shed: 503 before any work, error).",
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

    def observe_acquire(self, outcome: str, seconds: float) -> None:
        self.acquire_seconds.labels(outcome=outcome).observe(seconds)

    def observe(self, route: str | None, method: str, status: int) -> None:
        self.requests.labels(
            route=route or UNMATCHED,
            method=method if method in _METHODS else "OTHER",
            status=str(status),
        ).inc()

    def render(self) -> bytes:
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
