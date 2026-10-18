import asyncio
import os
import shutil
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI, Request
from sqlalchemy.exc import TimeoutError as PoolTimeoutError
from starlette.responses import JSONResponse, Response

from minion_server.api import executor as executor_api
from minion_server.api import fleet as fleet_api
from minion_server.api import tasks as tasks_api
from minion_server.auth import KeyStore
from minion_server.db.session import create_engine, create_sessionmaker
from minion_server.errors import busy_error
from minion_server.limits import ArrivalStampMiddleware, BodyLimitMiddleware
from minion_server.metrics import (
    MULTIPROC_ENV,
    Metrics,
    MetricsMiddleware,
    mark_worker_dead,
    prepare_metrics_dir,
)
from minion_server.metrics import router as metrics_router
from minion_server.models_registry import load_models
from minion_server.netutil import is_loopback
from minion_server.ratelimit import MinIntervalLimiter
from minion_server.services.splitter import SplitLimits, run_splitter
from minion_server.settings import ServerSettings
from minion_server.storage import S3Storage
from minion_server.ui.headers import UiHeadersMiddleware
from minion_server.ui.routes import router as ui_router


def create_app(settings: ServerSettings | None = None) -> FastAPI:
    """Build the app. With no settings, they are read from the environment at startup."""

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        cfg = settings or ServerSettings()
        api_keys = KeyStore(cfg.api_keys_file, cfg.api_keys_reload_s) if cfg.api_keys_file else None
        engine = create_engine(
            cfg.database_url,
            pool_size=cfg.db_pool_size,
            max_overflow=cfg.db_max_overflow,
            pool_timeout=cfg.db_pool_timeout_s,
        )
        app.state.acquire_slots = asyncio.Semaphore(cfg.acquire_concurrency_limit)
        app.state.acquire_inflight = {}  # executor_id -> acquires in flight (cap per executor)
        app.state.settings = cfg
        app.state.metrics = Metrics() if cfg.metrics_enabled else None
        if cfg.metrics_enabled and cfg.metrics_token is None and not is_loopback(cfg.host):
            structlog.get_logger().warning("metrics_unauthenticated", host=cfg.host)
        if cfg.executor_online_idle_s != 120.0 or cfg.executor_online_busy_s != 600.0:
            structlog.get_logger().warning(
                "executor_online_thresholds_differ_from_dashboards",
                idle_s=cfg.executor_online_idle_s,
                busy_s=cfg.executor_online_busy_s,
                note="minion_ro.executor.online (migration 0007) hardcodes 120/600",
            )
        app.state.limiter = MinIntervalLimiter(
            {
                "register": cfg.register_min_interval_s,
                "acquire": cfg.acquire_min_interval_s,
                "presence": cfg.presence_min_interval_s,
                "preference": cfg.preference_min_interval_s,
            },
            max_entries=cfg.rate_limit_max_entries,
        )
        app.state.api_keys = api_keys
        app.state.ui_secret = (
            cfg.ui_session_secret.get_secret_value().encode("utf-8")
            if cfg.ui_session_secret is not None
            else None
        )
        app.state.engine = engine
        app.state.sessionmaker = create_sessionmaker(engine)
        app.state.storage = S3Storage(cfg)
        app.state.models = load_models(cfg.models_file)
        splitter = asyncio.create_task(
            run_splitter(
                app.state.sessionmaker,
                app.state.storage,
                cfg.splitter_poll_s,
                SplitLimits.from_settings(cfg),
            )
        )
        app.state.splitter_task = splitter
        try:
            yield
        finally:
            splitter.cancel()
            await asyncio.gather(splitter, return_exceptions=True)
            await engine.dispose()
            if cfg.metrics_enabled:
                mark_worker_dead()

    app = FastAPI(title="open-minions server", version="0.1.0", lifespan=lifespan)

    app.add_middleware(BodyLimitMiddleware)
    app.add_middleware(ArrivalStampMiddleware)
    app.add_middleware(UiHeadersMiddleware)
    app.add_middleware(MetricsMiddleware)  # outermost: counts 413s too
    app.include_router(tasks_api.router)
    app.include_router(executor_api.router)
    app.include_router(fleet_api.router)
    app.include_router(metrics_router)
    app.include_router(ui_router)

    @app.exception_handler(PoolTimeoutError)
    async def pool_timeout_handler(request: Request, exc: PoolTimeoutError) -> Response:
        """No connection within db_pool_timeout_s: overload, not a bug (TASK-7.4.4). Never a 500;
        the log carries the route template only (no ids, no payload)."""
        route = request.scope.get("route")
        structlog.get_logger().warning(
            "db_pool_timeout", route=getattr(route, "path", None), method=request.method
        )
        err = busy_error("SERVER_BUSY", "server is busy, retry later", retry_after=(2, 10))
        return JSONResponse(
            {"detail": err.detail}, status_code=err.status_code, headers=err.headers
        )

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    return app


def main() -> None:
    import uvicorn

    settings = ServerSettings()
    if settings.connection_budget >= settings.connection_limit:
        structlog.get_logger().warning(
            "db_connection_budget_exceeded",
            workers=settings.server_workers,
            per_worker=settings.db_pool_size + settings.db_max_overflow,
            watchdog=settings.watchdog_db_pool_size,
            budget=settings.connection_budget,
            db_max_connections=settings.db_max_connections,
            reserved=settings.db_reserved_connections,
            limit=settings.connection_limit,
        )
    if settings.server_workers == 1:
        # single process: prometheus_client already chose its value class at import, so an
        # inherited multiprocess dir cannot be dropped here; validate it like an operator dir
        inherited = os.environ.get(MULTIPROC_ENV)
        if inherited:
            os.environ[MULTIPROC_ENV], _ = prepare_metrics_dir(inherited)
        uvicorn.run(create_app(settings), host=settings.host, port=settings.port)
        return
    metrics_dir: str | None = None
    remove_dir = False
    if settings.metrics_enabled:
        # Set in the parent before spawning workers (fresh interpreters inherit the env).
        metrics_dir, remove_dir = prepare_metrics_dir(settings.metrics_multiproc_dir)
        os.environ[MULTIPROC_ENV] = metrics_dir
    try:
        uvicorn.run(
            "minion_server.app:create_app",
            factory=True,
            workers=settings.server_workers,
            host=settings.host,
            port=settings.port,
        )
    finally:
        if metrics_dir is not None and remove_dir:
            shutil.rmtree(metrics_dir, ignore_errors=True)
