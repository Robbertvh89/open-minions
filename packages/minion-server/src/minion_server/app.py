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
from minion_server.auth import ExecutorPrincipal, KeyStore
from minion_server.db.session import create_engine, create_sessionmaker
from minion_server.errors import ExecutorRevokedError, busy_error
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
from minion_server.repositories import executors as executors_repo
from minion_server.services.splitter import SplitLimits, run_splitter
from minion_server.settings import ServerSettings
from minion_server.storage import S3Storage
from minion_server.tokencache import ExecutorTokenCache
from minion_server.ui.headers import UiHeadersMiddleware
from minion_server.ui.routes import router as ui_router


async def _preload_tokens(app: FastAPI) -> None:
    """Warm the token cache with live executors as STALE entries (one query): after a restart a
    known executor revalidates on the main pool instead of sharing the small auth lane with a
    flood. Best effort: a failure only costs the warm-up."""
    cfg: ServerSettings = app.state.settings
    cache: ExecutorTokenCache[ExecutorPrincipal] = app.state.executor_tokens
    try:
        async with app.state.sessionmaker() as session:
            idents = await executors_repo.list_active_identities(
                session, cfg.executor_auth_cache_max_entries
            )
    except Exception as exc:
        structlog.get_logger().warning("auth_cache_preload_failed", error=type(exc).__name__)
        return
    for i in idents:
        cache.preload(
            i.token_hash,
            ExecutorPrincipal(i.executor_id, i.owner, i.pool, i.token_hash),
            i.token_expires_at,
        )


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
        # Dedicated lane for lookups of tokens never seen (TASK-32): a flood of garbage bearers can
        # hold at most db_auth_pool_size connections here, never one from the main pool.
        auth_engine = create_engine(
            cfg.database_url,
            pool_size=cfg.db_auth_pool_size,
            max_overflow=0,
            pool_timeout=cfg.auth_lookup_timeout_s,
            pre_ping=False,  # a ping would double the lookups a flood can cost on this lane
        )
        app.state.auth_engine = auth_engine
        app.state.auth_sessionmaker = create_sessionmaker(auth_engine)
        app.state.executor_tokens = ExecutorTokenCache[ExecutorPrincipal](
            cfg.executor_auth_cache_ttl_s, cfg.executor_auth_cache_max_entries
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
        await _preload_tokens(app)
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
            await auth_engine.dispose()
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

    @app.exception_handler(ExecutorRevokedError)
    async def revoked_handler(request: Request, exc: ExecutorRevokedError) -> Response:
        """A mutation found the token's executor revoked/rotated: refuse and forget the token."""
        cache = getattr(request.app.state, "executor_tokens", None)
        if cache is not None and exc.token_hash is not None:
            cache.put_invalid(exc.token_hash)
        return JSONResponse(
            {"detail": {"code": "UNAUTHORIZED", "message": "invalid or missing credentials"}},
            status_code=401,
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
            per_worker=settings.db_pool_size
            + settings.db_max_overflow
            + settings.db_auth_pool_size,
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
