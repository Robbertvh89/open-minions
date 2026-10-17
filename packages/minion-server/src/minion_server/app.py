import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI

from minion_server.api import executor as executor_api
from minion_server.api import fleet as fleet_api
from minion_server.api import tasks as tasks_api
from minion_server.auth import KeyStore
from minion_server.db.session import create_engine, create_sessionmaker
from minion_server.limits import BodyLimitMiddleware
from minion_server.metrics import Metrics, MetricsMiddleware
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
            cfg.database_url, pool_size=cfg.db_pool_size, max_overflow=cfg.db_max_overflow
        )
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
            }
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

    app = FastAPI(title="open-minions server", version="0.1.0", lifespan=lifespan)

    app.add_middleware(BodyLimitMiddleware)
    app.add_middleware(UiHeadersMiddleware)
    app.add_middleware(MetricsMiddleware)  # outermost: counts 413s too
    app.include_router(tasks_api.router)
    app.include_router(executor_api.router)
    app.include_router(fleet_api.router)
    app.include_router(metrics_router)
    app.include_router(ui_router)

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    return app


def main() -> None:
    import uvicorn

    settings = ServerSettings()
    uvicorn.run(create_app(settings), host=settings.host, port=settings.port)
