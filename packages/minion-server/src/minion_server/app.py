import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from minion_server.api import executor as executor_api
from minion_server.api import fleet as fleet_api
from minion_server.api import tasks as tasks_api
from minion_server.auth import KeyStore
from minion_server.db.session import create_engine, create_sessionmaker
from minion_server.limits import BodyLimitMiddleware
from minion_server.models_registry import load_models
from minion_server.services.splitter import SplitLimits, run_splitter
from minion_server.settings import ServerSettings
from minion_server.storage import S3Storage


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
        app.state.api_keys = api_keys
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
    app.include_router(tasks_api.router)
    app.include_router(executor_api.router)
    app.include_router(fleet_api.router)

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    return app


def main() -> None:
    import uvicorn

    settings = ServerSettings()
    uvicorn.run(create_app(settings), host=settings.host, port=settings.port)
