import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from minion_server.api import tasks as tasks_api
from minion_server.auth import load_api_keys
from minion_server.db.session import create_engine, create_sessionmaker
from minion_server.models_registry import load_models
from minion_server.services.splitter import run_splitter
from minion_server.settings import ServerSettings
from minion_server.storage import S3Storage


def create_app(settings: ServerSettings | None = None) -> FastAPI:
    """Build the app. With no settings, they are read from the environment at startup."""

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        cfg = settings or ServerSettings()
        api_keys = load_api_keys(cfg.api_keys_file) if cfg.api_keys_file else None
        engine = create_engine(cfg.database_url)
        app.state.settings = cfg
        app.state.api_keys = api_keys
        app.state.engine = engine
        app.state.sessionmaker = create_sessionmaker(engine)
        app.state.storage = S3Storage(cfg)
        app.state.models = load_models(cfg.models_file)
        splitter = asyncio.create_task(
            run_splitter(app.state.sessionmaker, app.state.storage, cfg.splitter_poll_s)
        )
        try:
            yield
        finally:
            splitter.cancel()
            await asyncio.gather(splitter, return_exceptions=True)
            await engine.dispose()

    app = FastAPI(title="open-minions server", version="0.1.0", lifespan=lifespan)

    app.include_router(tasks_api.router)

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    return app


def main() -> None:
    import uvicorn

    settings = ServerSettings()
    uvicorn.run(create_app(settings), host=settings.host, port=settings.port)
