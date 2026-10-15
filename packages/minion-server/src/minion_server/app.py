from fastapi import FastAPI

from minion_server.settings import ServerSettings


def create_app() -> FastAPI:
    app = FastAPI(title="open-minions server", version="0.1.0")

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    return app


def main() -> None:
    import uvicorn

    settings = ServerSettings()
    uvicorn.run(create_app(), host=settings.host, port=settings.port)
