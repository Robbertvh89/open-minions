"""Operator CLI: `minion-server-admin issue-executor-token | hash-api-key`."""

import asyncio
import secrets
from datetime import UTC, datetime, timedelta
from typing import Annotated

import typer
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from minion_server.auth import sha256_hex
from minion_server.db.session import create_engine, create_sessionmaker
from minion_server.repositories import executors as executors_repo
from minion_server.settings import ServerSettings

app = typer.Typer(help="open-minions server administration", no_args_is_help=True)


async def issue_executor_token(
    sessionmaker: async_sessionmaker[AsyncSession], *, owner: str, pool: str, ttl_days: int
) -> tuple[str, str]:
    """Create an executor row and return `(executor_id, token)`. Only the hash is stored."""
    token = secrets.token_urlsafe(32)
    async with sessionmaker() as session, session.begin():
        executor_id = await executors_repo.insert_unregistered(
            session,
            token_hash=sha256_hex(token),
            owner=owner,
            pool=pool,
            token_expires_at=datetime.now(UTC) + timedelta(days=ttl_days),
        )
    return str(executor_id), token


@app.command("issue-executor-token")
def issue_executor_token_cmd(
    owner: Annotated[str, typer.Option(help="Mac owner (user name)")],
    pool: Annotated[str, typer.Option(help="Executor pool")] = "default",
    ttl_days: Annotated[int | None, typer.Option(min=1, help="Token lifetime in days")] = None,
) -> None:
    settings = ServerSettings()
    days = ttl_days or settings.executor_token_ttl_days

    async def _run() -> tuple[str, str]:
        engine = create_engine(settings.database_url)
        try:
            return await issue_executor_token(
                create_sessionmaker(engine), owner=owner, pool=pool, ttl_days=days
            )
        finally:
            await engine.dispose()

    executor_id, token = asyncio.run(_run())
    typer.echo(f"executor_id: {executor_id}")
    typer.echo(f"token (shown once): {token}")


@app.command("hash-api-key")
def hash_api_key(key: Annotated[str, typer.Argument(help="API key to hash")]) -> None:
    """Print the sha256 to put in the API keys YAML file."""
    typer.echo(sha256_hex(key))


if __name__ == "__main__":
    app()
