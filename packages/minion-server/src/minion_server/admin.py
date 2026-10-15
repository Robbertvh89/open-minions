"""Operator CLI: `generate-api-key | issue-executor-token | revoke-executor-token`."""

import asyncio
import re
import secrets
import uuid
from datetime import UTC, datetime, timedelta
from typing import Annotated

import typer
import yaml
from minion_common.schemas import SLUG_PATTERN
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from minion_server.auth import sha256_hex
from minion_server.db.session import create_engine, create_sessionmaker
from minion_server.repositories import executors as executors_repo
from minion_server.settings import ServerSettings

MAX_TTL_DAYS = 365
NAME_PATTERN = r"[\w.@-]{1,100}"

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
    ttl_days: Annotated[
        int | None, typer.Option(min=1, max=MAX_TTL_DAYS, help="Token lifetime in days")
    ] = None,
) -> None:
    settings = ServerSettings()
    days = min(ttl_days or settings.executor_token_ttl_days, MAX_TTL_DAYS)

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


@app.command("revoke-executor-token")
def revoke_executor_token_cmd(
    executor_id: Annotated[uuid.UUID, typer.Option(help="Executor to revoke")],
) -> None:
    settings = ServerSettings()

    async def _run() -> bool:
        engine = create_engine(settings.database_url)
        try:
            async with create_sessionmaker(engine)() as session, session.begin():
                return await executors_repo.revoke(session, executor_id)
        finally:
            await engine.dispose()

    if not asyncio.run(_run()):
        typer.echo(f"no such executor: {executor_id}", err=True)
        raise typer.Exit(1)
    typer.echo(f"revoked: {executor_id}")


def api_key_snippet(name: str, key: str, teams: list[str]) -> dict[str, list[dict[str, object]]]:
    return {"keys": [{"name": name, "sha256": sha256_hex(key), "teams": teams}]}


@app.command("generate-api-key")
def generate_api_key_cmd(
    name: Annotated[str, typer.Option(help="Key owner name")],
    teams: Annotated[str, typer.Option(help="Comma-separated team slugs")],
) -> None:
    """Generate a key, print it once, and print the YAML entry (hash only) for the keys file."""
    if not re.fullmatch(NAME_PATTERN, name):
        typer.echo("invalid --name: 1-100 chars of letters, digits, _ . @ -", err=True)
        raise typer.Exit(2)
    team_list = [t.strip() for t in teams.split(",") if t.strip()]
    if not team_list or not all(re.fullmatch(SLUG_PATTERN, t) for t in team_list):
        typer.echo("invalid --teams: comma-separated lowercase slugs required", err=True)
        raise typer.Exit(2)
    key = secrets.token_urlsafe(32)
    typer.echo(f"api key (shown once): {key}")
    typer.echo("keys file snippet (append the entry under `keys:` if the file exists):")
    typer.echo(yaml.safe_dump(api_key_snippet(name, key, team_list), sort_keys=False), nl=False)


if __name__ == "__main__":
    app()
