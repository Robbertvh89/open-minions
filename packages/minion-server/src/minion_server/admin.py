"""Operator CLI: `generate-api-key | issue/revoke/rotate-executor-token | list-executors`."""

import asyncio
import http.client
import re
import secrets
import uuid
from datetime import UTC, datetime, timedelta
from typing import Annotated
from urllib.parse import urlsplit

import typer
import yaml
from minion_common.schemas import SLUG_PATTERN
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from minion_server.auth import MAX_TEAMS_PER_KEY, sha256_hex
from minion_server.db.session import create_engine, create_sessionmaker
from minion_server.repositories import executors as executors_repo
from minion_server.settings import ServerSettings

MAX_TTL_DAYS = 365
NAME_PATTERN = r"[\w.@-]{1,100}"


def printable(value: str) -> str:
    """Escape control characters (ANSI/OSC, tabs, newlines) so output cannot spoof the terminal."""
    return "".join(
        c if c.isprintable() and c not in "\\" else c.encode("unicode_escape").decode("ascii")
        for c in value
    )


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
    if not re.fullmatch(SLUG_PATTERN, pool):
        typer.echo("invalid --pool: lowercase slug required", err=True)
        raise typer.Exit(2)
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


def _ttl_note() -> str:
    """The revocation bound of this deployment's settings (default text if they do not load)."""
    try:
        ttl = ServerSettings().executor_auth_cache_ttl_s
    except Exception:
        ttl = 10.0
    when = "at once" if ttl == 0 else f"within {ttl:g} s"
    return (
        f"(the old token stops working {when}: MINION_EXECUTOR_AUTH_CACHE_TTL_S, "
        "0 = immediate; per server worker)"
    )


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
    typer.echo(_ttl_note())


@app.command("rotate-executor-token")
def rotate_executor_token_cmd(
    executor_id: Annotated[uuid.UUID, typer.Option(help="Executor to re-key")],
    ttl_days: Annotated[
        int | None, typer.Option(min=1, max=MAX_TTL_DAYS, help="New token lifetime in days")
    ] = None,
) -> None:
    """Issue a new token for the same executor id; the old one stops working within the TTL."""
    settings = ServerSettings()
    days = min(ttl_days or settings.executor_token_ttl_days, MAX_TTL_DAYS)

    async def _run() -> str | None:
        engine = create_engine(settings.database_url)
        try:
            async with create_sessionmaker(engine)() as session, session.begin():
                return await rotate_executor_token(session, executor_id, ttl_days=days)
        finally:
            await engine.dispose()

    token = asyncio.run(_run())
    if token is None:
        typer.echo(f"no such (or revoked) executor: {executor_id}", err=True)
        raise typer.Exit(1)
    typer.echo(f"executor_id: {executor_id}")
    typer.echo(f"token (shown once): {token}")
    typer.echo(_ttl_note())


async def rotate_executor_token(
    session: AsyncSession, executor_id: uuid.UUID, *, ttl_days: int
) -> str | None:
    token = secrets.token_urlsafe(32)
    ok = await executors_repo.rotate_token(
        session,
        executor_id,
        token_hash=sha256_hex(token),
        token_expires_at=datetime.now(UTC) + timedelta(days=ttl_days),
    )
    return token if ok else None


@app.command("list-executors")
def list_executors_cmd() -> None:
    """List executors (id, owner, pool, status, expiry). Never prints token hashes."""
    settings = ServerSettings()

    async def _run() -> list[executors_repo.ExecutorSummary]:
        engine = create_engine(settings.database_url)
        try:
            async with create_sessionmaker(engine)() as session, session.begin():
                return await executors_repo.list_all(session)
        finally:
            await engine.dispose()

    def ts(v: datetime | None) -> str:
        return v.isoformat(timespec="seconds") if v else "-"

    header = ["executor_id", "owner", "pool", "status", "hostname", "last_seen", "token_expires"]
    typer.echo("\t".join(header))
    for e in asyncio.run(_run()):
        typer.echo(
            "\t".join(
                [
                    str(e.executor_id),
                    printable(e.owner),
                    printable(e.pool),
                    e.status.value,
                    printable(e.hostname),
                    f"last_seen={ts(e.last_seen_at)}",
                    f"token_expires={ts(e.token_expires_at)}",
                ]
            )
        )


def api_key_snippet(
    name: str,
    key: str,
    teams: list[str],
    *,
    operator: bool = False,
    expires_at: datetime | None = None,
) -> dict[str, list[dict[str, object]]]:
    entry: dict[str, object] = {"name": name, "sha256": sha256_hex(key), "teams": teams}
    if operator:
        entry["operator"] = True
    if expires_at is not None:
        entry["expires_at"] = expires_at.isoformat(timespec="seconds")
    return {"keys": [entry]}


@app.command("generate-api-key")
def generate_api_key_cmd(
    name: Annotated[str, typer.Option(help="Key owner name")],
    teams: Annotated[str, typer.Option(help="Comma-separated team slugs")],
    operator: Annotated[bool, typer.Option("--operator", help="Grant the operator role")] = False,
    expires_days: Annotated[
        int | None, typer.Option(min=1, max=3650, help="Key expires after N days")
    ] = None,
) -> None:
    """Generate a key, print it once, and print the YAML entry (hash only) for the keys file."""
    if not re.fullmatch(NAME_PATTERN, name):
        typer.echo("invalid --name: 1-100 chars of letters, digits, _ . @ -", err=True)
        raise typer.Exit(2)
    team_list = [t.strip() for t in teams.split(",") if t.strip()]
    if not team_list or not all(re.fullmatch(SLUG_PATTERN, t) for t in team_list):
        typer.echo("invalid --teams: comma-separated lowercase slugs required", err=True)
        raise typer.Exit(2)
    if len(team_list) > MAX_TEAMS_PER_KEY:
        typer.echo(f"invalid --teams: at most {MAX_TEAMS_PER_KEY} teams per key", err=True)
        raise typer.Exit(2)
    key = secrets.token_urlsafe(32)
    typer.echo(f"api key (shown once): {key}")
    typer.echo("keys file snippet (append the entry under `keys:` if the file exists):")
    expires = datetime.now(UTC) + timedelta(days=expires_days) if expires_days else None
    snippet = api_key_snippet(name, key, team_list, operator=operator, expires_at=expires)
    typer.echo(yaml.safe_dump(snippet, sort_keys=False), nl=False)


@app.command("generate-ui-secret")
def generate_ui_secret_cmd() -> None:
    """Print a random MINION_UI_SESSION_SECRET (UI cookie key); rotating it logs everyone out."""
    typer.echo(f"MINION_UI_SESSION_SECRET={secrets.token_urlsafe(48)}")


@app.command("check-storage")
def check_storage_cmd() -> None:
    """Preflight: verify the S3 backend enforces the signed Range on input GET URLs (SPEC S-2).

    Run against the production bucket before going live. Exit 0 = enforcing, 1 = not enforcing
    (observed `(status, body length)` per check is printed), 2 = config/connectivity error.
    Needs the server settings (MINION_DATABASE_URL is required by ServerSettings even though no
    DB access happens, plus MINION_S3_*). Run it from a network position like the executors'.
    """
    from botocore.exceptions import BotoCoreError, ClientError

    from minion_server.storage import S3Storage
    from minion_server.storage_check import run_check

    try:
        settings = ServerSettings()
        storage = S3Storage(settings)
    except ValidationError as exc:  # field locations only: values may be secrets
        locs = ", ".join(sorted({".".join(str(p) for p in e["loc"]) for e in exc.errors()}))
        typer.echo(f"configuration error in: {locs}", err=True)
        raise typer.Exit(2) from None
    except Exception as exc:
        typer.echo(f"configuration error: {type(exc).__name__}", err=True)
        raise typer.Exit(2) from None
    host = urlsplit(settings.public_s3_endpoint_url).netloc
    typer.echo(f"checking bucket {settings.s3_bucket!r} through endpoint host {host}")
    try:
        checks = asyncio.run(
            run_check(storage, warn=lambda m: typer.echo(f"WARNING: {m}", err=True))
        )
    except (OSError, BotoCoreError, ClientError, http.client.HTTPException) as exc:
        typer.echo(f"storage unreachable or refused: {type(exc).__name__}", err=True)
        raise typer.Exit(2) from None
    for c in checks:
        typer.echo(
            f"{'ok  ' if c.ok else 'FAIL'} {c.name}: observed (status, length) = "
            f"{c.observed}, expected {c.expected}"
        )
    if not all(c.ok for c in checks):
        typer.echo(
            "FAIL: the backend does not enforce the signed Range; input GET URLs are not "
            "slice-scoped here (SPEC S-2). Do not use it for confidential tasks.",
            err=True,
        )
        raise typer.Exit(1)
    typer.echo("ok: signed Range enforced")


if __name__ == "__main__":
    app()
