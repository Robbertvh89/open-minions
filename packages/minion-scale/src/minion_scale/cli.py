"""`minion-scale`: seed / run / explain / verify / cleanup / report (DEV ONLY).

Database and S3 settings come from the `MINION_*` environment variables only (no `.env` file is
read, so a developer's regular settings are never picked up by accident); the S3 endpoint has no
default. Commands that write refuse a non-loopback database, or one holding tasks that this
harness did not create, unless `--yes-this-is-a-scale-db` is given. Tokens live in a 0600 file
written by `seed --tokens-file`; nothing here prints them.
"""

import asyncio
from pathlib import Path
from typing import Annotated, Any

import boto3
import typer
from botocore.config import Config as BotoConfig
from minion_common.client import require_secure_url
from minion_server.db.session import create_engine, create_sessionmaker
from minion_server.models_registry import load_models
from pydantic import SecretStr, ValidationError
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError

from minion_scale import dbstats, report
from minion_scale.explain import explain_all, hot_statements
from minion_scale.seed import (
    CREATED_BY,
    DEFAULT_MODEL,
    DEFAULT_POOL,
    DEFAULT_PREFIX,
    DEFAULT_PROJECT,
    DEFAULT_TEAM,
    RECORDS_PER_SUBTASK,
    SeedError,
    SeedSpec,
    count_remaining,
    read_tokens_file,
    revoke_executors,
    seed_executors,
    seed_queue,
    upload_input,
    write_tokens_file,
)
from minion_scale.sim import (
    ACQUIRE_PACE_S,
    EXECUTOR_BACKOFF,
    FaultRates,
    SimConfig,
    run_fleet,
)
from minion_scale.verify import verify

app = typer.Typer(help="open-minions scale harness (DEV ONLY)", no_args_is_help=True)

DEFAULT_MODELS_FILE = Path("deploy/scale/models.scale.yaml")


class ScaleSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="MINION_", env_file=None, extra="ignore")

    database_url: SecretStr
    s3_endpoint_url: str | None = None  # no default: never guess which object store to write to
    s3_access_key: SecretStr | None = None
    s3_secret_key: SecretStr | None = None
    s3_bucket: str = "minion"
    s3_region: str = "us-east-1"


def _settings() -> ScaleSettings:
    try:
        return ScaleSettings()
    except ValidationError as exc:
        missing = ", ".join(f"MINION_{str(e['loc'][0]).upper()}" for e in exc.errors())
        raise _fail(f"invalid or missing settings: {missing}") from None


def _s3(cfg: ScaleSettings) -> Any:
    if cfg.s3_endpoint_url is None or cfg.s3_access_key is None or cfg.s3_secret_key is None:
        return None
    return boto3.client(
        "s3",
        endpoint_url=cfg.s3_endpoint_url,
        aws_access_key_id=cfg.s3_access_key.get_secret_value(),
        aws_secret_access_key=cfg.s3_secret_key.get_secret_value(),
        region_name=cfg.s3_region,
        config=BotoConfig(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def _sessionmaker(cfg: ScaleSettings) -> tuple[Any, Any]:
    engine = create_engine(cfg.database_url, pool_size=5, max_overflow=5)
    return engine, create_sessionmaker(engine)


def _fail(msg: str) -> typer.Exit:
    typer.echo(msg, err=True)
    return typer.Exit(2)


LOOPBACK_HOSTS = frozenset({"localhost", "127.0.0.1", "::1"})
YesScaleDb = Annotated[
    bool,
    typer.Option(
        "--yes-this-is-a-scale-db",
        help="Allow a non-loopback database or one that holds tasks not created by minion-scale",
    ),
]


def check_scale_host(database_url: str, yes: bool) -> None:
    """Refuse a database that is not on this machine unless the operator said it is a scale DB.
    The message names the host only, never the credentials."""
    host = make_url(database_url).host
    if host is not None and host.lower() not in LOOPBACK_HOSTS and not yes:
        raise _fail(
            f"refusing database host {host!r}: not loopback; pass --yes-this-is-a-scale-db "
            "if it is a throwaway scale-test database"
        )


async def check_scale_db(sm: Any, yes: bool) -> None:
    """Refuse a database that holds tasks this harness did not create (a real queue)."""
    if yes:
        return
    try:
        async with sm() as s:
            foreign = int(
                (
                    await s.execute(
                        text("SELECT count(*) FROM minion_task WHERE created_by <> :me"),
                        {"me": CREATED_BY},
                    )
                ).scalar_one()
            )
    except DBAPIError:
        return  # not migrated: the command itself fails with its own error
    if foreign:
        raise _fail(
            f"refusing: the database holds {foreign} task(s) not created by {CREATED_BY}; "
            "pass --yes-this-is-a-scale-db if this really is a scale-test database"
        )


@app.command("seed")
def seed_cmd(
    tasks: Annotated[int, typer.Option(min=1, help="Tasks to create")] = 10,
    subtasks: Annotated[int, typer.Option(min=1, help="PENDING subtasks per task")] = 100,
    executors: Annotated[int, typer.Option(min=0, help="Executor tokens to issue")] = 0,
    tokens_file: Annotated[
        Path | None, typer.Option(help="New 0600 file for the issued tokens (never printed)")
    ] = None,
    models_file: Annotated[Path, typer.Option(help="Model allow-list")] = DEFAULT_MODELS_FILE,
    model: Annotated[str, typer.Option(help="Model id from the file")] = DEFAULT_MODEL,
    team: str = DEFAULT_TEAM,
    project: str = DEFAULT_PROJECT,
    prefix: Annotated[str, typer.Option(help="Task name prefix")] = DEFAULT_PREFIX,
    records: int = RECORDS_PER_SUBTASK,
    max_attempts: int = 5,
    yes_scale_db: YesScaleDb = False,
) -> None:
    """Bulk-insert tasks x PENDING subtasks and (optionally) executor tokens."""
    if executors and tokens_file is None:
        raise _fail("--tokens-file is required with --executors (tokens are never printed)")
    cfg = _settings()
    check_scale_host(cfg.database_url.get_secret_value(), yes_scale_db)
    s3 = _s3(cfg)
    if s3 is None:
        raise _fail(
            "MINION_S3_ENDPOINT_URL, MINION_S3_ACCESS_KEY and MINION_S3_SECRET_KEY are required "
            "to upload the input"
        )
    spec = SeedSpec(
        tasks=tasks,
        subtasks=subtasks,
        models_file=models_file,
        bucket=cfg.s3_bucket,
        team=team,
        project=project,
        prefix=prefix,
        model_id=model,
        records=records,
        max_attempts=max_attempts,
    )

    async def go() -> None:
        engine, sm = _sessionmaker(cfg)
        try:
            await check_scale_db(sm, yes_scale_db)
            size = await asyncio.to_thread(upload_input, s3, spec)
            res = await seed_queue(sm, spec, input_bytes=size)
            typer.echo(f"seeded {res.tasks} tasks, {res.subtasks} PENDING subtasks")
            if executors:
                creds = await seed_executors(sm, executors, pool=DEFAULT_POOL)
                assert tokens_file is not None  # noqa: S101 - checked above
                write_tokens_file(tokens_file, creds)
                typer.echo(f"issued {len(creds)} executor tokens -> {tokens_file}")
        finally:
            await engine.dispose()

    try:
        asyncio.run(go())
    except (SeedError, FileExistsError, OSError) as exc:
        raise _fail(f"seed failed: {type(exc).__name__}: {exc}") from exc


@app.command("run")
def run_cmd(
    server: Annotated[str, typer.Option(help="Server base URL (https or loopback http)")],
    tokens_file: Annotated[Path, typer.Option(help="File written by `seed`")],
    sims: Annotated[
        int | None, typer.Option(min=1, help="Simulated executors (default: all)")
    ] = None,
    procs: Annotated[int, typer.Option(min=1, help="Processes to spread the sims over")] = 1,
    work_s: Annotated[float, typer.Option(min=0, help="Simulated work per subtask")] = 1.0,
    heartbeat_interval_s: Annotated[float, typer.Option(min=0.05)] = 5.0,
    backoff_lo: float = EXECUTOR_BACKOFF[0],
    backoff_hi: float = EXECUTOR_BACKOFF[1],
    acquire_pace_s: float = ACQUIRE_PACE_S,
    vanish: Annotated[float, typer.Option(min=0, max=1, help="P(stop beating, walk away)")] = 0.0,
    release: Annotated[float, typer.Option(min=0, max=1)] = 0.0,
    fail: Annotated[float, typer.Option(min=0, max=1, help="P(retryable fail)")] = 0.0,
    late: Annotated[float, typer.Option(min=0, max=1, help="P(complete after reclaim)")] = 0.0,
    late_wait_s: Annotated[
        float, typer.Option(help="Silence before a late complete; > heartbeat timeout + a tick")
    ] = 30.0,
    team: Annotated[
        str, typer.Option(help="Stop when this team has no PENDING/RUNNING left")
    ] = DEFAULT_TEAM,
    max_duration_s: Annotated[float | None, typer.Option(help="Hard stop")] = None,
    poll_s: float = 1.0,
    seed: int = 0,
    docker_project: Annotated[
        str, typer.Option(help="Compose project to read `docker stats` from (default: none)")
    ] = "",
    out: Annotated[Path | None, typer.Option(help="Write the JSON report here")] = None,
    yes_scale_db: YesScaleDb = False,
) -> None:
    """Run simulated executors until the queue is drained (or --max-duration-s).

    Exit 1 when the queue did not drain (time ran out, every sim ended, a sim process crashed)
    or the server answered 5xx; exit 2 on bad input.
    """
    cfg = _settings()
    check_scale_host(cfg.database_url.get_secret_value(), yes_scale_db)
    try:
        require_secure_url(server)
        creds = read_tokens_file(tokens_file)
        faults = FaultRates(vanish=vanish, release=release, fail=fail, late=late)
    except (SeedError, ValueError, OSError) as exc:
        raise _fail(f"cannot start: {type(exc).__name__}: {exc}") from exc
    tokens = [c.token for c in creds][: sims or len(creds)]
    sim_cfg = SimConfig(
        base_url=server,
        work_s=work_s,
        heartbeat_interval_s=heartbeat_interval_s,
        backoff=(backoff_lo, backoff_hi),
        acquire_pace_s=acquire_pace_s,
        faults=faults,
        late_wait_s=late_wait_s,
        seed=seed,
    )

    async def go() -> dict[str, Any]:
        engine, sm = _sessionmaker(cfg)
        try:
            await check_scale_db(sm, yes_scale_db)
            await dbstats.ensure_extension(sm)
            before = await dbstats.snapshot(sm)

            async def drained() -> bool:
                return await count_remaining(sm, team=team) == 0

            rec, secs, did_drain = await run_fleet(
                sim_cfg,
                tokens,
                procs=procs,
                until=drained,
                poll_s=poll_s,
                max_duration_s=max_duration_s,
            )
            after = await dbstats.snapshot(sm)
            docker = await dbstats.docker_stats_async(docker_project)
        finally:
            await engine.dispose()
        return report.new_report(
            config={
                "server": report.server_label(server),
                "sims": len(tokens),
                "procs": procs,
                "work_s": work_s,
                "heartbeat_interval_s": heartbeat_interval_s,
                "backoff_s": [backoff_lo, backoff_hi],
                "acquire_pace_s": acquire_pace_s,
                "fault_rates": {"vanish": vanish, "release": release, "fail": fail, "late": late},
                "late_wait_s": late_wait_s,
                "seed": seed,
                "team": team,
            },
            run=report.run_section(
                rec, duration_s=secs, sims=len(tokens), procs=procs, drained=did_drain
            ),
            db=dbstats.db_report(before, after),
            docker=docker,
        )

    try:
        doc = asyncio.run(go())
    except ValueError as exc:  # e.g. an insecure server URL the sims refused
        raise _fail(f"run failed: {type(exc).__name__}: {exc}") from exc
    _emit(doc, out)
    if not doc["run"]["drained"]:
        typer.echo("the queue did not drain (see run.events: sim_aborted / proc_crashed)", err=True)
    if doc["run"]["server_errors_5xx"] or not doc["run"]["drained"]:
        raise typer.Exit(1)


@app.command("explain")
def explain_cmd(
    models_file: Annotated[Path, typer.Option()] = DEFAULT_MODELS_FILE,
    team: str = DEFAULT_TEAM,
    heartbeat_timeout_s: Annotated[float, typer.Option(help="Watchdog timeout in use")] = 900.0,
    max_lease_s: float = 6 * 3600.0,
    out: Annotated[Path | None, typer.Option(help="Write the JSON report here")] = None,
    yes_scale_db: YesScaleDb = False,
) -> None:
    """EXPLAIN (ANALYZE, BUFFERS) the candidate, stale-heartbeat and over-lease queries."""
    cfg = _settings()
    check_scale_host(cfg.database_url.get_secret_value(), yes_scale_db)
    stmts = hot_statements(
        model_ids=list(load_models(models_file)),
        team=team,
        heartbeat_timeout_s=heartbeat_timeout_s,
        max_lease_s=max_lease_s,
        pool=DEFAULT_POOL,
    )

    async def go() -> list[dict[str, Any]]:
        engine, sm = _sessionmaker(cfg)
        try:
            await check_scale_db(sm, yes_scale_db)
            async with sm() as s:
                await s.execute(text("ANALYZE"))
                await s.commit()
            return await explain_all(sm, stmts)
        finally:
            await engine.dispose()

    _emit(report.new_report(explain=asyncio.run(go())), out)


@app.command("verify")
def verify_cmd(
    team: str = DEFAULT_TEAM,
    out: Annotated[Path | None, typer.Option(help="Write the JSON report here")] = None,
) -> None:
    """Check NF-2 / NF-3 and the task counters after a drained run; exit 1 on a violation."""
    cfg = _settings()

    async def go() -> dict[str, Any]:
        engine, sm = _sessionmaker(cfg)
        try:
            rep = await verify(sm, team=team, s3=_s3(cfg), bucket=cfg.s3_bucket)
        finally:
            await engine.dispose()
        return rep.to_dict()

    result = asyncio.run(go())
    _emit(report.new_report(verify=result), out)
    if not result["ok"]:
        raise typer.Exit(1)


@app.command("cleanup")
def cleanup_cmd(
    tokens_file: Annotated[Path, typer.Option(help="File written by `seed`")],
    yes_scale_db: YesScaleDb = False,
) -> None:
    """Revoke the executor tokens `seed` issued and delete the tokens file."""
    cfg = _settings()
    check_scale_host(cfg.database_url.get_secret_value(), yes_scale_db)
    try:
        creds = read_tokens_file(tokens_file)
    except (SeedError, ValueError, OSError) as exc:
        raise _fail(f"cannot clean up: {type(exc).__name__}: {exc}") from exc

    async def go() -> int:
        engine, sm = _sessionmaker(cfg)
        try:
            await check_scale_db(sm, yes_scale_db)
            return await revoke_executors(sm, [c.executor_id for c in creds])
        finally:
            await engine.dispose()

    revoked = asyncio.run(go())
    tokens_file.unlink()
    typer.echo(f"revoked {revoked} executor token(s), deleted {tokens_file}")


@app.command("report")
def report_cmd(
    inputs: Annotated[list[Path], typer.Argument(help="Report files from run/explain/verify")],
    out: Annotated[Path | None, typer.Option(help="Write the merged JSON report here")] = None,
) -> None:
    """Merge partial reports into one and print a summary."""
    try:
        merged = report.merge([report.read_json(p) for p in inputs])
    except (ValueError, OSError) as exc:
        raise _fail(f"cannot merge: {type(exc).__name__}: {exc}") from exc
    _emit(merged, out)


def _emit(doc: dict[str, Any], out: Path | None) -> None:
    if out is not None:
        report.write_json(out, doc)
        typer.echo(f"report written to {out}")
    for line in report.summary_lines(doc):
        typer.echo(line)


if __name__ == "__main__":
    app()
