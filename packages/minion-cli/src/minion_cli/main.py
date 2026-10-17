"""`minion` CLI: submit, status, fetch, cancel.

Exit codes: 0 ok, 1 task FAILED/CANCELLED, 2 usage/config/auth/4xx, 3 task PARTIAL,
4 timeout or server/S3 unreachable, 130 Ctrl-C (the task is never cancelled by Ctrl-C).
Nothing here prints record or line content.
"""

from __future__ import annotations

import asyncio
import json
import re
from collections.abc import Coroutine
from pathlib import Path
from typing import Annotated, Any, NoReturn

import httpx
import typer
from minion_common.client import ApiError, UserClient
from minion_common.enums import TaskStatus, TaskType
from minion_common.schemas import SLUG_PATTERN, TaskOut
from pydantic import ValidationError

from minion_cli import flows
from minion_cli.errors import (
    EXIT_FAILED,
    EXIT_INTERRUPTED,
    EXIT_OK,
    EXIT_PARTIAL,
    EXIT_USAGE,
    CliError,
)
from minion_cli.flows import Target
from minion_cli.secrets import load_api_key
from minion_cli.settings import CliSettings
from minion_cli.store import ObjectStore, make_store

app = typer.Typer(
    help="open-minions submitter: upload JSONL, submit a task, follow it, fetch results.",
    no_args_is_help=True,
    add_completion=False,
)

ServerOpt = Annotated[str | None, typer.Option(help="Server URL (else $MINION_CLI_SERVER).")]
TeamOpt = Annotated[str | None, typer.Option(help="Team (else $MINION_CLI_TEAM).")]
ProjectOpt = Annotated[str | None, typer.Option(help="Project (else $MINION_CLI_PROJECT).")]
NameArg = Annotated[str, typer.Argument(help="Task name.")]


def make_client(server: str, key: str) -> UserClient:
    """Seam for tests (they patch this with a MockTransport client)."""
    from pydantic import SecretStr

    return UserClient(server, SecretStr(key))


def build_store(settings: CliSettings) -> ObjectStore:
    """Seam for tests (they patch this with an in-memory store)."""
    return make_store(settings)


def exit_code_for(status: TaskStatus) -> int:
    if status in (TaskStatus.FAILED, TaskStatus.CANCELLED):
        return EXIT_FAILED
    if status is TaskStatus.PARTIAL:
        return EXIT_PARTIAL
    return EXIT_OK


def _echo_err(msg: str) -> None:
    typer.echo(msg, err=True)


def _fail(err: CliError) -> NoReturn:
    _echo_err(f"error: {err}")
    raise typer.Exit(err.exit_code)


def _run[T](coro: Coroutine[Any, Any, T], task_name: str | None = None) -> T:
    try:
        return asyncio.run(coro)
    except CliError as exc:
        _fail(exc)
    except KeyboardInterrupt:
        hint = f"; task {task_name!r} keeps running (see `minion status`)" if task_name else ""
        _echo_err(f"interrupted{hint}")
        raise typer.Exit(EXIT_INTERRUPTED) from None
    except (ApiError, httpx.HTTPError) as exc:
        _fail(flows.api_failure(exc))


def _load(server: str | None, team: str | None, project: str | None) -> tuple[CliSettings, str]:
    try:
        settings = CliSettings()
    except (ValidationError, ValueError) as exc:
        # Pydantic error text can embed the offending value; report field names only.
        fields = (
            ", ".join(str(e["loc"][0]) for e in exc.errors())
            if isinstance(exc, ValidationError)
            else ""
        )
        _fail(CliError(f"invalid configuration ({fields or 'see MINION_CLI_* settings'})"))
    try:
        if server:
            from minion_common.client import require_secure_url

            settings.server = require_secure_url(server)
    except ValueError as exc:
        _fail(CliError(str(exc)))
    if team:
        settings.team = team
    if project:
        settings.project = project
    if not settings.server:
        _fail(CliError("no server: pass --server or set MINION_CLI_SERVER"))
    if not settings.team:
        _fail(CliError("no team: pass --team or set MINION_CLI_TEAM"))
    for label, value in (("team", settings.team), ("project", settings.project)):
        if not re.fullmatch(SLUG_PATTERN, value):
            _fail(CliError(f"invalid {label} {value!r}"))
    try:
        key = load_api_key().get_secret_value()
    except CliError as exc:
        _fail(exc)
    return settings, key


def _target(settings: CliSettings, name: str) -> Target:
    if not re.fullmatch(SLUG_PATTERN, name):
        _fail(CliError(f"invalid task name {name!r}"))
    assert settings.team is not None  # noqa: S101 - _load guarantees it
    return Target(settings.team, settings.project, name)


def _summary(task: TaskOut) -> str:
    c = task.counts
    return (
        f"{task.task_name}: {task.status.value} "
        f"(subtasks {c.succeeded}/{c.total} done, {c.running} running, {c.pending} pending, "
        f"{c.failed} failed, {c.cancelled} cancelled)"
    )


def _progress(task: TaskOut) -> None:
    _echo_err(_summary(task))


@app.command()
def submit(
    files: Annotated[list[Path], typer.Argument(help="Input JSONL files, in output order.")],
    type_: Annotated[TaskType, typer.Option("--type", help="embedding or completion.")],
    model: Annotated[str, typer.Option(help="Model id.")],
    output: Annotated[
        Path | None, typer.Option("-o", "--output", help="Result directory (default ./NAME).")
    ] = None,
    name: Annotated[
        str | None, typer.Option(help="Task name (default: file stem + UTC time).")
    ] = None,
    server: ServerOpt = None,
    team: TeamOpt = None,
    project: ProjectOpt = None,
    no_wait: Annotated[bool, typer.Option("--no-wait", help="Return after submitting.")] = False,
    timeout: Annotated[float | None, typer.Option(help="Give up waiting after N seconds.")] = None,
    records_per_subtask: Annotated[int, typer.Option(min=1, max=100_000)] = 500,
    priority: Annotated[int, typer.Option(min=-1000, max=1000)] = 0,
    pool: Annotated[str | None, typer.Option(help="Restrict to an executor pool.")] = None,
    params: Annotated[str | None, typer.Option(help="Task params as a JSON object.")] = None,
) -> None:
    """Upload FILES, submit the task, wait for it and download the results."""
    settings, key = _load(server, team, project)
    task_name = name or flows.default_name(files[0])
    target = _target(settings, task_name)
    parsed: dict[str, Any] = {}
    if params:
        try:
            value = json.loads(params)
        except ValueError:
            _fail(CliError("--params is not valid JSON"))
        if not isinstance(value, dict):
            _fail(CliError("--params must be a JSON object"))
        parsed = value
    out_dir = output or Path(task_name)

    async def go() -> int:
        assert settings.server is not None  # noqa: S101
        store = build_store(settings)
        async with make_client(settings.server, key) as client:
            task = await flows.submit_task(
                client,
                store,
                settings,
                target,
                files,
                type=type_,
                model=model,
                params=parsed,
                records_per_subtask=records_per_subtask,
                priority=priority,
                allowed_pool=pool,
            )
            typer.echo(f"submitted {task.task_name} ({task.status.value})")
            if no_wait:
                return EXIT_OK
            return await _wait_and_fetch(client, store, settings, target, out_dir, timeout)

    raise typer.Exit(_run(go(), task_name))


async def _wait_and_fetch(
    client: UserClient,
    store: ObjectStore,
    settings: CliSettings,
    target: Target,
    out_dir: Path,
    timeout: float | None,  # noqa: ASYNC109 - passed to wait_task, not an asyncio timeout
) -> int:
    task = await flows.wait_task(client, target, timeout_s=timeout, on_progress=_progress)
    return await _fetch_done(client, store, settings, target, task, out_dir)


async def _fetch_done(
    client: UserClient,
    store: ObjectStore,
    settings: CliSettings,
    target: Target,
    task: TaskOut,
    out_dir: Path,
) -> int:
    res = await flows.fetch_results(client, store, settings, target, task, out_dir)
    typer.echo(
        f"{task.status.value}: {res.records} records in {res.parts} parts "
        f"({res.downloaded} downloaded) -> {res.results_path}"
    )
    if res.not_succeeded:
        _echo_err(f"{res.not_succeeded} subtasks did not succeed; see {res.manifest_path}")
    return exit_code_for(task.status)


@app.command()
def status(
    name: NameArg,
    server: ServerOpt = None,
    team: TeamOpt = None,
    project: ProjectOpt = None,
) -> None:
    """Show a task's status and subtask counts (exit code follows the task status)."""
    settings, key = _load(server, team, project)
    target = _target(settings, name)

    async def go() -> int:
        assert settings.server is not None  # noqa: S101
        async with make_client(settings.server, key) as client:
            task = await client.get_task(target.team, target.project, target.name)
        typer.echo(_summary(task))
        if task.error:
            typer.echo(f"error: {task.error}")
        return exit_code_for(task.status)

    raise typer.Exit(_run(go()))


@app.command()
def fetch(
    name: NameArg,
    output: Annotated[Path | None, typer.Option("-o", "--output")] = None,
    wait: Annotated[bool, typer.Option("--wait", help="Wait for a running task first.")] = False,
    timeout: Annotated[float | None, typer.Option(help="Give up waiting after N seconds.")] = None,
    server: ServerOpt = None,
    team: TeamOpt = None,
    project: ProjectOpt = None,
) -> None:
    """Download (or refresh) a finished task's results; already-downloaded parts are skipped."""
    settings, key = _load(server, team, project)
    target = _target(settings, name)

    async def go() -> int:
        assert settings.server is not None  # noqa: S101
        store = build_store(settings)
        async with make_client(settings.server, key) as client:
            if wait:
                task = await flows.wait_task(
                    client, target, timeout_s=timeout, on_progress=_progress
                )
            else:
                task = await client.get_task(target.team, target.project, target.name)
                if task.status not in flows.TERMINAL:
                    raise CliError(f"task {name!r} is {task.status.value}; use --wait", EXIT_USAGE)
            return await _fetch_done(client, store, settings, target, task, output or Path(name))

    raise typer.Exit(_run(go(), name))


@app.command()
def cancel(
    name: NameArg,
    server: ServerOpt = None,
    team: TeamOpt = None,
    project: ProjectOpt = None,
) -> None:
    """Cancel a task (the only way to cancel one; Ctrl-C never does)."""
    settings, key = _load(server, team, project)
    target = _target(settings, name)

    async def go() -> int:
        assert settings.server is not None  # noqa: S101
        async with make_client(settings.server, key) as client:
            task = await client.cancel_task(target.team, target.project, target.name)
        typer.echo(_summary(task))
        return EXIT_OK

    raise typer.Exit(_run(go()))
