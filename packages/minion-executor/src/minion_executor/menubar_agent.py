"""`minion-executor menubar install|uninstall|status`: launch the menu-bar app at login (ADR-0011).

A second, independent per-user LaunchAgent. It reuses the helpers in `service` (TCC check, agents
dir vetting, safe plist write, launchctl) but has its own label and plist, and refuses to touch
the executor's. The plist carries no secret and no token: the menu-bar app only reads and writes
the local status.json / control.json.
"""

from __future__ import annotations

import contextlib
import os
import plistlib
from collections.abc import Mapping
from pathlib import Path
from typing import Annotated

import typer

from minion_executor import launchd, service
from minion_executor.launchd import MENUBAR_LABEL, SERVICE_NOT_FOUND, AgentsDirOpt, LabelOpt
from minion_executor.settings import ExecutorSettings

MENUBAR_ENV_ALLOW: tuple[str, ...] = ("MINION_EXECUTOR_STATE_DIR", "MINION_EXECUTOR_CONFIG")
MENUBAR_THROTTLE_S = 30
MENUBAR_STOP_WAIT_S = 15.0
MENUBAR_ARGV1 = "menubar"

menubar_app = typer.Typer(
    help="Launch the menu-bar app at login (per-user LaunchAgent). Without a subcommand: run it.",
    invoke_without_command=True,
)


def render_menubar_plist(
    *, label: str, executable: str, log_path: str, env: dict[str, str] | None = None
) -> bytes:
    """Deterministic plist bytes. Only STATE_DIR / CONFIG may be carried (explicitly set paths)."""
    variables = {k: v for k, v in (env or {}).items() if k in MENUBAR_ENV_ALLOW and v}
    doc: dict[str, object] = {
        "Label": label,
        "ProgramArguments": [executable, "menubar"],
        "RunAtLoad": True,
        "KeepAlive": {"SuccessfulExit": False},
        "ThrottleInterval": MENUBAR_THROTTLE_S,
        "LimitLoadToSessionType": "Aqua",
        "ProcessType": "Interactive",
        "StandardOutPath": log_path,
        "StandardErrorPath": log_path,
        "Umask": 0o077,
    }
    if variables:
        doc["EnvironmentVariables"] = variables
    return plistlib.dumps(doc, fmt=plistlib.FMT_XML, sort_keys=True)


def is_menubar_plist(doc: Mapping[str, object] | None) -> bool:
    """ProgramArguments are exactly `[…/minion-executor, menubar]`: the only job this removes."""
    args = doc.get("ProgramArguments") if doc else None
    return launchd.is_our_program(doc, MENUBAR_ARGV1) and isinstance(args, list) and len(args) == 2


def _check_menubar_label(label: str) -> None:
    launchd.check_label(label)
    if label == service.DEFAULT_LABEL:
        raise ValueError(f"{label} is the executor's label; use `service` for it")


@menubar_app.command()
def install(
    label: LabelOpt = MENUBAR_LABEL,
    dry_run: Annotated[bool, typer.Option(help="Print the plist; change nothing")] = False,
    no_load: Annotated[bool, typer.Option(help="Write the plist, do not bootstrap it")] = False,
    agents_dir: AgentsDirOpt = None,
) -> None:
    """Write ~/Library/LaunchAgents/<label>.plist and load it (GUI session only)."""
    from minion_executor.capacity.safefile import ensure_private_dir
    from minion_executor.main import DEV_ENV

    if not launchd.is_darwin():
        raise launchd.fail("launchd services are macOS only")
    home = Path.home()
    log_path = service.default_log_path(home, label)
    plist_dir = launchd.agents_dir(agents_dir)
    plist_path = plist_dir / f"{label}.plist"
    try:
        launchd.refuse_root()
        _check_menubar_label(label)
        if os.environ.get(DEV_ENV) == "1":
            raise ValueError(f"refusing to install with {DEV_ENV}=1 (dev mode is never a service)")
        settings = ExecutorSettings()
        executable = launchd.executable()
        launchd.check_install_chain(executable)
        items = [
            i for i in service.install_locations(executable, settings) if i[0] != "model cache dir"
        ]
        service.check_not_protected(home, items)
        env: dict[str, str] = {}
        if os.environ.get("MINION_EXECUTOR_STATE_DIR"):
            env["MINION_EXECUTOR_STATE_DIR"] = str(settings.state_dir)
        if os.environ.get("MINION_EXECUTOR_CONFIG") and settings.config_path is not None:
            env["MINION_EXECUTOR_CONFIG"] = str(settings.config_path)
        if not dry_run:
            launchd.check_agents_dir(plist_dir, create=True)
            ensure_private_dir(settings.state_dir)
    except (ValueError, RuntimeError, OSError) as exc:
        raise launchd.fail(str(exc)) from None
    for name in sorted(os.environ):
        if name.startswith("MINION_EXECUTOR_") and name not in MENUBAR_ENV_ALLOW:
            typer.echo(f"warning: {name} is set but not carried into the menu-bar agent", err=True)
    data = render_menubar_plist(label=label, executable=executable, log_path=str(log_path), env=env)
    if dry_run:
        typer.echo(data.decode())
        return
    if plist_path.is_symlink() or plist_path.exists():
        try:
            launchd.check_plist_file(plist_path)
        except (ValueError, OSError) as exc:
            raise launchd.fail(f"{exc}; run `menubar uninstall --label {label}` first") from None
        existing = launchd.read_plist(plist_path)
        if existing is not None and is_menubar_plist(existing) and existing == plistlib.loads(data):
            typer.echo(f"{plist_path} is up to date")
            if not no_load:
                rc, out = launchd.run_launchctl(["print", f"gui/{launchd.uid()}/{label}"])
                if rc == SERVICE_NOT_FOUND:
                    _bootstrap(label, plist_path, remove_on_fail=False)
                elif rc != 0:
                    raise launchd.fail(f"cannot query launchd ({rc}): {out}")
            return
        raise launchd.fail(f"{plist_path} exists; run `menubar uninstall --label {label}` first")
    try:
        service.prepare_log_file(log_path)
        launchd.write_plist_safely(plist_path, data)
    except FileExistsError:
        raise launchd.fail(f"{plist_path} exists; run `menubar uninstall` first") from None
    except (OSError, ValueError) as exc:
        raise launchd.fail(f"cannot write {plist_path}: {exc}") from None
    typer.echo(f"wrote {plist_path}")
    if not no_load:
        _bootstrap(label, plist_path, remove_on_fail=True)


def _bootstrap(label: str, plist_path: Path, *, remove_on_fail: bool) -> None:
    rc, out = launchd.run_launchctl(["bootstrap", f"gui/{launchd.uid()}", str(plist_path)])
    if rc != 0:
        if remove_on_fail:
            with contextlib.suppress(OSError):
                plist_path.unlink()
        raise launchd.fail(f"launchctl bootstrap failed ({rc}): {out}")
    typer.echo(f"loaded {label}")


@menubar_app.command()
def uninstall(label: LabelOpt = MENUBAR_LABEL, agents_dir: AgentsDirOpt = None) -> None:
    """Stop the menu-bar agent and remove its plist. Never touches the executor service."""
    try:
        launchd.refuse_root()
        _check_menubar_label(label)
        plist_dir = launchd.agents_dir(agents_dir)
        launchd.check_agents_dir(plist_dir, create=False)
    except (ValueError, OSError) as exc:
        raise launchd.fail(str(exc)) from None
    launchd.uninstall_agent(
        label=label,
        plist_dir=plist_dir,
        is_ours=is_menubar_plist,
        wait_s=lambda _doc: MENUBAR_STOP_WAIT_S,
        kind="menu-bar agent",
    )


@menubar_app.command("status")
def menubar_status(label: LabelOpt = MENUBAR_LABEL) -> None:
    """launchd state of the menu-bar agent."""
    try:
        _check_menubar_label(label)
    except ValueError as exc:
        raise launchd.fail(str(exc)) from None
    rc, out = launchd.run_launchctl(["print", f"gui/{launchd.uid()}/{label}"])
    if rc == SERVICE_NOT_FOUND:
        typer.echo(f"{label}: not loaded")
    elif rc != 0:
        typer.echo(f"{label}: cannot query launchd ({rc}): {out}", err=True)
    else:
        info = service.parse_print(out)
        typer.echo(
            f"{label}: state={info.get('state', '?')} pid={info.get('pid', '-')} "
            f"last_exit={info.get('last_exit', '-')}"
        )
