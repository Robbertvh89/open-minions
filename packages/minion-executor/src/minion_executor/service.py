"""`minion-executor service`: run the executor as a per-user launchd LaunchAgent (ADR-0011).

The plist is rendered by the pure `render_plist` (deterministic, no I/O). It never carries a
secret: the token stays in the Keychain, and only an allow-list of non-secret
`MINION_EXECUTOR_*` variables is copied from the environment. Nothing here keeps the Mac awake
(invariant 6) and `launchctl` is called with an absolute path, no shell and a timeout.
"""

from __future__ import annotations

import contextlib
import math
import os
import plistlib
import re
import subprocess
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Annotated

import typer

from minion_executor.settings import ExecutorSettings

DEFAULT_LABEL = "com.open-minions.executor"
LAUNCHCTL = "/bin/launchctl"
LAUNCHCTL_TIMEOUT_S = 30.0
EXIT_TIMEOUT_MARGIN_S = 15  # FR-17: shutdown budget is grace + 10 s, plus slack
THROTTLE_INTERVAL_S = 60
DEFAULT_SERVER = "http://localhost:8080"
_LABEL_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")

# Non-secret settings copied from the installing shell. Everything else (TOKEN, DEV, TEST_HOOKS,
# SANDBOX, FETCH_SANDBOX, FAKE_SIGNALS, HF_TOKEN, proxy and CA variables) is dropped on purpose.
ENV_ALLOW_LIST: tuple[str, ...] = (
    "MINION_EXECUTOR_STATE_DIR",
    "MINION_EXECUTOR_CONFIG",
    "MINION_EXECUTOR_MODEL_CACHE_DIR",
    "MINION_EXECUTOR_MODEL_CACHE_GB",
    "MINION_EXECUTOR_HEARTBEAT_INTERVAL_S",
    "MINION_EXECUTOR_IDLE_UNLOAD_S",
)
GRACE_ENV = "MINION_EXECUTOR_STOP_GRACE_S"
EXIT_TIMEOUT_ENV = "MINION_EXECUTOR_LAUNCHD_EXIT_TIMEOUT_S"


def exit_timeout_for(grace_s: float) -> int:
    """launchd `ExitTimeOut` for a stop grace: ceil(grace) + 15 s."""
    return math.ceil(grace_s) + EXIT_TIMEOUT_MARGIN_S


def check_launchd_timeout(settings: ExecutorSettings) -> None:
    """`run` fails fast (ValueError, exit 2) when launchd would SIGKILL it mid-release."""
    t = settings.launchd_exit_timeout_s
    if t is not None and t < settings.stop_grace_s + EXIT_TIMEOUT_MARGIN_S:
        raise ValueError(
            f"{EXIT_TIMEOUT_ENV} ({t:g}s) must be >= {GRACE_ENV} + {EXIT_TIMEOUT_MARGIN_S} s "
            f"({settings.stop_grace_s + EXIT_TIMEOUT_MARGIN_S:g}s); re-run `service install`"
        )


def default_log_path(home: Path, label: str) -> Path:
    name = "executor.log" if label == DEFAULT_LABEL else f"{label}.log"
    return home / "Library" / "Logs" / "open-minions" / name


def render_plist(
    *,
    label: str,
    executable: str,
    server: str,
    log_path: str,
    grace_s: float,
    profile: str | None = None,
    engines: str = "auto",
    env: Mapping[str, str] | None = None,
) -> bytes:
    """Deterministic plist bytes. `env` is filtered through the allow-list."""
    args = [executable, "run", "--server", server]
    if profile is not None:
        args += ["--profile", profile]
    if engines != "auto":
        args += ["--engines", engines]
    variables = {k: v for k, v in (env or {}).items() if k in ENV_ALLOW_LIST and v}
    timeout = exit_timeout_for(grace_s)
    variables[GRACE_ENV] = f"{grace_s:g}"
    variables[EXIT_TIMEOUT_ENV] = str(timeout)
    doc: dict[str, object] = {
        "Label": label,
        "ProgramArguments": args,
        "EnvironmentVariables": variables,
        "ExitTimeOut": timeout,
        "RunAtLoad": True,
        "KeepAlive": {"SuccessfulExit": False},
        "ThrottleInterval": THROTTLE_INTERVAL_S,
        "LimitLoadToSessionType": "Aqua",
        "ProcessType": "Standard",
        "StandardOutPath": log_path,
        "StandardErrorPath": log_path,
    }
    return plistlib.dumps(doc, fmt=plistlib.FMT_XML, sort_keys=True)


def run_launchctl(args: list[str], timeout: float = LAUNCHCTL_TIMEOUT_S) -> tuple[int, str]:
    """(returncode, combined output). OSError/timeout map to (127, message)."""
    try:
        out = subprocess.run(  # noqa: S603 - fixed binary, no shell
            [LAUNCHCTL, *args],
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        return 127, f"launchctl failed: {exc}"
    return out.returncode, (out.stdout + out.stderr).strip()


def parse_print(text: str) -> dict[str, str]:
    """Pick state, pid, last exit code and exit timeout out of `launchctl print` output."""
    wanted = {
        "state": "state",
        "pid": "pid",
        "last exit code": "last_exit",
        "exit timeout": "exit_timeout",
    }
    found: dict[str, str] = {}
    for line in text.splitlines():
        key, sep, value = line.strip().partition(" = ")
        if sep and key in wanted and wanted[key] not in found:
            found[wanted[key]] = value.strip()
    return found


def write_plist_safely(path: Path, data: bytes) -> None:
    """Create `path` 0644, refusing an existing file or symlink (O_EXCL | O_NOFOLLOW)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o644)
    try:
        os.fchmod(fd, 0o644)  # umask-proof
        with os.fdopen(fd, "wb") as f:
            fd = -1
            f.write(data)
    except BaseException:
        if fd >= 0:
            os.close(fd)
        with contextlib.suppress(OSError):
            path.unlink()
        raise


def _uid() -> int:
    return os.getuid()


def _fail(message: str) -> typer.Exit:
    typer.echo(f"error: {message}", err=True)
    return typer.Exit(code=2)


def _executable() -> str:
    """Absolute path of this install's `minion-executor` (symlinks kept, so `uv tool` upgrades
    that repoint the shim keep working)."""
    sibling = Path(sys.executable).parent / "minion-executor"
    if sibling.is_file():
        return str(sibling.absolute())
    raise ValueError("cannot find the minion-executor script next to the Python interpreter")


service_app = typer.Typer(help="Run the executor as a per-user launchd LaunchAgent.")

LabelOpt = Annotated[str, typer.Option(help="launchd label")]
AgentsDirOpt = Annotated[Path | None, typer.Option(hidden=True)]


def _agents_dir(override: Path | None) -> Path:
    return override or Path.home() / "Library" / "LaunchAgents"


def _check_label(label: str) -> None:
    if not _LABEL_RE.fullmatch(label):
        raise ValueError(f"invalid label {label!r}")


@service_app.command()
def install(
    server: Annotated[str, typer.Option(help="Minion server base URL")] = DEFAULT_SERVER,
    profile: Annotated[str | None, typer.Option(help="Pin a capacity profile")] = None,
    engines: Annotated[str, typer.Option(help="auto, or a list such as mlx,ollama")] = "auto",
    label: LabelOpt = DEFAULT_LABEL,
    dry_run: Annotated[bool, typer.Option(help="Print the plist; change nothing")] = False,
    no_load: Annotated[bool, typer.Option(help="Write the plist, do not bootstrap it")] = False,
    agents_dir: AgentsDirOpt = None,
) -> None:
    """Validate like `run`, write the plist and load it. Token must be in the Keychain."""
    from minion_common.client import require_secure_url

    from minion_executor.capacity.config import load_config, resolve_profiles
    from minion_executor.capacity.profiles import parse_profile
    from minion_executor.capacity.safefile import ensure_private_dir
    from minion_executor.capacity.signals import default_source
    from minion_executor.main import (
        DEV_ENV,
        _requested_engines,
        _validate_heartbeat,
    )
    from minion_executor.secrets import ENV_VAR, KEYCHAIN_SERVICE, keychain_token

    try:
        _check_label(label)
        if os.environ.get(DEV_ENV) == "1":
            raise ValueError(f"refusing to install with {DEV_ENV}=1 (dev mode is never a service)")
        require_secure_url(server)
        _requested_engines(engines, None)
        settings = ExecutorSettings()
        hb = os.environ.get("MINION_EXECUTOR_HEARTBEAT_INTERVAL_S")
        _validate_heartbeat(float(hb) if hb else 300.0)
        config = load_config(settings.config_path)
        specs = resolve_profiles(config)
        if profile is not None:
            parse_profile(profile, specs)
        if not dry_run:
            ensure_private_dir(settings.state_dir)
        default_source(None)  # the real signal source must be readable
        executable = _executable()
        if keychain_token() is None:
            hint = f" (${ENV_VAR} is not visible to launchd)" if os.environ.get(ENV_VAR) else ""
            msg = f"no Keychain item {KEYCHAIN_SERVICE!r}{hint}; see docs/ops/executor-install.md"
            if not dry_run:
                raise ValueError(msg)
            typer.echo(f"warning: {msg}", err=True)
    except (ValueError, RuntimeError) as exc:
        raise _fail(str(exc)) from None

    home = Path.home()
    log_path = default_log_path(home, label)
    data = render_plist(
        label=label,
        executable=executable,
        server=server,
        log_path=str(log_path),
        grace_s=settings.stop_grace_s,
        profile=profile,
        engines=engines,
        env=os.environ,
    )
    if dry_run:
        typer.echo(data.decode())
        return
    plist_path = _agents_dir(agents_dir) / f"{label}.plist"
    try:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        write_plist_safely(plist_path, data)
    except FileExistsError:
        raise _fail(f"{plist_path} exists; run `service uninstall --label {label}` first") from None
    except OSError as exc:
        raise _fail(f"cannot write {plist_path}: {exc}") from None
    typer.echo(f"wrote {plist_path}")
    if no_load:
        return
    rc, out = run_launchctl(["bootstrap", f"gui/{_uid()}", str(plist_path)])
    if rc != 0:
        with contextlib.suppress(OSError):
            plist_path.unlink()
        raise _fail(f"launchctl bootstrap failed ({rc}): {out}")
    typer.echo(f"loaded {label}")


@service_app.command()
def uninstall(label: LabelOpt = DEFAULT_LABEL, agents_dir: AgentsDirOpt = None) -> None:
    """Stop the service (SIGTERM, graceful) and remove the plist."""
    try:
        _check_label(label)
    except ValueError as exc:
        raise _fail(str(exc)) from None
    plist_path = _agents_dir(agents_dir) / f"{label}.plist"
    rc, out = run_launchctl(["bootout", f"gui/{_uid()}/{label}"], timeout=LAUNCHCTL_TIMEOUT_S + 330)
    # 3 = not loaded (ESRCH), 113 = could not find service: fine, we still remove the plist
    if rc not in (0, 3, 113):
        raise _fail(f"launchctl bootout failed ({rc}): {out}; plist left in place")
    try:
        plist_path.unlink()
        typer.echo(f"removed {plist_path}")
    except FileNotFoundError:
        typer.echo(f"{plist_path} not present")
    except OSError as exc:
        raise _fail(f"cannot remove {plist_path}: {exc}") from None


@service_app.command("status")
def service_status(label: LabelOpt = DEFAULT_LABEL) -> None:
    """launchd state of the service, then the executor's own status."""
    from minion_executor.main import status as executor_status

    try:
        _check_label(label)
        settings = ExecutorSettings()
    except ValueError as exc:
        raise _fail(str(exc)) from None
    rc, out = run_launchctl(["print", f"gui/{_uid()}/{label}"])
    if rc != 0:
        typer.echo(f"{label}: not loaded")
    else:
        info = parse_print(out)
        typer.echo(
            f"{label}: state={info.get('state', '?')} pid={info.get('pid', '-')} "
            f"last_exit={info.get('last_exit', '-')} exit_timeout={info.get('exit_timeout', '?')}"
        )
        need = exit_timeout_for(settings.stop_grace_s)
        raw = info.get("exit_timeout", "")
        try:
            have = float(raw.split()[0])
        except (ValueError, IndexError):
            typer.echo("warning: exit timeout not reported by launchd", err=True)
        else:
            if have < need:
                typer.echo(
                    f"warning: exit timeout {have:g}s < grace + 15 = {need}s; launchd may SIGKILL "
                    "a stopping executor. Re-run `service uninstall` and `service install`.",
                    err=True,
                )
    executor_status()
