"""`minion-executor service`: run the executor as a per-user launchd LaunchAgent (ADR-0011).

The plist is rendered by the pure `render_plist` (deterministic, no I/O). It never carries a
secret: the token stays in the Keychain, and only an allow-list of non-secret
`MINION_EXECUTOR_*` variables is copied from the environment. Nothing here keeps the Mac awake
(invariant 6) and `launchctl` is called with an absolute path, no shell and a timeout.
"""

from __future__ import annotations

import contextlib
import json
import math
import os
import plistlib
import re
import shlex
import stat
import subprocess
import sys
import time
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Annotated
from urllib.parse import urlsplit

import typer
from minion_common.enums import Engine

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
    "MINION_EXECUTOR_MIN_FREE_DISK_GB",
    "MINION_EXECUTOR_DOWNLOAD_TIMEOUT_S",
)
GRACE_ENV = "MINION_EXECUTOR_STOP_GRACE_S"
EXIT_TIMEOUT_ENV = "MINION_EXECUTOR_LAUNCHD_EXIT_TIMEOUT_S"
LABEL_ENV = "MINION_EXECUTOR_SERVICE_LABEL"
SERVER_AUTH_CODES = frozenset({"UNAUTHORIZED", "FORBIDDEN"})  # the server's own 401/403 codes
FAILURE_FILE = "service_failure.json"
FAILURE_MAX_BYTES = 4096
BOOTOUT_IN_PROGRESS = 36  # EINPROGRESS: a graceful stop is under way
SERVICE_NOT_FOUND = 113
STOP_POLL_S = 1.0
STOP_WAIT_SLACK_S = 10.0
_now: Callable[[], float] = time.monotonic
_sleep: Callable[[float], None] = time.sleep


def service_mode() -> bool:
    """True when running under the installed LaunchAgent: the plist exports the exit timeout and
    the label, and launchd sets XPC_SERVICE_NAME to that label (or we are its direct child, ppid 1).
    A shell that merely exports the timeout is not service mode."""
    if not os.environ.get(EXIT_TIMEOUT_ENV, "").strip():
        return False
    label = os.environ.get(LABEL_ENV, "")
    return (bool(label) and os.environ.get("XPC_SERVICE_NAME") == label) or os.getppid() == 1


def check_server_url(url: str) -> None:
    """Reject userinfo, query and fragment (they would land in the plist, `ps` and logs)."""
    parts = urlsplit(url)
    if parts.username is not None or parts.password is not None or "@" in parts.netloc:
        raise ValueError("server URL must not contain credentials (user:pass@)")
    if parts.query or parts.fragment:
        raise ValueError("server URL must not contain a query or fragment")


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


def sanitize(text: str, limit: int = 200) -> str:
    """Printable ASCII only, truncated: safe to echo or store (server text is untrusted)."""
    clean = "".join(c if " " <= c <= "~" else "?" for c in text)
    return clean if len(clean) <= limit else clean[: limit - 3] + "..."


def record_failure(state_dir: Path, reason: str, detail: str, ts: str) -> None:
    """Persist the last permanent failure (0600, in the state dir) for `service status`."""
    from minion_executor.capacity.safefile import atomic_write_private

    doc = {"ts": sanitize(ts, 32), "reason": sanitize(reason, 64), "detail": sanitize(detail, 300)}
    atomic_write_private(state_dir / FAILURE_FILE, json.dumps(doc).encode())


def read_failure(state_dir: Path) -> dict[str, str] | None:
    """The recorded permanent failure, or None (absent, unsafe, malformed)."""
    from minion_executor.capacity.safefile import SafeFileError, read_private_file

    try:
        raw = read_private_file(state_dir / FAILURE_FILE, "failure record", FAILURE_MAX_BYTES)
        doc = json.loads(raw) if raw else None
    except (SafeFileError, ValueError):
        return None
    if not isinstance(doc, dict):
        return None
    out = {k: doc.get(k) for k in ("ts", "reason", "detail")}
    if not all(isinstance(v, str) for v in out.values()):
        return None
    return {k: sanitize(str(v), 300) for k, v in out.items()}


def clear_failure(state_dir: Path) -> None:
    """The service started again: the old failure no longer applies."""
    with contextlib.suppress(OSError):
        (state_dir / FAILURE_FILE).unlink()


# macOS TCC: a LaunchAgent is not allowed to read these without a Files & Folders grant, so Python
# blocks forever in open() on the first file there. The service must not reference them at all.
TCC_PROTECTED: tuple[str, ...] = (
    "Documents",
    "Desktop",
    "Downloads",
    "Library/Mobile Documents",
    "Library/CloudStorage",
)
_PTH_MAX_BYTES = 65536


def tcc_roots(home: Path) -> list[Path]:
    """The protected folders under `home`, as written and with symlinks resolved."""
    roots: list[Path] = []
    for base in {home, home.resolve()}:
        for rel in TCC_PROTECTED:
            roots.append(base / rel)
            with contextlib.suppress(OSError, RuntimeError):
                roots.append((base / rel).resolve())
    return list(dict.fromkeys(roots))


def _identity(path: Path) -> tuple[int, int] | None:
    try:
        st = path.stat()
    except (OSError, RuntimeError):
        return None
    return (st.st_dev, st.st_ino)


def is_tcc_protected(path: Path | str, roots: list[Path]) -> bool:
    """True when `path` is, or lies under, a protected root. Compared as written, symlink-resolved,
    case-folded (APFS is case-insensitive) and by (st_dev, st_ino) of every existing ancestor
    (firmlinks such as /System/Volumes/Data/Users/<u>)."""
    raw = Path(os.path.abspath(path))  # noqa: PTH100 - normalise ".." without symlinks
    cands = [raw]
    with contextlib.suppress(OSError, RuntimeError):
        cands.append(raw.resolve())
    if any(c == r or r in c.parents for c in cands for r in roots):
        return True
    folded = [Path(str(r).casefold()) for r in roots]
    for c in cands:
        fc = Path(str(c).casefold())
        if any(fc == r or r in fc.parents for r in folded):
            return True
    root_ids = {i for r in roots if (i := _identity(r)) is not None}
    if not root_ids:
        return False
    return any(
        (i := _identity(a)) is not None and i in root_ids
        for c in cands
        for a in (*reversed(c.parents), c)  # outermost first: never stat inside a protected dir
    )


def pth_targets(directory: Path) -> list[Path]:
    """Path lines of the `*.pth` files in `directory` (what `site` would add to sys.path)."""
    found: list[Path] = []
    try:
        files = sorted(directory.glob("*.pth"))
    except OSError:
        return found
    for f in files:
        try:
            with f.open("rb") as fh:
                text = fh.read(_PTH_MAX_BYTES).decode("utf-8", "replace")
        except OSError:
            continue
        for line in text.splitlines():
            line = line.strip()
            if line and not line.startswith(("#", "import ", "import\t")):
                found.append(directory / line)
    return found


def install_locations(
    executable: str, settings: ExecutorSettings, search_path: list[str] | None = None
) -> list[tuple[str, Path | str]]:
    """Everything the installed service would read at start-up: (what, path)."""
    items: list[tuple[str, Path | str]] = [
        ("executable", executable),
        ("python", sys.executable),
        ("sys.prefix", sys.prefix),
        ("sys.base_prefix", sys.base_prefix),
        ("state dir", settings.state_dir),
        ("model cache dir", settings.model_cache_dir),
    ]
    if settings.config_path is not None:
        items.append(("config file", settings.config_path))
    for entry in sys.path if search_path is None else search_path:
        if not entry:
            continue
        items.append(("sys.path entry", entry))
        d = Path(entry)
        if d.is_dir():
            items += [(f".pth target in {d}", t) for t in pth_targets(d)]
    return items


def check_not_protected(home: Path, items: list[tuple[str, Path | str]]) -> None:
    """ValueError naming each location under a TCC-protected folder."""
    roots = tcc_roots(home)
    bad = [f"{what}: {path}" for what, path in items if is_tcc_protected(path, roots)]
    if bad:
        shown = "\n  ".join(dict.fromkeys(bad))
        raise ValueError(
            "the service would read files under a macOS-protected folder (Documents, Desktop, "
            "Downloads, iCloud Drive); launchd agents are blocked there without a Files & "
            "Folders grant and would hang at start-up:\n  "
            + shown
            + "\nInstall outside those folders (non-editable), e.g. `uv tool install "
            "'./packages/minion-executor[mlx]'`; see docs/ops/executor-install.md"
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
    variables[LABEL_ENV] = label
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
        "Umask": 0o077,
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


_PRINT_FIELDS = {
    "state": "state",
    "pid": "pid",
    "path": "path",
    "last exit code": "last_exit",
    "last exit reason": "last_exit_reason",
    "last terminating signal": "last_signal",
    "exit timeout": "exit_timeout",
}


def parse_print(text: str) -> dict[str, str]:
    """Top-level fields of the service block in `launchctl print` output.

    Only lines indented by exactly one tab count: nested sections (endpoints, ...) carry their own
    `state = active` lines that must not be mistaken for the job state.
    """
    found: dict[str, str] = {}
    for line in text.splitlines():
        if not line.startswith("\t") or line.startswith(("\t\t", "\t ")):
            continue
        key, sep, value = line[1:].partition(" = ")
        name = _PRINT_FIELDS.get(key.strip())
        if sep and name is not None and name not in found:
            found[name] = value.strip()
    return found


def check_agents_dir(path: Path, *, create: bool) -> None:
    """A real directory (not a symlink) we own, not writable by group or others."""
    if create:
        path.mkdir(mode=0o755, parents=True, exist_ok=True)
    try:
        st = path.lstat()
    except FileNotFoundError:
        if create:
            raise
        return
    if not stat.S_ISDIR(st.st_mode):
        raise ValueError(f"{path} is not a real directory (symlink?); refusing")
    if st.st_uid != os.getuid():
        raise ValueError(f"{path} is not owned by you; refusing")
    if st.st_mode & 0o022:
        raise ValueError(f"{path} is writable by group or others; refusing")


def prepare_log_file(path: Path) -> None:
    """Log directory 0700, log file 0600 (created here so launchd does not make it 0644)."""
    from minion_executor.capacity.safefile import ensure_private_dir, open_private_dir

    ensure_private_dir(path.parent)
    dfd = open_private_dir(path.parent)  # vetted: a real directory that is ours
    try:
        os.fchmod(dfd, 0o700)  # an older, laxer directory of ours is tightened too
    finally:
        os.close(dfd)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND | os.O_NOFOLLOW, 0o600)
    try:
        st = os.fstat(fd)
        if not stat.S_ISREG(st.st_mode) or st.st_uid != os.getuid():
            raise ValueError(f"{path} is not a regular file of yours; refusing")
        os.fchmod(fd, 0o600)
    finally:
        os.close(fd)


def probe_engines(requested: list[Engine], settings: ExecutorSettings) -> list[Engine]:
    """Which of `requested` work here, in a sandboxed worker subprocess (as `run` does)."""
    from minion_executor.main import probe_available, sandbox_policy

    return probe_available(requested, sandbox_policy(settings, requested))


def read_plist(path: Path) -> dict[str, object] | None:
    """The installed plist as a dict, or None when absent or unreadable."""
    try:
        with path.open("rb") as f:
            doc = plistlib.load(f)
    except (OSError, plistlib.InvalidFileException, ValueError):
        return None
    return doc if isinstance(doc, dict) else None


def is_our_plist(doc: Mapping[str, object] | None) -> bool:
    """ProgramArguments are `[…/minion-executor, run, …]`: the only jobs we remove."""
    args = doc.get("ProgramArguments") if doc else None
    return (
        isinstance(args, list)
        and len(args) >= 2
        and isinstance(args[0], str)
        and args[0].endswith("minion-executor")
        and args[1] == "run"
    )


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


def _is_darwin() -> bool:
    return sys.platform == "darwin"


def _uid() -> int:
    return os.getuid()


def _fail(message: str) -> typer.Exit:
    typer.echo(f"error: {message}", err=True)
    return typer.Exit(code=2)


def _executable() -> str:
    """Absolute path of the `minion-executor` script in this install's own environment (e.g. the
    `uv tool` venv's bin/), not the `~/.local/bin` shim. It survives `uv tool upgrade`; after an
    uninstall and reinstall of the tool, re-run `service install`."""
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
    """Validate like `run`, probe the engines, write the plist and load it.

    The token must be in the Keychain.
    """
    from minion_common.client import require_secure_url

    from minion_executor.capacity.config import OnCall, load_config, resolve_profiles
    from minion_executor.capacity.profiles import parse_profile
    from minion_executor.capacity.safefile import ensure_private_dir
    from minion_executor.capacity.signals import default_source
    from minion_executor.main import (
        DEV_ENV,
        _requested_engines,
        _validate_heartbeat,
    )
    from minion_executor.secrets import ENV_VAR, KEYCHAIN_SERVICE, keychain_present
    from minion_executor.worker.process import restrict_environ

    if not _is_darwin():
        raise _fail("launchd services are macOS only")
    home = Path.home()
    log_path = default_log_path(home, label)
    plist_dir = _agents_dir(agents_dir)
    try:
        _check_label(label)
        if os.environ.get(DEV_ENV) == "1":
            raise ValueError(f"refusing to install with {DEV_ENV}=1 (dev mode is never a service)")
        require_secure_url(server)
        check_server_url(server)
        requested = _requested_engines(engines, None)
        settings = ExecutorSettings()
        hb = os.environ.get("MINION_EXECUTOR_HEARTBEAT_INTERVAL_S")
        _validate_heartbeat(float(hb) if hb else 300.0)
        config = load_config(settings.config_path)
        specs = resolve_profiles(config)
        if profile is not None:
            parse_profile(profile, specs)
        default_source(
            None, want_call=config.on_call is not OnCall.IGNORE
        )  # the real signal source must be readable
        executable = _executable()
        check_not_protected(home, install_locations(executable, settings))
        if not keychain_present():
            hint = f" (${ENV_VAR} is not visible to launchd)" if os.environ.get(ENV_VAR) else ""
            msg = f"no Keychain item {KEYCHAIN_SERVICE!r}{hint}; see docs/ops/executor-install.md"
            if not dry_run:
                raise ValueError(msg)
            typer.echo(f"warning: {msg}", err=True)
        if not dry_run:
            check_agents_dir(plist_dir, create=True)
            ensure_private_dir(settings.state_dir)
            if (plist_dir / f"{label}.plist").is_symlink() or (
                plist_dir / f"{label}.plist"
            ).exists():
                raise ValueError(
                    f"{plist_dir / f'{label}.plist'} exists; run `service uninstall --label "
                    f"{label}` first"
                )
    except (ValueError, RuntimeError, OSError) as exc:
        raise _fail(str(exc)) from None

    carried = _carried_env(settings)
    for name in sorted(os.environ):
        if (
            name.startswith("MINION_EXECUTOR_")
            and name not in ENV_ALLOW_LIST
            and name not in (GRACE_ENV, EXIT_TIMEOUT_ENV)
        ):
            typer.echo(f"warning: {name} is set but not carried into the service", err=True)
    data = render_plist(
        label=label,
        executable=executable,
        server=server,
        log_path=str(log_path),
        grace_s=settings.stop_grace_s,
        profile=profile,
        engines=engines,
        env=carried,
    )
    plist_path = plist_dir / f"{label}.plist"
    if dry_run:
        typer.echo(data.decode())
        return
    _warn_if_lock_held(settings.state_dir)
    # The self-test (AC#6): the sandboxed probe must find an engine before a service is installed.
    # The probe worker must not inherit this shell's secrets.
    restrict_environ()
    try:
        available = probe_engines(requested, settings)
    except Exception as exc:  # RunnerError / WorkerCrashed / sandbox failure
        raise _fail(f"engine probe failed: {exc}") from None
    if not available:
        from minion_executor.main import NO_ENGINE_MSG

        raise _fail(NO_ENGINE_MSG)
    typer.echo(f"engines: {','.join(e.value for e in available)}")
    try:
        prepare_log_file(log_path)
        write_plist_safely(plist_path, data)
    except FileExistsError:
        raise _fail(f"{plist_path} exists; run `service uninstall --label {label}` first") from None
    except (OSError, ValueError) as exc:
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


def _warn_if_lock_held(state_dir: Path) -> None:
    """Another executor on this state dir: the service will retry every 60 s until it stops."""
    from minion_executor.capacity.instance import AlreadyRunningError, acquire_instance_lock
    from minion_executor.capacity.safefile import SafeFileError

    try:
        os.close(acquire_instance_lock(state_dir))
    except AlreadyRunningError:
        typer.echo(
            f"warning: an executor already runs with state dir {state_dir}; the service will "
            "retry every 60 s until it stops",
            err=True,
        )
    except SafeFileError:
        pass  # `run` reports an unusable lock file itself


def _carried_env(settings: ExecutorSettings) -> dict[str, str]:
    """Allow-listed variables from the shell, with the paths replaced by the resolved absolutes."""
    env = {k: v for k, v in os.environ.items() if k in ENV_ALLOW_LIST and v}
    env["MINION_EXECUTOR_STATE_DIR"] = str(settings.state_dir)
    env["MINION_EXECUTOR_MODEL_CACHE_DIR"] = str(settings.model_cache_dir)
    if settings.config_path is not None:
        env["MINION_EXECUTOR_CONFIG"] = str(settings.config_path)
    return env


def _wait_unloaded(label: str, timeout_s: float) -> bool:
    """Poll `launchctl print` until the service is gone (rc 113) or `timeout_s` passes."""
    deadline = _now() + timeout_s
    while True:
        rc, _ = run_launchctl(["print", f"gui/{_uid()}/{label}"])
        if rc == SERVICE_NOT_FOUND:
            return True
        if _now() >= deadline:
            return False
        _sleep(STOP_POLL_S)


def _manual_removal(label: str, plist_path: Path) -> str:
    rm = shlex.quote(str(plist_path))
    return f"remove it by hand: `launchctl bootout gui/$(id -u)/{label}` then `rm {rm}`"


@service_app.command()
def uninstall(label: LabelOpt = DEFAULT_LABEL, agents_dir: AgentsDirOpt = None) -> None:
    """Stop the service (SIGTERM, graceful) and remove the plist."""
    try:
        _check_label(label)
        plist_dir = _agents_dir(agents_dir)
        check_agents_dir(plist_dir, create=False)
    except (ValueError, OSError) as exc:
        raise _fail(str(exc)) from None
    plist_path = plist_dir / f"{label}.plist"
    doc = read_plist(plist_path)
    if doc is None:
        # Absent or unreadable. Only "not loaded and no file" is a clean nothing-to-do: a loaded
        # job, a launchd we cannot query, or a file we cannot parse must not be silently left.
        rc, _ = run_launchctl(["print", f"gui/{_uid()}/{label}"])
        present = plist_path.is_symlink() or plist_path.exists()
        if rc != SERVICE_NOT_FOUND or present:
            state = (
                "loaded"
                if rc == 0
                else "not loaded"
                if rc == SERVICE_NOT_FOUND
                else "not confirmed unloaded"
            )
            file_state = "unreadable or malformed" if present else "missing"
            raise _fail(
                f"{plist_path} is {file_state} and the service is {state}; "
                + _manual_removal(label, plist_path)
            )
        typer.echo(f"{plist_path} not present and {label} not loaded; nothing to remove")
        return
    if not is_our_plist(doc):
        raise _fail(f"{plist_path} is not a minion-executor service plist; refusing to touch it")
    raw_timeout = doc.get("ExitTimeOut")
    exit_timeout = float(raw_timeout) if isinstance(raw_timeout, int | float) else 315.0
    started = _now()
    rc, out = run_launchctl(["bootout", f"gui/{_uid()}/{label}"], timeout=LAUNCHCTL_TIMEOUT_S + 330)
    # rc 0 and 36 (EINPROGRESS) both mean "stop under way": launchd answers before the job has
    # exited (measured: rc 0 after 0.6 s, process gone ~5 s later). Wait until it is unloaded.
    if rc in (0, BOOTOUT_IN_PROGRESS):
        if not _wait_unloaded(label, exit_timeout + STOP_WAIT_SLACK_S):
            raise _fail(
                "service is still stopping after the exit timeout; plist left in place. "
                + _manual_removal(label, plist_path)
            )
        typer.echo(f"stopped {label} in {_now() - started:.1f} s")
    # 3 = not loaded (ESRCH), 113 = could not find service: fine, we still remove the plist
    elif rc not in (3, SERVICE_NOT_FOUND):
        raise _fail(f"launchctl bootout failed ({rc}): {out}; plist left in place")
    try:
        plist_path.unlink()
        typer.echo(f"removed {plist_path}")
    except FileNotFoundError:
        typer.echo(f"{plist_path} not present")
    except OSError as exc:
        raise _fail(f"cannot remove {plist_path}: {exc}") from None


def _installed_settings(
    label: str, info: Mapping[str, str]
) -> tuple[float | None, float | None, Path | None]:
    """(ExitTimeOut, stop grace, state dir) as written in the installed plist, None if unknown."""
    candidates = []
    if info.get("path"):
        candidates.append(Path(info["path"]))
    candidates.append(_agents_dir(None) / f"{label}.plist")
    for path in candidates:
        doc = read_plist(path)
        if doc is None:
            continue
        raw = doc.get("ExitTimeOut")
        env = doc.get("EnvironmentVariables")
        timeout = float(raw) if isinstance(raw, int | float) else None
        grace: float | None = None
        state: Path | None = None
        if isinstance(env, dict):
            try:
                grace = float(env[GRACE_ENV])
            except (KeyError, ValueError, TypeError):
                grace = None
            sd = env.get("MINION_EXECUTOR_STATE_DIR")
            state = Path(sd) if isinstance(sd, str) and sd else None
        return timeout, grace, state
    return None, None, None


@service_app.command("status")
def service_status(label: LabelOpt = DEFAULT_LABEL) -> None:
    """launchd state of the service (with the last exit), then the executor's own status."""
    from minion_executor.main import status as executor_status

    try:
        _check_label(label)
        settings = ExecutorSettings()
    except ValueError as exc:
        raise _fail(str(exc)) from None
    state_dir = settings.state_dir
    rc, out = run_launchctl(["print", f"gui/{_uid()}/{label}"])
    if rc == SERVICE_NOT_FOUND:
        typer.echo(f"{label}: not loaded")
    elif rc != 0:
        typer.echo(f"{label}: cannot query launchd ({rc}): {out}", err=True)
    else:
        info = parse_print(out)
        line = (
            f"{label}: state={info.get('state', '?')} pid={info.get('pid', '-')} "
            f"last_exit={info.get('last_exit', '-')}"
        )
        for key, name in (("last_exit_reason", "last_exit_reason"), ("last_signal", "signal")):
            if key in info:
                line += f" {name}={info[key]}"
        typer.echo(line)
        timeout, grace, installed_state = _installed_settings(label, info)
        state_dir = installed_state or state_dir
        if timeout is None:
            raw = info.get("exit_timeout", "").split()
            try:
                timeout = float(raw[0])
            except (ValueError, IndexError):
                typer.echo("warning: exit timeout not reported by launchd", err=True)
        if grace is None:
            grace = settings.stop_grace_s  # installed plist unreadable: the shell is the best guess
        if timeout is not None:
            typer.echo(f"exit_timeout={timeout:g}s grace={grace:g}s")
            need = exit_timeout_for(grace)
            if timeout < need:
                typer.echo(
                    f"warning: exit timeout {timeout:g}s < grace + 15 = {need}s; launchd may "
                    "SIGKILL a stopping executor. Re-run `service uninstall` and "
                    "`service install`.",
                    err=True,
                )
    failure = read_failure(state_dir)
    if failure is not None:
        typer.echo(
            f"last permanent failure: {failure['ts']} reason={failure['reason']}: "
            f"{failure['detail']}"
        )
    executor_status()
