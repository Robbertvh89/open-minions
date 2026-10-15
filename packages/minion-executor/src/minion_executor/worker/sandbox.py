"""macOS Seatbelt sandbox for the inference worker (ADR-0009, D8).

`build_profile` is a pure function producing SBPL text; `apply` hands it to `libsandbox`'s
`sandbox_init` through ctypes. Everything deprecated or platform-specific about that API is
isolated here: Apple marks `sandbox_init` deprecated but ships no replacement for an
unprivileged, in-process, self-imposed sandbox (Chromium, Firefox and Bazel rely on it too).

Policy (profile order matters: the last matching rule wins). The base is `(allow default)`
because Metal, IOKit and dyld cannot be enumerated for every Mac; the profile then *denies
whole classes* and re-allows the minimum, validated against a real MLX GPU run (macos tests):
  * network: none, except outbound TCP to loopback `ollama_port` (Ollama workers only)
  * process-exec: none
  * LaunchServices (`lsopen`), AppleEvents, distributed notifications, other-process signals:
    denied (these are the "launch an app / drive an app / talk to other processes" escapes)
  * user preferences (`cfprefsd`): denied, so `~/Library/Preferences` is not readable through
    CFPreferences even though the file path is denied
  * Mach lookups: deny all, then allow only `MACH_ALLOW` (see the rationale there)
  * file writes: deny everything, then allow the worker tmp dir, a few `/dev` nodes and the
    per-user Metal shader cache
  * file reads: HOME, `/Users`, `/Volumes`, `/private/tmp`, `/private/var/folders` and
    `/Library` are denied, then the Python/model/tmp grants are re-allowed read-only
Residual risk is documented in ADR-0009 (read of the rest of the filesystem outside those
roots, e.g. `/etc`, `/opt`, `/var`; mach services on the allow list).
"""

from __future__ import annotations

import contextlib
import ctypes
import ctypes.util
import os
import sys
from collections.abc import Collection, Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from minion_executor.runners.base import RunnerError

SANDBOX_FAILED = "SANDBOX_FAILED"

# Mach services the worker may look up (everything else is denied). Ablation on macOS 27 /
# mlx 0.32 (`mx.compile`, `mx.fast.metal_kernel`, matmul/softmax on the GPU) showed MLX runs
# with an EMPTY list; the Metal/logging/libinfo services below are kept anyway because other
# macOS releases, Metal's JIT path (MTLCompilerService) and mlx-lm / transformers lean on them,
# and each is a system daemon that exposes no user data:
#   MTLCompilerService, cvmsServ, gpumemd.source  Metal shader compile / GPU memory accounting
#   logd                                          os_log / unified logging
#   system.notification_center                    notify(3), used by CoreFoundation and Metal
#   opendirectoryd.libinfo                        getpwuid / getgrgid style libinfo lookups
# Explicitly NOT here (asserted denied by tests): launchservicesd, appleevents, pasteboard,
# nsurlsessiond, windowserver, mds, tccd, SecurityServer, secd, trustd.
MACH_ALLOW = (
    "com.apple.MTLCompilerService",
    "com.apple.cvmsServ",
    "com.apple.gpumemd.source",
    "com.apple.logd",
    "com.apple.system.notification_center",
    "com.apple.system.opendirectoryd.libinfo",
)
MACH_ALLOW_PREFIXES: tuple[str, ...] = ()

# Device nodes a process legitimately writes to.
DEV_WRITE = ("/dev/null", "/dev/dtracehelper", "/dev/tty", "/dev/zero")

# Roots denied for reading (re-allowed selectively). HOME is added by the caller.
READ_DENY_ROOTS = (
    "/Users",
    "/Volumes",
    "/private/tmp",
    "/private/var/tmp",
    "/private/var/folders",
    "/Library",
)
# Nothing under /Library is re-allowed: ablation on macOS 27 showed MLX (GPU matmul, mx.compile
# and a custom Metal kernel) needs no /Library path, only /System, /usr and the Python prefix.

# Metal's per-process shader cache lives in `<DARWIN_USER_CACHE_DIR>/<bundle id>/com.apple.metal`;
# a Python worker's bundle id is `org.python.python` (framework builds) or the executable name.
# Only those two are granted; other apps' caches are neither readable nor writable.
METAL_CACHE_OWNERS = r"(org\.python\.python|python[0-9.]*)"


@dataclass(frozen=True)
class SandboxPolicy:
    """Control-side sandbox settings for a `WorkerHandle` (the paths that vary per load are
    added by the handle). `cache_root` is only validated; the worker is granted read access to
    the one model dir of the current load, not the whole cache. The Ollama port is not part of
    the policy: the handle derives it from its own `EngineConfig`, for Ollama loads only."""

    cache_root: Path | None = None


def sbpl_quote(value: str) -> str:
    """SBPL string literal. Backslash and double quote are escaped; control characters (which
    could smuggle a newline into the profile) are rejected."""
    if any(ord(c) < 0x20 or ord(c) == 0x7F for c in value):
        raise ValueError("control character in sandbox path")
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


def _subpath(p: str) -> str:
    return f"(subpath {sbpl_quote(p)})"


def _ancestors(path: str) -> list[str]:
    """Every proper ancestor directory of `path`, outermost first (excluding `/`), so `stat()`
    walks down to an allowed path below a denied root keep working."""
    parts = Path(path).parts[1:-1]
    cur = Path("/")
    out: list[str] = []
    for part in parts:
        cur = cur / part
        out.append(str(cur))
    return out


def _under(path: str, root: str) -> bool:
    return path == root or path.startswith(root.rstrip("/") + "/")


def build_profile(
    home: str | Iterable[str],
    allow_read: Sequence[str],
    allow_write: Sequence[str],
    ollama_port: int | None = None,
    *,
    metal_cache: str | None = None,
) -> str:
    """SBPL profile text. Pure: paths must already be absolute and symlink-resolved (Seatbelt
    matches resolved paths); see `resolve_paths`. `metal_cache` is the per-user cache dir
    (`DARWIN_USER_CACHE_DIR`) whose `com.apple.metal` subdirectories may be written."""
    homes = [home] if isinstance(home, str) else list(home)
    homes = list(dict.fromkeys(h for h in homes if h and h != "/"))
    lines = ["(version 1)", "(allow default)"]

    lines.append("(deny network*)")
    if ollama_port is not None:
        if not 0 < ollama_port < 65536:
            raise ValueError("bad ollama port")
        lines.append(f'(allow network-outbound (remote tcp "localhost:{ollama_port}"))')

    lines.append("(deny process-exec*)")
    lines += ["(deny lsopen)", "(deny appleevent-send)", "(deny distributed-notification-post)"]
    lines.append("(deny user-preference-read user-preference-write)")
    # `(deny signal (target others))` is accepted but does NOT deny (verified: SIGCONT to the
    # parent still succeeds); deny-all plus an allow for self does.
    lines += ["(deny signal)", "(allow signal (target self))"]

    lines.append("(deny mach-lookup)")
    names = " ".join(f"(global-name {sbpl_quote(n)})" for n in MACH_ALLOW)
    names += "".join(f" (global-name-prefix {sbpl_quote(n)})" for n in MACH_ALLOW_PREFIXES)
    lines.append(f"(allow mach-lookup {names})")

    # --- writes: nothing, except the tmp dir, a few device nodes and the Metal shader cache
    lines.append('(deny file-write* (subpath "/"))')
    for dev in DEV_WRITE:
        lines.append(f"(allow file-write* (literal {sbpl_quote(dev)}))")

    # --- reads
    roots = list(dict.fromkeys([*homes, *READ_DENY_ROOTS]))
    for r in roots:
        lines.append(f"(deny file-read* {_subpath(r)})")
    reads = list(dict.fromkeys([*allow_read, *allow_write]))
    metadata: list[str] = []
    for p in reads:
        if any(_under(p, r) for r in roots):
            metadata += _ancestors(p)
    for d in dict.fromkeys(metadata):
        if any(_under(d, r) for r in roots):
            lines.append(f"(allow file-read-metadata (literal {sbpl_quote(d)}))")
    for p in dict.fromkeys(allow_read):
        lines.append(f"(allow file-read* {_subpath(p)})")
    for p in dict.fromkeys(allow_write):
        lines.append(f"(allow file-read* file-write* {_subpath(p)})")
    if metal_cache:
        base = metal_cache.rstrip("/")
        for d in _ancestors(base + "/x"):
            if any(_under(d, r) for r in roots):
                lines.append(f"(allow file-read-metadata (literal {sbpl_quote(d)}))")
        rx = _regex_escape(base) + "/" + METAL_CACHE_OWNERS + r"/com\.apple\.metal(/.*)?"
        lines.append(f'(allow file-read* file-write* (regex #"^{rx}$"))')
    return "\n".join(lines) + "\n"


def _regex_escape(s: str) -> str:
    if any(ord(c) < 0x20 or c in '"' for c in s):
        raise ValueError("control character in sandbox path")
    out = []
    for c in s:
        out.append("\\" + c if c in ".^$*+?()[]{}|\\" else c)
    return "".join(out)


def _pth_roots(site_dir: Path) -> list[str]:
    """Directories named by `*.pth` files (editable installs)."""
    out: list[str] = []
    try:
        pths = sorted(site_dir.glob("*.pth"))
    except OSError:
        return out
    for pth in pths:
        try:
            text = pth.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        for line in text.splitlines():
            line = line.strip()
            if not line or line.startswith(("#", "import ", "import\t")):
                continue
            cand = site_dir / line  # absolute `line` replaces site_dir
            if cand.is_dir():
                out.append(str(cand))
    return out


def python_read_paths() -> list[str]:
    """Where this interpreter imports code from: `sys.prefix`, `sys.base_prefix`, the
    site-packages directories and the source roots editable installs point to via `.pth`
    files. Deliberately NOT `sys.path[0]`/cwd or `PYTHONPATH`."""
    import site
    import sysconfig

    cands: list[str] = [sys.prefix, sys.base_prefix]
    sites: list[str] = []
    for key in ("purelib", "platlib"):
        v = sysconfig.get_paths().get(key)
        if v:
            sites.append(v)
    with contextlib.suppress(AttributeError):  # very old virtualenvs lack getsitepackages
        sites += site.getsitepackages()
    cands += sites
    for sd in dict.fromkeys(sites):
        cands += _pth_roots(Path(sd))
    return [p for p in dict.fromkeys(cands) if Path(p).is_dir()]


def resolve_paths(paths: Iterable[str | Path], *, home: Iterable[str] = ()) -> list[str]:
    """Absolute, symlink-resolved, de-duplicated. Entries that are `/` or a HOME (or an ancestor
    of one) are dropped: allowing those would re-open everything the profile denies."""
    homes = [os.path.realpath(h) for h in home]
    out: list[str] = []
    for p in paths:
        r = os.path.realpath(p)
        if r == "/" or any(h == r or h.startswith(r.rstrip("/") + "/") for h in homes):
            continue
        if r not in out:
            out.append(r)
    return out


def home_dirs() -> list[str]:
    """The owner's home from the passwd database and from `$HOME`, resolved."""
    import pwd

    out = [pwd.getpwuid(os.getuid()).pw_dir]
    env_home = os.environ.get("HOME")
    if env_home:
        out.append(env_home)
    return list(dict.fromkeys(os.path.realpath(h) for h in out))


def user_cache_dir() -> str | None:
    """`DARWIN_USER_CACHE_DIR` (`/private/var/folders/xx/yyyy/C`), where Metal keeps shader
    caches; `None` if it cannot be determined."""
    try:
        libc = ctypes.CDLL(None)
        buf = ctypes.create_string_buffer(1024)
        n = libc.confstr(65538, buf, ctypes.c_size_t(len(buf)))  # _CS_DARWIN_USER_CACHE_DIR
    except (OSError, AttributeError):
        return None
    if n <= 0:
        return None
    return os.path.realpath(buf.value.decode("utf-8", "replace"))


def _load_libsandbox() -> Any:
    name = ctypes.util.find_library("sandbox") or "/usr/lib/libsandbox.1.dylib"
    lib = ctypes.CDLL(name, use_errno=True)
    lib.sandbox_init.argtypes = [
        ctypes.c_char_p,
        ctypes.c_uint64,
        ctypes.POINTER(ctypes.c_char_p),
    ]
    lib.sandbox_init.restype = ctypes.c_int
    lib.sandbox_free_error.argtypes = [ctypes.c_char_p]
    lib.sandbox_free_error.restype = None
    return lib


def apply(profile: str) -> None:
    """Confine the calling process (irreversibly). Raises `RunnerError(SANDBOX_FAILED)` on any
    failure; never silently continues unsandboxed. Only call on macOS."""
    if sys.platform != "darwin":
        raise RunnerError(SANDBOX_FAILED, "Seatbelt is only available on macOS")
    try:
        lib = _load_libsandbox()
        err = ctypes.c_char_p()
        rc = lib.sandbox_init(profile.encode("utf-8"), 0, ctypes.byref(err))
    except (OSError, AttributeError, ValueError) as exc:
        raise RunnerError(SANDBOX_FAILED, f"libsandbox: {type(exc).__name__}") from None
    if rc != 0:
        detail = err.value.decode("utf-8", "replace") if err.value else "unknown error"
        if err.value:
            lib.sandbox_free_error(err)
        raise RunnerError(SANDBOX_FAILED, detail[:200])


def _self_test(homes: Sequence[str]) -> None:
    """After `apply`: HOME must be unlistable and a write outside the grant must fail, else the
    sandbox is not doing its job and the worker exits (never continue unconfined)."""
    ok = True
    for h in homes:
        try:
            os.listdir(h)  # noqa: PTH208 - must really read the directory
            ok = False
        except OSError:
            pass
    probe = "/private/tmp/.minion-sandbox-selftest-" + str(os.getpid())
    try:
        os.close(os.open(probe, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600))
        ok = False
        Path(probe).unlink()
    except OSError:
        pass
    if not ok:
        os._exit(SELF_TEST_EXIT_CODE)


SELF_TEST_EXIT_CODE = 78


def apply_from_spec(spec: dict[str, Any]) -> None:
    """Worker-side entry: build the profile from a control-side spec and apply it. Off macOS
    only the fake engine may run (dev/CI): any other engine fails closed."""
    engines = [str(e) for e in spec.get("engines", [])]
    if sys.platform != "darwin":
        if engines and all(e == "fake" for e in engines):
            return
        raise RunnerError(SANDBOX_FAILED, "no sandbox available on this platform")
    homes = home_dirs()
    reads = resolve_paths(spec.get("read", []), home=homes)
    writes = resolve_paths(spec.get("write", []), home=homes)
    try:
        profile = build_profile(
            homes, reads, writes, spec.get("ollama_port"), metal_cache=user_cache_dir()
        )
    except (ValueError, TypeError) as exc:
        raise RunnerError(SANDBOX_FAILED, f"bad profile: {type(exc).__name__}") from None
    # Lazy imports made after the sandbox is on (e.g. `encodings.idna` in getaddrinfo) must not
    # trip over unreadable `sys.path` entries (cwd as '' under HOME): load what is commonly
    # needed now, move to the writable tmp dir and keep only granted import roots.
    with contextlib.suppress(ImportError):
        import encodings.idna  # noqa: F401
    if writes:
        with contextlib.suppress(OSError):
            os.chdir(writes[0])
    granted = [*reads, *writes]
    sys.path[:] = [
        p for p in sys.path if p and any(_under(os.path.realpath(p), g) for g in granted)
    ]
    sys.path_importer_cache.clear()
    apply(profile)
    _self_test(homes)


def check_model_dir(path: Path, homes: Iterable[str] | None = None) -> str:
    """Resolve `path` and refuse a dir that is (or contains) HOME or `~/Library/Keychains`."""
    real = os.path.realpath(path)
    for h in homes if homes is not None else home_dirs():
        hr = os.path.realpath(h)
        for guarded in (hr, str(Path(hr) / "Library" / "Keychains")):
            if _under(guarded, real):  # real == guarded, or real is an ancestor of it
                raise RunnerError(SANDBOX_FAILED, "model dir would expose the home directory")
    if real == "/":
        raise RunnerError(SANDBOX_FAILED, "model dir would expose the home directory")
    return real


def sandbox_spec(
    policy: SandboxPolicy,
    *,
    model_path: Path | None,
    tmp_dir: Path,
    engines: Collection[str],
    ollama_port: int | None = None,
    with_python: bool = True,
) -> dict[str, Any]:
    """Control-side: the JSON-able spec sent with `load` / `probe`. `ollama_port` must be passed
    only for an Ollama load."""
    read: list[str | Path] = []
    if with_python:
        read += python_read_paths()
    if policy.cache_root is not None:
        check_model_dir(policy.cache_root)
    if model_path is not None:
        read.append(check_model_dir(model_path))
    return {
        "read": [str(p) for p in read],
        "write": [str(tmp_dir)],
        "ollama_port": ollama_port,
        "engines": sorted(engines),
    }
