"""macOS Seatbelt sandbox for the inference worker (ADR-0009, D8).

`build_profile` is a pure function producing SBPL text; `apply` hands it to `libsandbox`'s
`sandbox_init` through ctypes. Everything deprecated or platform-specific about that API is
isolated here: Apple marks `sandbox_init` deprecated but ships no replacement for an
unprivileged, in-process, self-imposed sandbox (Chromium, Firefox and Bazel rely on it too).

Policy (profile order matters: the last matching rule wins):
  * start from `(allow default)`; the worker needs Metal, IOKit and dyld, which a deny-default
    profile cannot be validated against without every supported Mac and engine
  * no network at all, except outbound TCP to loopback `ollama_port` when Ollama is requested
  * the owner's HOME is unreadable and unwritable, except the explicit read-only allow list
    (model dir, cache root, Python prefixes and import path) and the worker's tmp dir
  * the read-only allow list is also write-protected (a compromised worker cannot poison
    other cached models)
  * no exec of any other program, no lookup of the Security server / securityd
"""

from __future__ import annotations

import ctypes
import ctypes.util
import os
import sys
import sysconfig
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from minion_executor.runners.base import RunnerError

SANDBOX_FAILED = "SANDBOX_FAILED"

# Mach services that front the Keychain / credential stores.
_DENIED_MACH_SERVICES = (
    "com.apple.SecurityServer",
    "com.apple.securityd.xpc",
    "com.apple.secd",
    "com.apple.security.agent",
    "com.apple.security.XPCKeychainSandboxCheck",
    "com.apple.security.keychain-cache",
)


@dataclass(frozen=True)
class SandboxPolicy:
    """Control-side sandbox settings for a `WorkerHandle` (the paths that vary per load are
    added by the handle)."""

    cache_root: Path | None = None
    ollama_port: int | None = None


def sbpl_quote(value: str) -> str:
    """SBPL string literal. Backslash and double quote are escaped; control characters (which
    could smuggle a newline into the profile) are rejected."""
    if any(ord(c) < 0x20 or ord(c) == 0x7F for c in value):
        raise ValueError("control character in sandbox path")
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


def _subpath(p: str) -> str:
    return f"(subpath {sbpl_quote(p)})"


def _ancestors_within(home: str, path: str) -> list[str]:
    """Directories strictly between `home` and `path` (plus `path`'s parent chain), outermost
    first, so that `stat()` walks to an allowed path under HOME keep working."""
    h, p = Path(home), Path(path)
    try:
        rel = p.relative_to(h)
    except ValueError:
        return []
    out = [str(h)]
    cur = h
    for part in rel.parts[:-1]:
        cur = cur / part
        out.append(str(cur))
    return out


def build_profile(
    home: str | Iterable[str],
    allow_read: Sequence[str],
    allow_write: Sequence[str],
    ollama_port: int | None = None,
) -> str:
    """SBPL profile text. Pure: paths must already be absolute and symlink-resolved (Seatbelt
    matches resolved paths); see `resolve_paths`."""
    homes = [home] if isinstance(home, str) else list(home)
    homes = list(dict.fromkeys(h for h in homes if h and h != "/"))
    lines = ["(version 1)", "(allow default)"]

    lines.append("(deny network*)")
    if ollama_port is not None:
        if not 0 < ollama_port < 65536:
            raise ValueError("bad ollama port")
        lines.append(f'(allow network-outbound (remote tcp "localhost:{ollama_port}"))')

    lines.append("(deny process-exec*)")
    names = " ".join(f"(global-name {sbpl_quote(n)})" for n in _DENIED_MACH_SERVICES)
    lines.append(f"(deny mach-lookup {names})")

    for h in homes:
        lines.append(f"(deny file-read* file-write* {_subpath(h)})")
    metadata: list[str] = []
    for h in homes:
        for p in allow_read:
            metadata += _ancestors_within(h, p)
        for p in allow_write:
            metadata += _ancestors_within(h, p)
    for d in dict.fromkeys(metadata):
        lines.append(f"(allow file-read-metadata (literal {sbpl_quote(d)}))")
    for p in dict.fromkeys(allow_read):
        lines.append(f"(allow file-read* {_subpath(p)})")
    for p in dict.fromkeys(allow_read):  # read-only means read-only, wherever it lives
        lines.append(f"(deny file-write* {_subpath(p)})")
    for p in dict.fromkeys(allow_write):
        lines.append(f"(allow file-read* file-write* {_subpath(p)})")
    return "\n".join(lines) + "\n"


def python_read_paths() -> list[str]:
    """Where this interpreter imports code from: prefixes, site-packages and every existing
    `sys.path` entry (editable installs live in the source checkout)."""
    cands: list[str] = [sys.prefix, sys.base_prefix, sys.exec_prefix, sys.base_exec_prefix]
    for key in ("purelib", "platlib", "stdlib", "platstdlib"):
        v = sysconfig.get_paths().get(key)
        if v:
            cands.append(v)
    cands += [p for p in sys.path if p]
    return [p for p in cands if Path(p).is_dir()]


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


def apply_from_spec(spec: dict[str, Any]) -> None:
    """Worker-side entry: build the profile from a control-side spec and apply it. A no-op off
    macOS (the engines are macOS-only; only the fake engine runs elsewhere, in dev/CI)."""
    if sys.platform != "darwin":
        return
    homes = home_dirs()
    try:
        profile = build_profile(
            homes,
            resolve_paths(spec.get("read", []), home=homes),
            resolve_paths(spec.get("write", []), home=homes),
            spec.get("ollama_port"),
        )
    except (ValueError, TypeError) as exc:
        raise RunnerError(SANDBOX_FAILED, f"bad profile: {type(exc).__name__}") from None
    apply(profile)


def sandbox_spec(
    policy: SandboxPolicy, *, model_path: Path | None, tmp_dir: Path, with_python: bool = True
) -> dict[str, Any]:
    """Control-side: the JSON-able spec sent with `load` / `probe`."""
    read: list[str | Path] = []
    if with_python:
        read += python_read_paths()
    if policy.cache_root is not None:
        read.append(policy.cache_root)
    if model_path is not None:
        read.append(model_path)
    return {
        "read": [str(p) for p in read],
        "write": [str(tmp_dir)],
        "ollama_port": policy.ollama_port,
    }
