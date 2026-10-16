"""Seatbelt gaps found by the security re-audit: unix-socket connects and other processes'
launch environment (KERN_PROCARGS2). Real sandbox, throwaway subprocesses. Auto-skipped off
macOS (conftest)."""

from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
from collections.abc import Iterator
from pathlib import Path

import pytest
from minion_executor.worker import sandbox as sbx

pytestmark = pytest.mark.macos

MARKER = "minion-procargs-marker-7f3a9c"

# Runs sandboxed (or not). Prints what it could do.
CHILD = r"""
import ctypes, json, os, socket, sys
from minion_executor.worker import sandbox as sbx

spec = json.loads(sys.argv[1])
if spec:
    sbx.apply_from_spec(spec)

def procargs(pid):
    libc = ctypes.CDLL(None, use_errno=True)
    mib = (ctypes.c_int * 3)(1, 49, pid)
    size = ctypes.c_size_t(1 << 18)
    buf = ctypes.create_string_buffer(size.value)
    if libc.sysctl(mib, 3, buf, ctypes.byref(size), None, 0) != 0:
        return None
    return buf.raw[: size.value]

def connect(af, addr):
    s = socket.socket(af)
    s.settimeout(3)
    try:
        s.connect(addr)
        return True
    except OSError:
        return False
    finally:
        s.close()

pa = procargs(os.getppid())
print(json.dumps({
    "procargs": pa is not None,
    "marker": pa is not None and sys.argv[2].encode() in pa,
    "self_procargs": procargs(os.getpid()) is not None,
    "unix": connect(socket.AF_UNIX, sys.argv[3]),
    "loopback": connect(socket.AF_INET, ("127.0.0.1", int(sys.argv[4]))),
}))
"""

# The middle process: launched WITH the marker, then pops it from os.environ (as
# `restrict_environ()` does) before starting the confined child.
PARENT = r"""
import os, subprocess, sys
os.environ.pop(sys.argv[1], None)
subprocess.run([sys.executable, "-I", *sys.argv[2:]], env=dict(os.environ), check=True)
"""


@pytest.fixture
def unix_socket() -> Iterator[str]:
    d = tempfile.mkdtemp(prefix="mx-", dir="/tmp")
    path = str(Path(d) / "s.sock")
    srv = socket.socket(socket.AF_UNIX)
    srv.bind(path)
    srv.listen(8)
    try:
        yield path
    finally:
        srv.close()
        shutil.rmtree(d, ignore_errors=True)


@pytest.fixture
def loopback_port() -> Iterator[int]:
    srv = socket.socket()
    srv.bind(("127.0.0.1", 0))
    srv.listen(8)
    stop = threading.Event()

    def accept() -> None:
        srv.settimeout(0.2)
        while not stop.is_set():
            try:
                srv.accept()[0].close()
            except OSError:
                continue

    t = threading.Thread(target=accept, daemon=True)
    t.start()
    try:
        yield int(srv.getsockname()[1])
    finally:
        stop.set()
        t.join(2)
        srv.close()


def _spec(kind: str, tmp: Path) -> dict[str, object]:
    base: dict[str, object] = {
        "read": sbx.python_read_paths(),
        "write": [str(tmp)],
        "metal": False,
    }
    if kind == "fetch":
        return {**base, "engines": ["fetch"], "network": True}
    return {**base, "engines": ["fake"]}


def _run(kind: str, tmp_path: Path, unix_socket: str, port: int) -> dict[str, bool]:
    parent, child = tmp_path / "parent.py", tmp_path / "child.py"
    parent.write_text(PARENT)
    child.write_text(CHILD)
    spec = {} if kind == "none" else _spec(kind, tmp_path)
    out = subprocess.run(  # noqa: S603 - fixed argv, test-owned scripts
        [
            sys.executable,
            "-I",
            str(parent),
            "MINION_AUDIT_MARKER",
            str(child),
            json.dumps(spec),
            MARKER,
            unix_socket,
            str(port),
        ],
        env={**os.environ, "MINION_AUDIT_MARKER": MARKER},
        capture_output=True,
        text=True,
        timeout=120,
        check=True,
    )
    res: dict[str, bool] = json.loads(out.stdout.strip().splitlines()[-1])
    return res


def test_baseline_unsandboxed_reads_the_parent_launch_env(
    tmp_path: Path, unix_socket: str, loopback_port: int
) -> None:
    """Guards the assertions below against passing for the wrong reason."""
    r = _run("none", tmp_path, unix_socket, loopback_port)
    assert r["marker"] is True, r  # the env var popped from os.environ is still in procargs
    assert r["unix"] is True and r["loopback"] is True, r


@pytest.mark.parametrize("kind", ["worker", "fetch"])
def test_procargs_unix_sockets_and_loopback_are_denied(
    kind: str, tmp_path: Path, unix_socket: str, loopback_port: int
) -> None:
    r = _run(kind, tmp_path, unix_socket, loopback_port)
    assert r["procargs"] is False and r["marker"] is False, r
    assert r["unix"] is False, r
    assert r["loopback"] is False, r
