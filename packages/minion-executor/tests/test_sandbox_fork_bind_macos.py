"""fork and listening sockets are denied in both sandbox profiles (a setsid grandchild binding
127.0.0.1:11434 must be impossible). Real sandbox, throwaway subprocesses. Auto-skipped off
macOS (conftest)."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
from minion_executor.worker import sandbox as sbx

pytestmark = pytest.mark.macos

CHILD = r"""
import json, os, socket, sys
from minion_executor.worker import sandbox as sbx

spec = json.loads(sys.argv[1])
if spec:
    sbx.apply_from_spec(spec)

def fork():
    try:
        pid = os.fork()
    except OSError:
        return False
    if pid == 0:
        os._exit(0)
    os.waitpid(pid, 0)
    return True

def bind(af, addr):
    s = socket.socket(af)
    try:
        s.bind(addr)
        s.listen(1)
        return True
    except OSError:
        return False
    finally:
        s.close()

print(json.dumps({
    "fork": fork(),
    "bind": bind(socket.AF_INET, ("127.0.0.1", 0)),
    "bind6": bind(socket.AF_INET6, ("::1", 0)),
}))
"""


def _run(kind: str, tmp_path: Path) -> dict[str, bool]:
    child = tmp_path / "child.py"
    child.write_text(CHILD)
    spec: dict[str, object] = {}
    if kind != "none":
        spec = {
            "read": sbx.python_read_paths(),
            "write": [str(tmp_path)],
            "metal": False,
            "engines": ["fetch"] if kind == "fetch" else ["fake"],
        }
        if kind == "fetch":
            spec["network"] = True
    out = subprocess.run(  # noqa: S603 - fixed argv, test-owned script
        [sys.executable, "-I", str(child), json.dumps(spec)],
        env=dict(os.environ),
        capture_output=True,
        text=True,
        timeout=120,
        check=True,
    )
    res: dict[str, bool] = json.loads(out.stdout.strip().splitlines()[-1])
    return res


def test_baseline_unsandboxed_can_fork_and_bind(tmp_path: Path) -> None:
    r = _run("none", tmp_path)
    assert r["fork"] is True and r["bind"] is True, r


@pytest.mark.parametrize("kind", ["worker", "fetch"])
def test_fork_and_bind_are_denied(kind: str, tmp_path: Path) -> None:
    r = _run(kind, tmp_path)
    assert r["fork"] is False, r
    assert r["bind"] is False and r["bind6"] is False, r
