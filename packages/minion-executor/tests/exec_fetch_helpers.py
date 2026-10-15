"""Module-level fake fetches the spawn-context fetch child can import (closures cannot be
pickled). Used by `test_model_cache.py` (spawn, unsandboxed) and the macOS sandbox test."""

from __future__ import annotations

import hashlib
import json
import os
import socket
import subprocess
from multiprocessing.connection import Connection
from pathlib import Path

from minion_executor.model_cache import FetchSpec


def _send_files(spec: FetchSpec, conn: Connection, files: dict[str, bytes]) -> None:
    plan = {k: [len(v), hashlib.sha256(v).hexdigest(), None] for k, v in files.items()}
    conn.send(("plan", sum(len(v) for v in files.values()), plan))
    assert conn.recv() == "go"
    for rel, data in files.items():
        p = Path(spec.staging) / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)
    conn.send(("done",))


def env_probe_fetch(spec: FetchSpec, conn: Connection) -> None:
    """Records which env vars the child sees, in `config.json`."""
    seen = {
        "has_token": os.environ.get("HF_TOKEN") == "hf_s3cret",
        "leak": "AWS_SECRET_ACCESS_KEY" in os.environ,
        "hf_home": os.environ.get("HF_HOME", ""),
        "xet": os.environ.get("HF_XET_CACHE", ""),
        "staging": spec.staging,
        "repo": spec.repo,
        "nice": os.nice(0),
    }
    _send_files(spec, conn, {"w.safetensors": b"x" * 10, "config.json": json.dumps(seen).encode()})


def _allowed(fn: object, *args: object) -> bool:
    try:
        fn(*args)  # type: ignore[operator]
    except OSError:
        return False
    return True


def sandbox_probe_fetch(spec: FetchSpec, conn: Connection) -> None:
    """Tries what a compromised fetch child would: results go to `config.json`."""
    home = Path.home()
    result = {
        "list_home": _allowed(os.listdir, str(home)),
        "write_outside": _allowed(
            lambda p: Path(p).write_bytes(b"x"), str(home / ".minion-fetch-sandbox-probe")
        ),
        "write_tmp": _allowed(lambda p: Path(p).write_bytes(b"x"), "/private/tmp/.minion-probe"),
        "write_staging": _allowed(
            lambda p: Path(p).write_bytes(b"x"), str(Path(spec.staging) / ".tmp" / "ok")
        ),
        "read_ca": _allowed(lambda p: Path(p).read_bytes(), os.environ.get("SSL_CERT_FILE", "")),
        "exec": False,
        "net": False,
    }
    try:
        subprocess.run(["/usr/bin/true"], check=True)
        result["exec"] = True
    except (OSError, subprocess.SubprocessError):
        pass
    port = os.environ.get("HF_HUB_PROBE_PORT")
    if port:
        try:
            socket.create_connection(("127.0.0.1", int(port)), timeout=3).close()
            result["net"] = True
        except OSError:
            pass
    _send_files(
        spec, conn, {"w.safetensors": b"x" * 10, "config.json": json.dumps(result).encode()}
    )
