"""CLI exit codes via typer's CliRunner against a MockTransport server and a FakeStore."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

import httpx
import pytest
from minion_cli import flows, main
from minion_cli.testing import FakeStore
from minion_common.client import UserClient
from pydantic import SecretStr
from typer.testing import CliRunner

SENTINEL = "SENTINEL-PAYLOAD-77c1"
OUT = "outputs/acme/default/job-1/"


def task_json(status: str, **counts: int) -> dict[str, Any]:
    return {
        "team": "acme",
        "project": "default",
        "task_name": "job-1",
        "type": "embedding",
        "model": "m",
        "input_uri": "s3://minion/inputs/acme/default/job-1/",
        "output_uri": f"s3://minion/{OUT}",
        "records_per_subtask": 500,
        "priority": 0,
        "status": status,
        "created_by": "me",
        "created_at": "2026-01-01T00:00:00Z",
        "counts": {"total": 2, **counts},
    }


def sub(i: int, status: str = "SUCCEEDED") -> dict[str, Any]:
    return {
        "subtask_id": i,
        "status": status,
        "attempt": 1,
        "max_attempts": 3,
        "record_count": 1,
        "updated_at": "2026-01-01T00:00:00Z",
    }


class Server:
    """GET task -> `status` (404 before submit when `exists` is False)."""

    def __init__(self, status: str = "SUCCEEDED", exists: bool = False) -> None:
        self.status, self.exists = status, exists
        self.created = False
        self.cancelled = False
        self.subtasks = [sub(0), sub(1)]

    def __call__(self, req: httpx.Request) -> httpx.Response:
        path = req.url.path
        if req.headers.get("authorization") != "Bearer test-key":
            return httpx.Response(401, json={"detail": {"code": "UNAUTHORIZED", "message": "no"}})
        if path.endswith("/subtasks"):
            return httpx.Response(200, json={"items": self.subtasks, "next_cursor": None})
        if path.endswith("/cancel"):
            self.cancelled = True
            return httpx.Response(200, json=task_json("CANCELLED"))
        if req.method == "POST":
            self.created = True
            return httpx.Response(201, json=task_json("SPLITTING"))
        if not (self.exists or self.created):
            return httpx.Response(404, json={"detail": {"code": "NOT_FOUND", "message": "no"}})
        return httpx.Response(200, json=task_json(self.status, succeeded=2))


@pytest.fixture
def env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> FakeStore:
    for k in ("MINION_CLI_SERVER", "MINION_CLI_TEAM", "MINION_CLI_PROJECT"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("MINION_CLI_SERVER", "http://localhost:8080")
    monkeypatch.setenv("MINION_CLI_TEAM", "acme")
    monkeypatch.setenv("MINION_API_KEY", "test-key")
    store = FakeStore()
    store.objects[f"{OUT}000000.jsonl"] = f'{{"v":"{SENTINEL}"}}\n'.encode()
    store.objects[f"{OUT}000001.jsonl"] = b'{"v":2}\n'
    monkeypatch.setattr(main, "build_store", lambda s: store)
    monkeypatch.chdir(tmp_path)
    return store


def use_server(monkeypatch: pytest.MonkeyPatch, handler: Callable[[httpx.Request], Any]) -> None:
    def make(server: str, key: str) -> UserClient:
        return UserClient(server, SecretStr(key), transport=httpx.MockTransport(handler))

    monkeypatch.setattr(main, "make_client", make)


def data_file(tmp_path: Path) -> Path:
    f = tmp_path / "data.jsonl"
    f.write_text(f'{{"text":"{SENTINEL}"}}\n')
    return f


def run(*args: str) -> Any:
    return CliRunner().invoke(main.app, list(args))


def submit_args(tmp_path: Path, *extra: str) -> list[str]:
    return [
        "submit", str(data_file(tmp_path)), "--type", "embedding", "--model", "m",
        "--name", "job-1", "-o", str(tmp_path / "out"), *extra,
    ]  # fmt: skip


def test_exit_0_submit_wait_fetch(
    env: FakeStore, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    use_server(monkeypatch, Server("SUCCEEDED"))
    r = run(*submit_args(tmp_path))
    assert r.exit_code == 0, r.output
    assert (tmp_path / "out" / "results.jsonl").read_text().splitlines()[1] == '{"v":2}'
    assert (tmp_path / "out" / "manifest.json").exists()
    assert "inputs/acme/default/job-1/0000-data.jsonl" in env.uploads
    assert SENTINEL not in r.output and "test-key" not in r.output
    assert SENTINEL not in (r.stderr if r.stderr_bytes else "")


def test_exit_1_failed_and_cancelled(env: FakeStore, monkeypatch: pytest.MonkeyPatch) -> None:
    use_server(monkeypatch, Server("FAILED", exists=True))
    assert run("status", "job-1").exit_code == 1
    use_server(monkeypatch, Server("CANCELLED", exists=True))
    assert run("fetch", "job-1").exit_code == 1


def test_exit_2_usage_errors(
    env: FakeStore, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    use_server(monkeypatch, Server("SUCCEEDED", exists=True))
    # existing task on submit
    assert run(*submit_args(tmp_path)).exit_code == 2
    # still running without --wait
    use_server(monkeypatch, Server("RUNNING", exists=True))
    assert run("fetch", "job-1").exit_code == 2
    # bad name, insecure server, missing team, missing key, bad key
    assert run("status", "Bad Name").exit_code == 2
    assert run("status", "job-1", "--server", "http://example.com").exit_code == 2
    monkeypatch.delenv("MINION_CLI_TEAM")
    assert run("status", "job-1").exit_code == 2
    monkeypatch.setenv("MINION_CLI_TEAM", "acme")
    monkeypatch.setenv("MINION_API_KEY", "wrong")
    assert run("status", "job-1").exit_code == 2


def test_no_key_exit_2(env: FakeStore, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MINION_API_KEY")
    monkeypatch.setattr("minion_cli.secrets.keychain_api_key", lambda: None)
    r = run("status", "job-1")
    assert r.exit_code == 2


def test_exit_3_partial(env: FakeStore, monkeypatch: pytest.MonkeyPatch) -> None:
    server = Server("PARTIAL", exists=True)
    server.subtasks = [sub(0), sub(1, "FAILED")]
    use_server(monkeypatch, server)
    r = run("fetch", "job-1")
    assert r.exit_code == 3
    assert (Path("job-1") / "results.jsonl").exists()


def test_exit_4_unreachable_and_timeout(env: FakeStore, monkeypatch: pytest.MonkeyPatch) -> None:
    def down(req: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("down")

    use_server(monkeypatch, down)
    assert run("status", "job-1").exit_code == 4
    use_server(monkeypatch, Server("RUNNING", exists=True))
    assert run("fetch", "job-1", "--wait", "--timeout", "0").exit_code == 4


def test_exit_130_ctrl_c_never_cancels(
    env: FakeStore, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    server = Server("RUNNING")
    use_server(monkeypatch, server)

    async def interrupted(*a: object, **k: object) -> None:
        raise KeyboardInterrupt

    monkeypatch.setattr(flows, "wait_task", interrupted)
    r = run(*submit_args(tmp_path))
    assert r.exit_code == 130
    assert server.created and not server.cancelled
    assert "keeps running" in r.output


def test_cancel_command(env: FakeStore, monkeypatch: pytest.MonkeyPatch) -> None:
    server = Server("RUNNING", exists=True)
    use_server(monkeypatch, server)
    r = run("cancel", "job-1")
    assert r.exit_code == 0 and server.cancelled


def test_no_wait_returns_after_submit(
    env: FakeStore, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    use_server(monkeypatch, Server("RUNNING"))
    r = run(*submit_args(tmp_path, "--no-wait"))
    assert r.exit_code == 0
    assert not (tmp_path / "out").exists()


def test_fetch_is_idempotent(env: FakeStore, monkeypatch: pytest.MonkeyPatch) -> None:
    use_server(monkeypatch, Server("SUCCEEDED", exists=True))
    assert run("fetch", "job-1").exit_code == 0
    n = len(env.downloads)
    assert run("fetch", "job-1").exit_code == 0
    assert len(env.downloads) == n


def test_params_validation(env: FakeStore, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    use_server(monkeypatch, Server("SUCCEEDED"))
    assert run(*submit_args(tmp_path, "--params", "[1]")).exit_code == 2
    assert run(*submit_args(tmp_path, "--params", "{")).exit_code == 2


def test_invalid_config_does_not_echo_value(
    env: FakeStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MINION_CLI_S3_ENDPOINT_URL", f"http://evil.example/{SENTINEL}")
    r = run("status", "job-1")
    assert r.exit_code == 2 and SENTINEL not in r.output
