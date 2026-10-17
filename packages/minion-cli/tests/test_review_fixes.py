"""TASK-25 review fixes: out-dir ownership, ETag-checked resume, pagination bounds, Ctrl-C."""

from __future__ import annotations

import json
import os
import stat
import sys
import threading
from collections.abc import Callable
from pathlib import Path
from typing import Any

import httpx
import pytest
import typer
from minion_cli import flows, main, store
from minion_cli.errors import EXIT_UNREACHABLE, EXIT_USAGE, CliError, clean
from minion_cli.flows import Target
from minion_cli.settings import CliSettings
from minion_common.client import UserClient
from minion_common.schemas import TaskOut
from pydantic import SecretStr
from typer.testing import CliRunner

sys.path.insert(0, str(Path(__file__).parent))  # tests/fakes.py (importlib import mode)
from fakes import FakeStore

T = Target("acme", "default", "job-1")
BASE = "outputs/acme/default/job-1/"
IN = "inputs/acme/default/job-1/"


def settings() -> CliSettings:
    return CliSettings(server="http://localhost:8080", team="acme")


def task_json(status: str = "SUCCEEDED", **over: Any) -> dict[str, Any]:
    total = over.pop("total", 2)
    return {
        "team": "acme",
        "project": "default",
        "task_name": "job-1",
        "type": "embedding",
        "model": "m",
        "input_uri": f"s3://minion/{IN}",
        "output_uri": f"s3://minion/{BASE}",
        "records_per_subtask": 500,
        "priority": 0,
        "status": status,
        "created_by": "me",
        "created_at": "2026-01-01T00:00:00Z",
        "counts": {"total": total},
        **over,
    }


def sub(i: int, status: str = "SUCCEEDED") -> dict[str, Any]:
    return {
        "subtask_id": i,
        "status": status,
        "attempt": 1,
        "max_attempts": 3,
        "record_count": 2,
        "error_code": "OOM" if status == "FAILED" else None,
        "updated_at": "2026-01-01T00:00:00Z",
    }


def client(handler: Callable[[httpx.Request], httpx.Response]) -> UserClient:
    return UserClient(
        "http://localhost:8080", SecretStr("k"), transport=httpx.MockTransport(handler)
    )


def page_handler(*pages: list[dict[str, Any]]) -> Callable[[httpx.Request], httpx.Response]:
    def h(req: httpx.Request) -> httpx.Response:
        assert req.url.params.get("limit") == "100"
        idx = int(req.url.params.get("cursor") or 0)
        nxt = str(idx + 1) if idx + 1 < len(pages) else None
        return httpx.Response(200, json={"items": pages[idx], "next_cursor": nxt})

    return h


async def nosleep(_: float) -> None:
    return None


def seed(store_: FakeStore, ids: list[int]) -> None:
    for i in ids:
        store_.put(f"{BASE}{i:06d}.jsonl", f'{{"o":{i}}}\n'.encode())
    store_.put(f"{BASE}_attempts/000001/1.jsonl", b"junk\n")


async def fetch(
    s: FakeStore,
    out: Path,
    task: dict[str, Any] | None = None,
    subs: list[dict[str, Any]] | None = None,
) -> flows.FetchResult:
    async with client(page_handler(subs or [sub(0), sub(1)])) as c:
        return await flows.fetch_results(
            c, s, settings(), T, TaskOut.model_validate(task or task_json()), out, sleep=nosleep
        )


# ---- fetch: ownership + content-checked skipping ---------------------------------------------


async def test_fetch_lists_non_recursively_and_dir_is_private(tmp_path: Path) -> None:
    s = FakeStore()
    seed(s, [0, 1])
    await fetch(s, tmp_path / "out")
    assert s.listed == [(BASE, False)]
    assert stat.S_IMODE((tmp_path / "out").stat().st_mode) == 0o700
    for name in ("manifest.json", "results.jsonl", "parts/000000.jsonl"):
        assert stat.S_IMODE((tmp_path / "out" / name).stat().st_mode) == 0o600
    assert not [p for p in (tmp_path / "out").iterdir() if p.name.startswith(".")]


async def test_fetch_refuses_other_tasks_dir(tmp_path: Path) -> None:
    s = FakeStore()
    seed(s, [0, 1])
    out = tmp_path / "out"
    await fetch(s, out)
    for field, value in (("created_at", "2026-02-02T00:00:00Z"), ("output_uri", "s3://x/y/")):
        with pytest.raises(CliError) as ei:
            await fetch(s, out, task_json(**{field: value}))
        assert ei.value.exit_code == EXIT_USAGE and "another task" in str(ei.value)
    manifest = json.loads((out / "manifest.json").read_text())
    assert manifest["team"] == "acme" and manifest["task_name"] == "job-1"
    assert manifest["output_uri"].endswith(BASE) and manifest["created_at"]


async def test_fetch_refuses_parts_without_manifest_and_bad_manifest(tmp_path: Path) -> None:
    s = FakeStore()
    seed(s, [0, 1])
    (tmp_path / "a" / "parts").mkdir(parents=True)
    with pytest.raises(CliError) as ei:
        await fetch(s, tmp_path / "a")
    assert ei.value.exit_code == EXIT_USAGE
    (tmp_path / "b").mkdir()
    (tmp_path / "b" / "manifest.json").write_text("{not json")
    with pytest.raises(CliError) as ei:
        await fetch(s, tmp_path / "b")
    assert ei.value.exit_code == EXIT_USAGE
    assert s.downloads == []


async def test_fetch_same_size_different_content_is_redownloaded(tmp_path: Path) -> None:
    s = FakeStore()
    seed(s, [0, 1])
    out = tmp_path / "out"
    await fetch(s, out)
    s.put(f"{BASE}000001.jsonl", b'{"o":9}\n')  # same size, new content (plain md5 ETag)
    r = await fetch(s, out)
    assert r.downloaded == 1
    assert (out / "parts" / "000001.jsonl").read_bytes() == b'{"o":9}\n'
    assert await fetch(s, out) is not None and s.downloads.count(f"{BASE}000001.jsonl") == 2


async def test_fetch_multipart_etag_uses_manifest_record(tmp_path: Path) -> None:
    s = FakeStore()
    seed(s, [0, 1])
    s.etags[f"{BASE}000000.jsonl"] = "abc-2"
    s.etags[f"{BASE}000001.jsonl"] = "def-2"
    out = tmp_path / "out"
    assert (await fetch(s, out)).downloaded == 2
    assert (await fetch(s, out)).downloaded == 0  # etag + sha256 match the manifest
    s.put(f"{BASE}000001.jsonl", b'{"o":9}\n', etag="ghi-2")  # remote changed
    assert (await fetch(s, out)).downloaded == 1
    (out / "parts" / "000000.jsonl").write_bytes(b'{"o":7}\n')  # local tampered, same size
    assert (await fetch(s, out)).downloaded == 1


async def test_fetch_wide_ids_and_noncanonical_names(tmp_path: Path) -> None:
    s = FakeStore()
    s.put(f"{BASE}1000000.jsonl", b"{}\n")
    s.put(f"{BASE}0000001.jsonl", b"{}\n")  # not canonical for id 1: ignored
    s.put(f"{BASE}000001.jsonl", b"{}\n")
    r = await fetch(s, tmp_path / "o", task_json(total=2), [sub(1), sub(1_000_000)])
    assert r.parts == 2 and sorted(s.downloads) == [f"{BASE}000001.jsonl", f"{BASE}1000000.jsonl"]


async def test_partial_manifest_lists_failed_subtasks(tmp_path: Path) -> None:
    s = FakeStore()
    seed(s, [0])
    r = await fetch(s, tmp_path / "o", task_json("PARTIAL"), [sub(0), sub(1, "FAILED")])
    doc = json.loads(r.manifest_path.read_text())
    assert doc["not_succeeded"] == [
        {"subtask_id": 1, "status": "FAILED", "record_count": 2, "attempt": 1, "error_code": "OOM"}
    ]
    assert doc["not_succeeded_subtasks"] == 1


async def test_merge_adds_missing_newline_and_streams(tmp_path: Path) -> None:
    s = FakeStore()
    s.put(f"{BASE}000000.jsonl", b'{"a":1}')
    s.put(f"{BASE}000001.jsonl", b'{"b":2}\n')
    r = await fetch(s, tmp_path / "o")
    assert r.results_path.read_bytes() == b'{"a":1}\n{"b":2}\n'


# ---- pagination ------------------------------------------------------------------------------


async def test_pagination_stops_at_total_and_detects_loops() -> None:
    calls: list[str | None] = []

    def many(req: httpx.Request) -> httpx.Response:
        calls.append(req.url.params.get("cursor"))
        return httpx.Response(200, json={"items": [sub(0), sub(1)], "next_cursor": "more"})

    async with client(many) as c:
        got = await flows._list_all_subtasks(c, T, 2, nosleep)
    assert len(got) == 2 and len(calls) == 1  # total reached: no further page

    def loop(req: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"items": [sub(0)], "next_cursor": "same"})

    async with client(loop) as c:
        with pytest.raises(CliError) as ei:
            await flows._list_all_subtasks(c, T, 50, nosleep)
    assert "repeated cursor" in str(ei.value) and ei.value.exit_code == EXIT_UNREACHABLE

    n = {"i": 0}

    def endless(req: httpx.Request) -> httpx.Response:
        n["i"] += 1
        return httpx.Response(200, json={"items": [], "next_cursor": f"c{n['i']}"})

    async with client(endless) as c:
        with pytest.raises(CliError):
            await flows._list_all_subtasks(c, T, 3, nosleep)
    assert n["i"] <= 3


async def test_list_subtasks_retries_transient_and_429() -> None:
    seq = iter([503, 429, 200])
    sleeps: list[float] = []

    async def sleep(s: float) -> None:
        sleeps.append(s)

    def h(req: httpx.Request) -> httpx.Response:
        code = next(seq)
        if code == 200:
            return httpx.Response(200, json={"items": [sub(0), sub(1)], "next_cursor": None})
        hdr = {"Retry-After": "17"} if code == 429 else {}
        return httpx.Response(code, json={"detail": {"code": "X", "message": "m"}}, headers=hdr)

    async with client(h) as c:
        assert len(await flows._list_all_subtasks(c, T, 2, sleep)) == 2
    assert sleeps[1] == 17.0

    async with client(lambda r: httpx.Response(404, json={})) as c:
        with pytest.raises(CliError) as ei:
            await flows._list_all_subtasks(c, T, 2, nosleep)
    assert ei.value.exit_code == EXIT_USAGE


# ---- upload resume ---------------------------------------------------------------------------


async def test_upload_resume_checks_content(tmp_path: Path) -> None:
    f = tmp_path / "a.jsonl"
    f.write_bytes(b'{"a":1}\n')
    s = FakeStore()
    await flows.upload_inputs(s, settings(), T, [f])
    key = f"{IN}0000-a.jsonl"
    assert s.meta[key]["sha256"]
    s.uploads.clear()
    await flows.upload_inputs(s, settings(), T, [f])  # sha256 metadata matches
    assert s.uploads == []
    s.put(key, b'{"a":2}\n')  # same size, other content, plain md5 ETag, no metadata
    s.meta.clear()
    with pytest.raises(CliError) as ei:
        await flows.upload_inputs(s, settings(), T, [f])
    assert ei.value.exit_code == EXIT_USAGE and "differs" in str(ei.value)
    s.put(key, b'{"a":1}\n')  # plain md5 ETag equal to local md5
    await flows.upload_inputs(s, settings(), T, [f])
    s.put(key, b'{"a":1}\n', etag="abc-2")  # multipart ETag, no metadata: cannot verify
    with pytest.raises(CliError):
        await flows.upload_inputs(s, settings(), T, [f])
    s.meta[key] = {"sha256": "0" * 64}  # metadata that disagrees
    with pytest.raises(CliError):
        await flows.upload_inputs(s, settings(), T, [f])


async def test_upload_rejects_more_than_9999_files(tmp_path: Path) -> None:
    f = tmp_path / "a.jsonl"
    with pytest.raises(CliError) as ei:
        await flows.upload_inputs(FakeStore(), settings(), T, [f] * 10_000)
    assert ei.value.exit_code == EXIT_USAGE


# ---- wait timeout ----------------------------------------------------------------------------


async def test_wait_timeout_sleeps_remaining_then_polls_once_more() -> None:
    now = {"t": 0.0}
    sleeps: list[float] = []
    polls = {"n": 0}

    async def sleep(s: float) -> None:
        sleeps.append(s)
        now["t"] += s

    def h(req: httpx.Request) -> httpx.Response:
        polls["n"] += 1
        return httpx.Response(200, json=task_json("RUNNING"))

    async with client(h) as c:
        with pytest.raises(CliError):
            await flows.wait_task(
                c, T, timeout_s=3, sleep=sleep, clock=lambda: now["t"], jitter=lambda: 1.0
            )
    assert sleeps[0] == 3.0
    assert sum(sleeps) == 3.0 and polls["n"] == 2  # final poll at the deadline


async def test_wait_final_poll_can_still_succeed() -> None:
    now = {"t": 0.0}
    seq = iter(["RUNNING", "SUCCEEDED"])

    async def sleep(s: float) -> None:
        now["t"] += s

    def h(req: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=task_json(next(seq)))

    async with client(h) as c:
        out = await flows.wait_task(
            c, T, timeout_s=1, sleep=sleep, clock=lambda: now["t"], jitter=lambda: 1.0
        )
    assert out.status == "SUCCEEDED"


# ---- main: Ctrl-C, errors, sanitising --------------------------------------------------------


def test_clean_strips_control_chars() -> None:
    assert clean("a\x1b[31mb\x00\x9bc\nd") == "a[31mbcd"


def test_run_interrupt_hint_only_after_submit(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(main, "abort_transfers", lambda: False)

    async def boom() -> int:
        raise KeyboardInterrupt

    def go(submitted: bool) -> str:
        err: list[str] = []
        monkeypatch.setattr(main, "_echo_err", err.append)
        with pytest.raises(typer.Exit) as ei:
            main._run(boom(), "job-1", lambda: submitted)
        assert ei.value.exit_code == 130
        return err[0]

    assert "keeps running" in go(True)
    assert "keeps running" not in go(False)


def test_run_interrupt_with_inflight_transfer_exits_hard(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(main, "abort_transfers", lambda: True)
    codes: list[int] = []

    def fake_exit(code: int) -> None:
        codes.append(code)
        raise SystemExit(code)

    monkeypatch.setattr(os, "_exit", fake_exit)

    async def boom() -> int:
        raise KeyboardInterrupt

    with pytest.raises(SystemExit):
        main._run(boom(), "job-1")
    assert codes == [130]


def test_abort_transfers_reports_running_thread() -> None:
    assert store.abort_transfers() is False
    started, release = threading.Event(), threading.Event()

    def work() -> None:
        started.set()
        release.wait(5)

    fut = store._get_pool().submit(store._tracked, work)
    assert started.wait(5)
    assert store.abort_transfers() is True
    release.set()
    fut.result(5)
    assert store.abort_transfers() is False


def test_run_maps_oserror_and_validation_error() -> None:
    async def disk() -> int:
        raise PermissionError("secret path")

    with pytest.raises(typer.Exit) as ei:
        main._run(disk())
    assert ei.value.exit_code == 2

    async def bad() -> int:
        TaskOut.model_validate({"nope": 1})
        return 0

    with pytest.raises(typer.Exit) as ei:
        main._run(bad())
    assert ei.value.exit_code == 2


def ok(body: dict[str, Any]) -> httpx.Response:
    return httpx.Response(200, json=body)


def test_status_error_text_is_sanitised(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MINION_CLI_SERVER", "http://localhost:8080")
    monkeypatch.setenv("MINION_CLI_TEAM", "acme")
    monkeypatch.setenv("MINION_API_KEY", "k")
    body = task_json("FAILED", error="bad\x1b]0;pwn\x07thing")
    monkeypatch.setattr(
        main,
        "make_client",
        lambda server, key: UserClient(
            server,
            SecretStr(key),
            transport=httpx.MockTransport(lambda r: httpx.Response(200, json=body)),
        ),
    )
    r = CliRunner().invoke(main.app, ["status", "job-1"])
    assert "\x1b" not in r.output and "\x07" not in r.output and "pwn" in r.output


def test_typer_hides_locals() -> None:
    assert main.app.pretty_exceptions_show_locals is False


def test_endpoint_from_environment_must_be_secure(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://evil.example")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "a")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "b")
    with pytest.raises(CliError) as ei:
        store.S3Store(CliSettings())
    assert ei.value.exit_code == EXIT_USAGE
    monkeypatch.setenv("AWS_ENDPOINT_URL", "https://s3.example.com")
    store.S3Store(CliSettings())
