"""`minion` CLI end to end: live uvicorn server, SeaweedFS (testcontainers), fake-engine executors.

The CLI runs through typer's CliRunner in a worker thread so the server and executor loops keep
running on the test's event loop.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
from collections.abc import AsyncIterator, Sequence
from pathlib import Path

import pytest
from minion_cli.main import app
from minion_common.client import ExecutorClient
from minion_common.enums import Engine
from minion_common.schemas import Budget
from minion_executor.loop import ExecutorLoop
from minion_executor.worker.process import WorkerHandle
from pydantic import SecretStr
from typer.testing import CliRunner, Result

from conftest import TEAM, Live

pytestmark = pytest.mark.integration

BUDGET = Budget.model_validate(
    {"profile": "balanced", "memory_gb": 8, "gpu_duty_cycle": 0.5, "max_batch_size": 7}
)
FAST = (0.02, 0.1)


@pytest.fixture(autouse=True)
def cli_env(live: Live, monkeypatch: pytest.MonkeyPatch) -> None:
    s3 = live.s3
    for k, v in {
        "MINION_API_KEY": live.api_key,
        "MINION_CLI_SERVER": live.url,
        "MINION_CLI_TEAM": TEAM,
        "MINION_CLI_PROJECT": "p",
        "MINION_CLI_S3_ENDPOINT_URL": s3.endpoint_url,
        "MINION_CLI_S3_BUCKET": s3.bucket,
        "MINION_CLI_S3_REGION": s3.region,
        "MINION_CLI_S3_ACCESS_KEY": s3.access_key,
        "MINION_CLI_S3_SECRET_KEY": s3.secret_key,
    }.items():
        monkeypatch.setenv(k, v)


async def minion(args: Sequence[str]) -> Result:
    return await asyncio.to_thread(lambda: CliRunner().invoke(app, list(args)))


@contextlib.asynccontextmanager
async def executors(live: Live, n: int, tmp: Path) -> AsyncIterator[None]:
    await asyncio.to_thread(tmp.mkdir, exist_ok=True)
    clients = [ExecutorClient(live.url, SecretStr(await live.token(f"o{i}"))) for i in range(n)]
    loops = [
        ExecutorLoop(
            c,
            WorkerHandle([Engine.FAKE]),
            BUDGET,
            [Engine.FAKE],
            backoff=FAST,
            tmp_root=tmp,
            allow_http_loopback=True,
        )
        for c in clients
    ]
    tasks = [asyncio.create_task(lp.run()) for lp in loops]
    try:
        yield
    finally:
        for lp in loops:
            lp.stop()
        await asyncio.wait_for(asyncio.gather(*tasks), 30)
        for c in clients:
            await c.aclose()


def write_jsonl(path: Path, prefix: str, n: int) -> Path:
    path.write_text("".join(f'{{"id":"{prefix}{i}","text":"t {prefix}{i}"}}\n' for i in range(n)))
    return path


def result_ids(out: Path) -> list[str]:
    return [json.loads(line)["id"] for line in (out / "results.jsonl").read_text().splitlines()]


async def test_submit_two_files_results_in_input_order(live: Live, tmp_path: Path) -> None:
    """AC #1/#3: two files, 11 subtasks, two executors; results.jsonl keeps argv order."""
    a = write_jsonl(tmp_path / "a.jsonl", "a", 130)
    b = write_jsonl(tmp_path / "b.jsonl", "b", 107)
    out = tmp_path / "out"
    async with executors(live, 2, tmp_path / "scratch"):
        res = await minion(
            [
                *("submit", str(a), str(b), "--type", "embedding", "--model", "fake-embed"),
                *("--name", "cli-ok", "--records-per-subtask", "25", "-o", str(out)),
            ]
        )
    assert res.exit_code == 0, res.output
    assert await live.task_status("cli-ok") == "SUCCEEDED"
    ids = result_ids(out)
    assert ids == [f"a{i}" for i in range(130)] + [f"b{i}" for i in range(107)]
    assert len(set(ids)) == len(ids)
    assert len(list((out / "parts").glob("*.jsonl"))) == 11

    # (b) re-fetch: nothing is downloaded again and the merged file is byte-identical.
    parts = sorted((out / "parts").glob("*.jsonl"))
    before = {p.name: p.stat().st_mtime_ns for p in parts}
    merged = (out / "results.jsonl").read_bytes()
    again = await minion(["fetch", "cli-ok", "-o", str(out)])
    assert again.exit_code == 0, again.output
    assert "(0 downloaded)" in again.output
    assert {p.name: p.stat().st_mtime_ns for p in parts} == before
    assert (out / "results.jsonl").read_bytes() == merged


async def test_no_wait_cancel_then_fetch_wait_exits_1(live: Live, tmp_path: Path) -> None:
    f = write_jsonl(tmp_path / "c.jsonl", "c", 30)
    res = await minion(
        [
            *("submit", str(f), "--type", "embedding", "--model", "fake-embed"),
            *("--name", "cli-cancel", "--records-per-subtask", "10", "--no-wait"),
        ]
    )
    assert res.exit_code == 0, res.output
    assert (await minion(["cancel", "cli-cancel"])).exit_code == 0
    got = await minion(["fetch", "cli-cancel", "--wait", "-o", str(tmp_path / "o")])
    assert got.exit_code == 1, got.output
    assert (await minion(["status", "cli-cancel"])).exit_code == 1


async def test_invalid_jsonl_fails_with_exit_1(live: Live, tmp_path: Path) -> None:
    f = tmp_path / "bad.jsonl"
    f.write_text('{"id":"ok0","text":"x"}\nthis is not json\n')
    async with executors(live, 1, tmp_path / "scratch"):
        res = await minion(
            [
                *("submit", str(f), "--type", "embedding", "--model", "fake-embed"),
                *("--name", "cli-bad", "-o", str(tmp_path / "o")),
            ]
        )
    assert res.exit_code == 1, res.output
    assert await live.task_status("cli-bad") == "FAILED"
    assert "not json" not in res.output


async def test_upload_inputs_resume_is_content_checked_over_multipart(
    live: Live, tmp_path: Path
) -> None:
    """Interrupt after the upload (before create), resubmit: nothing is uploaded twice."""
    from minion_cli import flows
    from minion_cli.settings import CliSettings
    from minion_cli.store import make_store

    big = tmp_path / "big.jsonl"
    big.write_bytes(os.urandom(9 * 1024 * 1024) + b"\n")  # > 8 MiB: multipart, non-md5 ETag
    small = write_jsonl(tmp_path / "s.jsonl", "s", 5)
    settings = CliSettings()
    target = flows.Target(TEAM, "p", "cli-resume")
    store = make_store(settings)
    uploads: list[str] = []
    real = store.upload_file

    async def counting(path: Path, key: str, *, sha256: str) -> None:
        uploads.append(key)
        await real(path, key, sha256=sha256)

    store.upload_file = counting  # type: ignore[method-assign]
    uri = await flows.upload_inputs(store, settings, target, [big, small])
    assert len(uploads) == 2
    head = await store.head(f"{target.input_prefix(settings)}0000-big.jsonl")
    assert head is not None and head.sha256 and head.etag and "-" in head.etag
    assert await flows.upload_inputs(store, settings, target, [big, small]) == uri
    assert len(uploads) == 2  # sha256 metadata round trip: no re-upload
    same_size = small.read_text().replace("s0", "z0", 1)
    small.write_text(same_size)
    with pytest.raises(Exception, match="differs"):
        await flows.upload_inputs(store, settings, target, [big, small])


async def test_fetch_ignores_planted_attempt_objects(live: Live, tmp_path: Path) -> None:
    f = write_jsonl(tmp_path / "a.jsonl", "a", 20)
    async with executors(live, 1, tmp_path / "scratch"):
        res = await minion(
            [
                *("submit", str(f), "--type", "embedding", "--model", "fake-embed"),
                *("--name", "cli-planted", "--records-per-subtask", "10"),
                *("-o", str(tmp_path / "first")),
            ]
        )
    assert res.exit_code == 0, res.output
    base = f"outputs/{TEAM}/p/cli-planted/"
    live.s3.client.put_object(
        Bucket=live.s3.bucket, Key=f"{base}_attempts/x/000000.jsonl", Body=b'{"id":"evil"}\n'
    )
    live.s3.client.put_object(
        Bucket=live.s3.bucket, Key=f"{base}_attempts/000000.jsonl", Body=b'{"id":"evil"}\n'
    )
    again = await minion(["fetch", "cli-planted", "-o", str(tmp_path / "second")])
    assert again.exit_code == 0, again.output
    assert "evil" not in (tmp_path / "second" / "results.jsonl").read_text()
    assert result_ids(tmp_path / "second") == result_ids(tmp_path / "first")
    assert len(list((tmp_path / "second" / "parts").glob("*.jsonl"))) == 2
