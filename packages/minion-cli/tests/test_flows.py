from __future__ import annotations

import json
import sys
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import httpx
import pytest
from minion_cli import flows
from minion_cli.errors import EXIT_UNREACHABLE, EXIT_USAGE, CliError
from minion_cli.flows import Target
from minion_cli.settings import CliSettings
from minion_common.client import ApiError, UserClient
from minion_common.enums import TaskType
from minion_common.schemas import TaskOut
from pydantic import SecretStr

sys.path.insert(0, str(Path(__file__).parent))  # tests/fakes.py (importlib import mode)
from fakes import FakeStore

SENTINEL = "SENTINEL-PAYLOAD-9f3a"
T = Target("acme", "default", "job-1")


def settings() -> CliSettings:
    return CliSettings(server="http://localhost:8080", team="acme")


def task_json(status: str = "RUNNING", **counts: int) -> dict[str, Any]:
    return {
        "team": "acme",
        "project": "default",
        "task_name": "job-1",
        "type": "embedding",
        "model": "m",
        "input_uri": "s3://minion/inputs/acme/default/job-1/",
        "output_uri": "s3://minion/outputs/acme/default/job-1/",
        "records_per_subtask": 500,
        "priority": 0,
        "status": status,
        "created_by": "me",
        "created_at": "2026-01-01T00:00:00Z",
        "counts": {"total": 3, **counts},
    }


def make_client(handler: Callable[[httpx.Request], httpx.Response]) -> UserClient:
    return UserClient(
        "http://localhost:8080", SecretStr("k"), transport=httpx.MockTransport(handler)
    )


def err(status: int, **headers: str) -> httpx.Response:
    return httpx.Response(status, json={"detail": {"code": "X", "message": "m"}}, headers=headers)


async def test_upload_inputs_order_and_resume(tmp_path: Path) -> None:
    a, b = tmp_path / "b.jsonl", tmp_path / "a x.jsonl"
    a.write_text(f'{{"t":"{SENTINEL}"}}\n')
    b.write_text('{"t":"2"}\n')
    store = FakeStore()
    uri = await flows.upload_inputs(store, settings(), T, [a, b])
    assert uri == "s3://minion/inputs/acme/default/job-1/"
    assert store.uploads == [
        "inputs/acme/default/job-1/0000-b.jsonl",
        "inputs/acme/default/job-1/0001-a_x.jsonl",
    ]
    store.uploads.clear()
    await flows.upload_inputs(store, settings(), T, [a, b])  # resume: nothing missing
    assert store.uploads == []
    del store.objects["inputs/acme/default/job-1/0001-a_x.jsonl"]
    await flows.upload_inputs(store, settings(), T, [a, b])
    assert store.uploads == ["inputs/acme/default/job-1/0001-a_x.jsonl"]


async def test_upload_foreign_object_aborts(tmp_path: Path) -> None:
    f = tmp_path / "a.jsonl"
    f.write_text("{}\n")
    store = FakeStore()
    store.objects["inputs/acme/default/job-1/other"] = b"x"
    with pytest.raises(CliError) as ei:
        await flows.upload_inputs(store, settings(), T, [f])
    assert ei.value.exit_code == EXIT_USAGE
    assert store.uploads == []


async def test_upload_size_mismatch_is_foreign(tmp_path: Path) -> None:
    f = tmp_path / "a.jsonl"
    f.write_text("{}\n")
    store = FakeStore()
    store.objects["inputs/acme/default/job-1/0000-a.jsonl"] = b"different size"
    with pytest.raises(CliError):
        await flows.upload_inputs(store, settings(), T, [f])


async def test_upload_missing_file(tmp_path: Path) -> None:
    with pytest.raises(CliError) as ei:
        await flows.upload_inputs(FakeStore(), settings(), T, [tmp_path / "nope"])
    assert ei.value.exit_code == EXIT_USAGE


def test_default_name() -> None:
    now = datetime(2026, 3, 4, 5, 6, 7, tzinfo=UTC)
    assert flows.default_name(Path("My Data!.jsonl"), now) == "my-data-20260304050607"
    assert flows.default_name(Path("!!!.jsonl"), now) == "task-20260304050607"


async def test_submit_creates_task(tmp_path: Path) -> None:
    f = tmp_path / "a.jsonl"
    f.write_text("{}\n")
    seen: dict[str, Any] = {}

    def handler(req: httpx.Request) -> httpx.Response:
        if req.method == "GET":
            return err(404)
        seen["body"] = json.loads(req.content)
        return httpx.Response(201, json=task_json("SPLITTING"))

    async with make_client(handler) as c:
        out = await flows.submit_task(
            c, FakeStore(), settings(), T, [f], type=TaskType.EMBEDDING, model="m"
        )
    assert out.task_name == "job-1"
    assert seen["body"]["input_uri"] == "s3://minion/inputs/acme/default/job-1/"
    assert seen["body"]["output_uri"] == "s3://minion/outputs/acme/default/job-1/"


async def test_submit_existing_task_exit_2(tmp_path: Path) -> None:
    f = tmp_path / "a.jsonl"
    f.write_text("{}\n")
    store = FakeStore()
    async with make_client(lambda r: httpx.Response(200, json=task_json())) as c:
        with pytest.raises(CliError) as ei:
            await flows.submit_task(
                c, store, settings(), T, [f], type=TaskType.EMBEDDING, model="m"
            )
    assert ei.value.exit_code == EXIT_USAGE and "fetch" in str(ei.value)
    assert store.uploads == []


async def test_submit_server_down_exit_4(tmp_path: Path) -> None:
    def handler(req: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("down")

    f = tmp_path / "a.jsonl"
    f.write_text("{}\n")
    async with make_client(handler) as c:
        with pytest.raises(CliError) as ei:
            await flows.submit_task(
                c, FakeStore(), settings(), T, [f], type=TaskType.EMBEDDING, model="m"
            )
    assert ei.value.exit_code == EXIT_UNREACHABLE


class Clock:
    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def __call__(self) -> float:
        return self.now

    async def sleep(self, s: float) -> None:
        self.sleeps.append(s)
        self.now += s


async def wait(
    handler: Callable[[httpx.Request], httpx.Response], **kw: Any
) -> tuple[TaskOut, Clock]:
    clock = Clock()
    async with make_client(handler) as c:
        out = await flows.wait_task(c, T, sleep=clock.sleep, clock=clock, jitter=lambda: 1.0, **kw)
    return out, clock


async def test_wait_backoff_and_progress() -> None:
    seq = iter(
        [task_json("RUNNING", pending=3)] * 3
        + [task_json("RUNNING", succeeded=1)] * 2
        + [task_json("SUCCEEDED", succeeded=3)]
    )
    seen: list[str] = []
    out, clock = await wait(
        lambda r: httpx.Response(200, json=next(seq)), on_progress=lambda t: seen.append(t.status)
    )
    assert out.status == "SUCCEEDED"
    assert clock.sleeps == [3.0, 4.5, 6.75, 3.0, 4.5]  # resets to the start delay on progress
    assert len(seen) == 3  # printed only on change


async def test_wait_retries_transient_then_ok() -> None:
    calls = {"n": 0}

    def handler(req: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            return err(503)
        if calls["n"] == 2:
            raise httpx.ReadTimeout("t")
        return httpx.Response(200, json=task_json("FAILED"))

    out, _ = await wait(handler)
    assert out.status == "FAILED"


async def test_wait_429_honours_retry_after() -> None:
    calls = {"n": 0}

    def handler(req: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            return err(429, **{"Retry-After": "17"})
        return httpx.Response(200, json=task_json("SUCCEEDED"))

    _, clock = await wait(handler)
    assert clock.sleeps == [17.0]


async def test_wait_gives_up_after_10_transient() -> None:
    with pytest.raises(CliError) as ei:
        await wait(lambda r: err(502))
    assert ei.value.exit_code == EXIT_UNREACHABLE


@pytest.mark.parametrize("status", [401, 403, 404])
async def test_wait_fatal_statuses(status: int) -> None:
    with pytest.raises(CliError) as ei:
        await wait(lambda r: err(status))
    assert ei.value.exit_code == EXIT_USAGE


async def test_wait_timeout() -> None:
    with pytest.raises(CliError) as ei:
        await wait(lambda r: httpx.Response(200, json=task_json("RUNNING")), timeout_s=5)
    assert ei.value.exit_code == EXIT_UNREACHABLE and "timed out" in str(ei.value)


def subtasks_handler(statuses: dict[int, str]) -> Callable[[httpx.Request], httpx.Response]:
    items = [
        {
            "subtask_id": i,
            "status": s,
            "attempt": 1,
            "max_attempts": 3,
            "record_count": 2,
            "updated_at": "2026-01-01T00:00:00Z",
        }
        for i, s in sorted(statuses.items(), reverse=True)  # server order must not matter
    ]
    first, second = items[: len(items) // 2], items[len(items) // 2 :]

    def handler(req: httpx.Request) -> httpx.Response:
        if req.url.params.get("cursor") == "p2":
            return httpx.Response(200, json={"items": second, "next_cursor": None})
        return httpx.Response(200, json={"items": first, "next_cursor": "p2"})

    return handler


def seed_outputs(store: FakeStore, ids: list[int]) -> None:
    base = "outputs/acme/default/job-1/"
    for i in ids:
        store.objects[f"{base}{i:06d}.jsonl"] = f'{{"o":"{SENTINEL}-{i}"}}\n{{"o":{i}}}\n'.encode()
    store.objects[f"{base}_attempts/000001/1.jsonl"] = b"junk\n"


async def test_fetch_merges_in_id_order_idempotent(tmp_path: Path) -> None:
    store = FakeStore()
    seed_outputs(store, [0, 1, 2, 10])
    task = TaskOut.model_validate(task_json("PARTIAL", succeeded=4))
    statuses = {0: "SUCCEEDED", 1: "SUCCEEDED", 2: "FAILED", 10: "SUCCEEDED"}
    async with make_client(subtasks_handler(statuses)) as c:
        r = await flows.fetch_results(c, store, settings(), T, task, tmp_path / "out")
        assert (r.parts, r.downloaded, r.records, r.not_succeeded) == (3, 3, 6, 1)
        lines = r.results_path.read_text().splitlines()
        assert [json.loads(x)["o"] for x in lines[1::2]] == [0, 1, 10]
        assert not any("_attempts" in k for k in store.downloads)
        again = await flows.fetch_results(c, store, settings(), T, task, tmp_path / "out")
        assert again.downloaded == 0
        assert r.results_path.read_bytes() == again.results_path.read_bytes()
        (tmp_path / "out" / "parts" / "000001.jsonl").write_text("x")  # wrong size -> refetched
        third = await flows.fetch_results(c, store, settings(), T, task, tmp_path / "out")
        assert third.downloaded == 1
    manifest = json.loads(r.manifest_path.read_text())
    assert manifest["status"] == "PARTIAL"
    assert [p["subtask_id"] for p in manifest["parts"]] == [0, 1, 10]
    assert manifest["not_succeeded_subtasks"] == 1


async def test_fetch_missing_object(tmp_path: Path) -> None:
    store = FakeStore()
    task = TaskOut.model_validate(task_json("SUCCEEDED"))
    async with make_client(subtasks_handler({0: "SUCCEEDED"})) as c:
        with pytest.raises(CliError) as ei:
            await flows.fetch_results(c, store, settings(), T, task, tmp_path / "out")
    assert ei.value.exit_code == EXIT_USAGE


async def test_errors_never_carry_payload(tmp_path: Path) -> None:
    f = tmp_path / "a.jsonl"
    f.write_text(f'{{"t":"{SENTINEL}"}}\n')
    store = FakeStore()
    store.objects["inputs/acme/default/job-1/0000-a.jsonl"] = b"zz"
    with pytest.raises(CliError) as ei:
        await flows.upload_inputs(store, settings(), T, [f])
    assert SENTINEL not in str(ei.value)


async def test_submit_429_live_task_cap_is_usage_error_and_not_retried(tmp_path: Path) -> None:
    f = tmp_path / "a.jsonl"
    f.write_text("{}\n")

    class Capped:
        creates = 0

        async def get_task(self, *a: Any) -> Any:
            raise ApiError(404, "NOT_FOUND", "no")

        async def create_task(self, req: Any) -> Any:
            Capped.creates += 1
            raise ApiError(429, "TOO_MANY_LIVE_TASKS", "team is at its live-task limit (500)")

    with pytest.raises(CliError) as ei:
        await flows.submit_task(
            cast("UserClient", Capped()),
            FakeStore(),
            settings(),
            T,
            [f],
            type=TaskType.EMBEDDING,
            model="m",
        )
    assert ei.value.exit_code == EXIT_USAGE and "TOO_MANY_LIVE_TASKS" in str(ei.value)
    assert Capped.creates == 1
