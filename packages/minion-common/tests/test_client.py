import json
import uuid
from collections.abc import Callable
from datetime import UTC, datetime

import httpx
import pytest
from minion_common.client import (
    ApiError,
    ExecutorClient,
    StaleAcquirementError,
    UserClient,
    require_secure_url,
)
from minion_common.enums import CapacityProfile, Engine
from minion_common.schemas import (
    AcquireRequest,
    Budget,
    CompleteRequest,
    FailRequest,
    HeartbeatRequest,
    HeartbeatResponse,
    PreferenceRequest,
    PresenceRequest,
    RegisterRequest,
    TaskCreate,
)
from pydantic import SecretStr

BUDGET = Budget(profile=CapacityProfile.LIGHT, memory_gb=4, gpu_duty_cycle=0.25, max_batch_size=4)
REQ = AcquireRequest(engines=[Engine.MLX], budget=BUDGET)
AID = uuid.UUID("11111111-1111-1111-1111-111111111111")
ASSIGNMENT = {
    "acquirement_id": str(AID),
    "attempt": 1,
    "team": "ml",
    "project": "p",
    "task_name": "t",
    "subtask_id": 3,
    "type": "embedding",
    "model": {
        "model_id": "fake-embed",
        "type": "embedding",
        "engine": "mlx",
        "ref": "o/m",
        "revision": "a" * 40,
        "memory_gb": 1,
    },
    "params": {},
    "input_url": "http://s3/in",
    "byte_start": 0,
    "byte_end": 10,
    "record_count": 2,
    "output_url": "http://s3/out",
    "expires_at": datetime.now(UTC).isoformat(),
}


def _client(handler: Callable[[httpx.Request], httpx.Response]) -> ExecutorClient:
    return ExecutorClient(
        "http://localhost:8080",
        SecretStr("tok123"),
        transport=httpx.MockTransport(handler),
    )


@pytest.mark.parametrize(
    "url",
    ["https://minions.example.com", "http://localhost:8080", "http://127.0.0.1", "http://[::1]:80"],
)
def test_secure_urls_ok(url: str) -> None:
    assert require_secure_url(url) == url


@pytest.mark.parametrize(
    "url", ["http://example.com", "ftp://localhost", "localhost:8080", "http://localhost.evil.com"]
)
def test_insecure_urls_rejected(url: str) -> None:
    with pytest.raises(ValueError):
        require_secure_url(url)


def test_token_not_in_repr() -> None:
    assert "tok123" not in repr(SecretStr("tok123"))


async def test_acquire_200_and_204() -> None:
    seen: list[httpx.Request] = []

    def handler(req: httpx.Request) -> httpx.Response:
        seen.append(req)
        if len(seen) == 1:
            return httpx.Response(200, json=ASSIGNMENT)
        return httpx.Response(204)

    async with _client(handler) as c:
        a = await c.acquire(REQ)
        assert a is not None and a.acquirement_id == AID
        assert await c.acquire(REQ) is None
    assert seen[0].url.path == "/api/v1/executor/acquire"
    assert seen[0].headers["authorization"] == "Bearer tok123"
    assert json.loads(seen[0].content)["engines"] == ["mlx"]


async def test_presence_posts_budget() -> None:
    seen: list[httpx.Request] = []

    def handler(req: httpx.Request) -> httpx.Response:
        seen.append(req)
        return httpx.Response(204)

    async with _client(handler) as c:
        await c.presence(PresenceRequest(budget=BUDGET, paused_reason="thermal"))
    assert seen[0].method == "POST"
    assert seen[0].url.path == "/api/v1/executor/presence"
    assert json.loads(seen[0].content)["paused_reason"] == "thermal"


async def test_register_and_preference() -> None:
    def handler(req: httpx.Request) -> httpx.Response:
        if req.url.path.endswith("register"):
            return httpx.Response(200, json={"executor_id": str(AID)})
        assert req.method == "PUT"
        return httpx.Response(204)

    async with _client(handler) as c:
        r = await c.register(
            RegisterRequest(
                hostname="h",
                chip="M3",
                total_memory_gb=16,
                os_version="15",
                engines=[Engine.MLX],
                profile=CapacityProfile.LIGHT,
                budget=BUDGET,
            )
        )
        assert r.executor_id == AID
        await c.set_preference(PreferenceRequest(team="ml"))


@pytest.mark.parametrize("op", ["complete", "fail", "release"])
async def test_409_raises_stale(op: str) -> None:
    def handler(req: httpx.Request) -> httpx.Response:
        return httpx.Response(
            409, json={"detail": {"code": "STALE_ACQUIREMENT", "message": "gone"}}
        )

    async with _client(handler) as c:
        with pytest.raises(StaleAcquirementError) as ei:
            if op == "complete":
                await c.complete(AID, CompleteRequest(record_count=1, byte_size=1, sha256="a" * 64))
            elif op == "fail":
                await c.fail(AID, FailRequest(retryable=True, code="X", message="m"))
            else:
                await c.release(AID)
    assert ei.value.code == "STALE_ACQUIREMENT"
    assert ei.value.status_code == 409


async def test_other_errors_raise_api_error() -> None:
    async with _client(lambda r: httpx.Response(401, text="nope")) as c:
        with pytest.raises(ApiError) as ei:
            await c.release(AID)
    assert not isinstance(ei.value, StaleAcquirementError)
    assert ei.value.status_code == 401


async def test_user_client_tasks() -> None:
    task = {
        "team": "ml",
        "project": "p",
        "task_name": "t",
        "type": "embedding",
        "model": "fake-embed",
        "input_uri": "s3://b/inputs/",
        "output_uri": "s3://b/outputs/",
        "records_per_subtask": 500,
        "priority": 0,
        "status": "SPLITTING",
        "created_by": "alice",
        "created_at": datetime.now(UTC).isoformat(),
    }

    def handler(req: httpx.Request) -> httpx.Response:
        return httpx.Response(202 if req.method == "POST" else 200, json=task)

    async with UserClient(
        "http://localhost", SecretStr("k"), transport=httpx.MockTransport(handler)
    ) as u:
        out = await u.create_task(
            TaskCreate(
                team="ml",
                project="p",
                task_name="t",
                type="embedding",
                model="fake-embed",
                input_uri="s3://b/inputs/",
                output_uri="s3://b/outputs/",
            )
        )
        assert out.created_by == "alice"
        assert (await u.get_task("ml", "p", "t")).counts.total == 0


def test_insecure_client_rejected() -> None:
    with pytest.raises(ValueError):
        ExecutorClient("http://example.com", SecretStr("t"))


async def test_other_409_is_plain_api_error() -> None:
    def handler(req: httpx.Request) -> httpx.Response:
        return httpx.Response(409, json={"detail": {"code": "CONFLICT", "message": "x"}})

    async with _client(handler) as c:
        with pytest.raises(ApiError) as ei:
            await c.release(AID)
    assert not isinstance(ei.value, StaleAcquirementError)
    assert ei.value.code == "CONFLICT"


@pytest.mark.parametrize("bad", ["../x", "a/b", "A", "a?b", ""])
async def test_get_task_rejects_bad_segments(bad: str) -> None:
    called: list[httpx.Request] = []

    def handler(req: httpx.Request) -> httpx.Response:
        called.append(req)
        return httpx.Response(200)

    async with UserClient(
        "http://localhost", SecretStr("k"), transport=httpx.MockTransport(handler)
    ) as u:
        with pytest.raises(ValueError):
            await u.get_task("ml", bad, "t")
    assert not called


async def test_heartbeat_ok_and_stale() -> None:
    from minion_common.schemas import HeartbeatRequest, Progress

    seen: list[httpx.Request] = []

    def handler(req: httpx.Request) -> httpx.Response:
        seen.append(req)
        if len(seen) == 1:
            return httpx.Response(
                200,
                json={
                    "cancel": True,
                    "output_url": "http://s3/new",
                    "expires_at": datetime.now(UTC).isoformat(),
                },
            )
        return httpx.Response(
            409, json={"detail": {"code": "STALE_ACQUIREMENT", "message": "gone"}}
        )

    req = HeartbeatRequest(progress=Progress(records_done=1, records_total=2), budget=BUDGET)
    async with _client(handler) as c:
        out = await c.heartbeat(AID, req)
        assert out.cancel is True
        assert out.output_url == "http://s3/new"
        with pytest.raises(StaleAcquirementError):
            await c.heartbeat(AID, HeartbeatRequest())
    assert seen[0].url.path.endswith(f"/subtasks/{AID}/heartbeat")
    assert json.loads(seen[0].content)["progress"] == {"records_done": 1, "records_total": 2}


async def test_cancel_task() -> None:
    task = {
        "team": "ml",
        "project": "p",
        "task_name": "t",
        "type": "embedding",
        "model": "fake-embed",
        "input_uri": "s3://b/inputs/",
        "output_uri": "s3://b/outputs/",
        "params": {},
        "records_per_subtask": 500,
        "priority": 0,
        "status": "CANCELLED",
        "created_by": "alice",
        "created_at": datetime.now(UTC).isoformat(),
    }
    calls: list[httpx.Request] = []

    def handler(req: httpx.Request) -> httpx.Response:
        calls.append(req)
        if len(calls) == 1:
            return httpx.Response(200, json=task)
        return httpx.Response(409, json={"detail": {"code": "TASK_FINISHED", "message": "done"}})

    async with UserClient(
        "http://localhost", SecretStr("k"), transport=httpx.MockTransport(handler)
    ) as u:
        out = await u.cancel_task("ml", "p", "t")
        assert out.status.value == "CANCELLED"
        with pytest.raises(ApiError) as ei:
            await u.cancel_task("ml", "p", "t")
        with pytest.raises(ValueError):
            await u.cancel_task("ml", "../x", "t")
    assert ei.value.code == "TASK_FINISHED"
    assert not isinstance(ei.value, StaleAcquirementError)
    assert calls[0].method == "POST"
    assert calls[0].url.path.endswith("/tasks/ml/p/t/cancel")
    assert len(calls) == 2


@pytest.mark.parametrize(
    "raw,expected",
    [("30", 30.0), (" 7 ", 7.0), ("\u00b2", None), ("-5", None), ("1.5", None),
     ("Wed, 21 Oct 2026 07:28:00 GMT", None), ("1234567", None), ("", None)],
)  # fmt: skip
async def test_retry_after_parsing_is_strict_ascii_seconds(
    raw: str, expected: float | None
) -> None:
    def handler(req: httpx.Request) -> httpx.Response:
        return httpx.Response(
            429, headers=[(b"Retry-After", raw.encode("latin-1"))], json={"detail": "slow"}
        )

    async with _client(handler) as c:
        with pytest.raises(ApiError) as ei:
            await c.heartbeat(AID, HeartbeatRequest())
    assert ei.value.retry_after == expected


def test_heartbeat_response_requires_aware_expires_at() -> None:
    with pytest.raises(ValueError):
        HeartbeatResponse.model_validate(
            {"cancel": False, "output_url": "http://s3/out", "expires_at": "2026-01-01T00:00:00"}
        )


@pytest.mark.parametrize(
    "body",
    [
        {"detail": {"code": "lowercase", "message": "m"}},
        {"detail": {"code": "A" * 65, "message": "m"}},
        {"detail": {"code": "BAD\x1b[31m", "message": "m"}},
        {"detail": {"code": "BAD\n", "message": "m"}},
        {"detail": "Not Found"},
        {"detail": [{"loc": ["body"], "msg": "field required", "type": "missing"}]},
        {"detail": {"message": "no code"}},
        ["not", "a", "dict"],
    ],
)
def test_malformed_error_bodies_fall_back_to_http_error(body: object) -> None:
    from minion_common.client import _error_from

    err = _error_from(httpx.Response(422, json=body))
    assert err.status_code == 422 and err.code == "HTTP_ERROR"


def test_error_body_ignores_unknown_fields() -> None:
    from minion_common.client import _error_from

    err = _error_from(
        httpx.Response(403, json={"detail": {"code": "FORBIDDEN", "message": "m", "x": 1}})
    )
    assert err.code == "FORBIDDEN"
