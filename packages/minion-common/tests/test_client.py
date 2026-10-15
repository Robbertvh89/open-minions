import json
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
    PreferenceRequest,
    RegisterRequest,
    TaskCreate,
)
from pydantic import SecretStr

BUDGET = Budget(profile=CapacityProfile.LIGHT, memory_gb=4, gpu_duty_cycle=0.25, max_batch_size=4)
REQ = AcquireRequest(engines=[Engine.MLX], budget=BUDGET)
ASSIGNMENT = {
    "acquirement_id": 7,
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
        "ref": "r",
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
        assert a is not None and a.acquirement_id == 7
        assert await c.acquire(REQ) is None
    assert seen[0].url.path == "/api/v1/executor/acquire"
    assert seen[0].headers["authorization"] == "Bearer tok123"
    assert json.loads(seen[0].content)["engines"] == ["mlx"]


async def test_register_and_preference() -> None:
    def handler(req: httpx.Request) -> httpx.Response:
        if req.url.path.endswith("register"):
            return httpx.Response(200, json={"executor_id": 5})
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
        assert r.executor_id == 5
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
                await c.complete(7, CompleteRequest(record_count=1, byte_size=1, sha256="a" * 64))
            elif op == "fail":
                await c.fail(7, FailRequest(retryable=True, code="X", message="m"))
            else:
                await c.release(7)
    assert ei.value.code == "STALE_ACQUIREMENT"
    assert ei.value.status_code == 409


async def test_other_errors_raise_api_error() -> None:
    async with _client(lambda r: httpx.Response(401, text="nope")) as c:
        with pytest.raises(ApiError) as ei:
            await c.release(1)
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
