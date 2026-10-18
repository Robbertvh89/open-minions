"""Claim/acquire guards that need no database (ADR-0013)."""

import uuid
from typing import Any

import pytest
import structlog
from minion_common.enums import Engine
from minion_server.auth import ExecutorPrincipal
from minion_server.repositories import acquisition as repo
from minion_server.repositories import executors as executors_repo
from minion_server.services import acquisition as svc
from minion_server.settings import ServerSettings
from pydantic import SecretStr, ValidationError

from conftest import _req


def _cfg(**kw: Any) -> ServerSettings:
    return ServerSettings(
        database_url=SecretStr("postgresql+asyncpg://x@h/db"),
        s3_access_key=SecretStr("ak"),
        s3_secret_key=SecretStr("sk"),
        _env_file=None,
        **kw,
    )


@pytest.mark.parametrize("bad", [0, -1, 257])
def test_settings_reject_window_out_of_bounds(bad: int) -> None:
    with pytest.raises(ValidationError):
        _cfg(acquire_window=bad)


@pytest.mark.parametrize("ok", [1, 16, 256])
def test_settings_accept_window_bounds(ok: int) -> None:
    assert _cfg(acquire_window=ok).acquire_window == ok


@pytest.mark.parametrize("bad", [0, -3, 257])
async def test_claim_rejects_bad_window(bad: int) -> None:
    with pytest.raises(ValueError, match="window"):
        await repo.claim(
            None,  # type: ignore[arg-type]  # rejected before any SQL
            uuid.uuid4(),
            engines=[Engine.MLX],
            model_ids=["m"],
            memory_gb=8,
            pool="default",
            preference=None,
            window=bad,
        )


@pytest.mark.parametrize("bad", [0, 257])
async def test_acquire_rejects_bad_window(bad: int) -> None:
    p = ExecutorPrincipal(uuid.uuid4(), "o", "default")
    with pytest.raises(ValueError, match="window"):
        await svc.acquire(
            None,  # type: ignore[arg-type]
            p,
            _req(),
            settings=_cfg(),
            storage=None,  # type: ignore[arg-type]
            models={},
            window=bad,
        )


def _row(ins_key: tuple[Any, ...], pick_key: tuple[Any, ...]) -> tuple[Any, ...]:
    rest: tuple[Any, ...] = ("m", "k", 0, 10, 1, "s3://b/o", {})
    return (1, uuid.uuid4(), 1, *ins_key, *pick_key, *rest)


async def test_claim_rejects_inserted_key_differing_from_pick() -> None:
    row = _row(("t", "p", "n", 1), ("t", "p", "n", 2))
    with pytest.raises(RuntimeError, match="differs"):
        await repo._claim_from_row(None, row)  # type: ignore[arg-type]


class _Ctx:
    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, *a: Any) -> None:
        return None


class _FakeSession:
    def begin(self) -> _Ctx:
        return _Ctx()

    async def connection(self) -> None:
        return None


async def test_exhausted_rounds_are_logged(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {"claim": 0}

    async def touch(*a: Any, **kw: Any) -> None:
        return None

    async def pref(*a: Any, **kw: Any) -> None:
        return None

    async def claim(*a: Any, **kw: Any) -> repo.Claim:
        calls["claim"] += 1
        return repo.Claim(3)  # work exists, the pick always loses the race

    monkeypatch.setattr(executors_repo, "touch", touch)
    monkeypatch.setattr(executors_repo, "get_preference", pref)
    monkeypatch.setattr(repo, "claim", claim)
    p = ExecutorPrincipal(uuid.uuid4(), "o", "default")
    with structlog.testing.capture_logs() as logs:
        got = await svc.acquire(
            _FakeSession(),  # type: ignore[arg-type]
            p,
            _req(),
            settings=_cfg(),
            storage=None,  # type: ignore[arg-type]
            models={"m": None},  # type: ignore[dict-item]
        )
    assert got is None and calls["claim"] == svc.MAX_ROUNDS
    [ev] = [e for e in logs if e["event"] == "acquire_rounds_exhausted"]
    assert ev["rounds"] == svc.MAX_ROUNDS and "executor_id" not in ev


async def test_empty_queue_is_not_logged_as_exhausted(monkeypatch: pytest.MonkeyPatch) -> None:
    async def noop(*a: Any, **kw: Any) -> None:
        return None

    async def claim(*a: Any, **kw: Any) -> repo.Claim:
        return repo.Claim(0)

    monkeypatch.setattr(executors_repo, "touch", noop)
    monkeypatch.setattr(executors_repo, "get_preference", noop)
    monkeypatch.setattr(repo, "claim", claim)
    p = ExecutorPrincipal(uuid.uuid4(), "o", "default")
    with structlog.testing.capture_logs() as logs:
        got = await svc.acquire(
            _FakeSession(),  # type: ignore[arg-type]
            p,
            _req(),
            settings=_cfg(),
            storage=None,  # type: ignore[arg-type]
            models={"m": None},  # type: ignore[dict-item]
        )
    assert got is None and logs == []
