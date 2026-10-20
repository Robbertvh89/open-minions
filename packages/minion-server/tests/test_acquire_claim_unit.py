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
    p = ExecutorPrincipal(uuid.uuid4(), "o", "default", "h")
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


async def _lock_ok(*a: Any, **kw: Any) -> bool:
    return True


async def _zero(*a: Any, **kw: Any) -> int:
    return 0


async def test_exhausted_rounds_are_logged(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {"claim": 0}

    async def touch(*a: Any, **kw: Any) -> bool:
        return True

    async def pref(*a: Any, **kw: Any) -> None:
        return None

    async def claim(*a: Any, **kw: Any) -> repo.Claim:
        calls["claim"] += 1
        return repo.Claim(3)  # work exists, the pick always loses the race

    monkeypatch.setattr(executors_repo, "touch", touch)
    monkeypatch.setattr(executors_repo, "get_preference", pref)
    monkeypatch.setattr(repo, "claim", claim)
    monkeypatch.setattr(repo, "lock_executor_for_claim", _lock_ok)
    monkeypatch.setattr(repo, "count_open_acquirements", _zero)
    p = ExecutorPrincipal(uuid.uuid4(), "o", "default", "h")
    trace = svc.AcquireTrace()
    with structlog.testing.capture_logs() as logs:
        got = await svc.acquire(
            _FakeSession(),  # type: ignore[arg-type]
            p,
            _req(),
            settings=_cfg(),
            storage=None,  # type: ignore[arg-type]
            models={"m": None},  # type: ignore[dict-item]
            trace=trace,
        )
    assert got is None and calls["claim"] == svc.MAX_ROUNDS
    assert trace.exhausted is True
    [ev] = [e for e in logs if e["event"] == "acquire_rounds_exhausted"]
    assert ev["rounds"] == svc.MAX_ROUNDS and "executor_id" not in ev


async def test_preferred_pass_exhausted_then_fifo_empty_is_exhausted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def live(*a: Any, **kw: Any) -> bool:
        return True

    async def pref(*a: Any, **kw: Any) -> object:
        return object()  # a preference exists -> two passes

    async def claim(*a: Any, preference: Any = None, **kw: Any) -> repo.Claim:
        return repo.Claim(3) if preference is not None else repo.Claim(0)

    monkeypatch.setattr(executors_repo, "touch", live)
    monkeypatch.setattr(executors_repo, "get_preference", pref)
    monkeypatch.setattr(repo, "claim", claim)
    monkeypatch.setattr(repo, "lock_executor_for_claim", _lock_ok)
    monkeypatch.setattr(repo, "count_open_acquirements", _zero)
    trace = svc.AcquireTrace()
    got = await svc.acquire(
        _FakeSession(),  # type: ignore[arg-type]
        ExecutorPrincipal(uuid.uuid4(), "o", "default", "h"),
        _req(),
        settings=_cfg(),
        storage=None,  # type: ignore[arg-type]
        models={"m": None},  # type: ignore[dict-item]
        trace=trace,
    )
    assert got is None and trace.exhausted is True


async def test_empty_queue_is_not_logged_as_exhausted(monkeypatch: pytest.MonkeyPatch) -> None:
    async def noop(*a: Any, **kw: Any) -> None:
        return None

    async def claim(*a: Any, **kw: Any) -> repo.Claim:
        return repo.Claim(0)

    async def live(*a: Any, **kw: Any) -> bool:
        return True

    monkeypatch.setattr(executors_repo, "touch", live)
    monkeypatch.setattr(executors_repo, "get_preference", noop)
    monkeypatch.setattr(repo, "claim", claim)
    monkeypatch.setattr(repo, "lock_executor_for_claim", _lock_ok)
    monkeypatch.setattr(repo, "count_open_acquirements", _zero)
    p = ExecutorPrincipal(uuid.uuid4(), "o", "default", "h")
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


async def test_stale_token_hash_is_refused_pre_claim(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from minion_server.errors import ExecutorRevokedError

    async def live(*a: Any, **kw: Any) -> bool:
        return True

    async def noop(*a: Any, **kw: Any) -> None:
        return None

    seen: list[Any] = []

    async def stale(*a: Any, **kw: Any) -> bool:
        seen.append(a[2] if len(a) > 2 else kw.get("token_hash"))
        return False  # the row no longer carries this digest (rotated away / revoked)

    async def boom(*a: Any, **kw: Any) -> Any:
        raise AssertionError("must not be reached with a stale token digest")

    monkeypatch.setattr(executors_repo, "touch", live)
    monkeypatch.setattr(executors_repo, "get_preference", noop)
    monkeypatch.setattr(repo, "lock_executor_for_claim", stale)
    monkeypatch.setattr(repo, "claim", boom)
    p = ExecutorPrincipal(uuid.uuid4(), "o", "default", "stale")
    with pytest.raises(ExecutorRevokedError):
        await svc.acquire(
            _FakeSession(),  # type: ignore[arg-type]
            p,
            _req(),
            settings=_cfg(),
            storage=None,  # type: ignore[arg-type]
            models={"m": None},  # type: ignore[dict-item]
        )
    assert seen == ["stale"]  # the lock filters on the principal's own digest
