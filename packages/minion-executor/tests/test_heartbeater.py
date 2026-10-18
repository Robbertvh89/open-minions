"""Heartbeater unit tests: interval validation and jitter."""

import uuid

import pytest
from minion_common.schemas import PresignedPost
from minion_executor.heartbeat import Heartbeater


def _post(key: str = "k", policy: str = "p") -> PresignedPost:
    return PresignedPost(url="https://s3.test/out", fields={"key": key, "policy": policy})


def _hb(interval: float) -> Heartbeater:
    return Heartbeater(
        None,  # type: ignore[arg-type]
        uuid.uuid4(),
        interval_s=interval,
        budget=None,
        total=1,
        initial_output=_post(),
    )


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), float("-inf"), 0.5, 3601.0])
def test_rejects_bad_intervals(bad: float) -> None:
    with pytest.raises(ValueError, match="heartbeat interval"):
        _hb(bad)


def test_jitter_never_goes_below_interval() -> None:
    hb = _hb(10.0)
    assert all(10.0 <= hb._jittered(10.0) <= 11.0 for _ in range(500))


async def test_beats_are_serialized_and_429_retry_waits_once() -> None:
    import asyncio
    from datetime import UTC, datetime

    from minion_common.client import ApiError
    from minion_common.schemas import HeartbeatResponse

    active = 0
    peak = 0
    calls = 0
    slept: list[float] = []

    class C:
        async def heartbeat(self, aid: uuid.UUID, req: object) -> HeartbeatResponse:
            nonlocal active, peak, calls
            calls += 1
            active += 1
            peak = max(peak, active)
            await asyncio.sleep(0.01)
            active -= 1
            if calls == 1:
                raise ApiError(429, "RATE_LIMITED", "slow down", retry_after=2.0)
            return HeartbeatResponse(
                cancel=False, output_post=_post(), expires_at=datetime.now(UTC)
            )

    async def fake_sleep(d: float) -> None:
        slept.append(d)

    hb = Heartbeater(
        C(),  # type: ignore[arg-type]
        uuid.uuid4(),
        interval_s=10.0,
        budget=None,
        total=1,
        initial_output=_post(),
        sleep=fake_sleep,
    )
    first, second = await asyncio.gather(hb.beat(retry_rate_limited=True), hb.beat())
    assert peak == 1
    assert slept == [2.0]
    assert first == (None, None)  # retried once, then succeeded
    assert second == (None, None)
    assert calls == 3


async def test_refresh_accepts_same_key_newer_expiry_and_ignores_other_key() -> None:
    from datetime import UTC, datetime, timedelta

    from minion_common.schemas import HeartbeatResponse

    now = datetime.now(UTC)
    resp: list[HeartbeatResponse] = []

    class C:
        async def heartbeat(self, aid: uuid.UUID, req: object) -> HeartbeatResponse:
            return resp.pop(0)

    hb = Heartbeater(
        C(),  # type: ignore[arg-type]
        uuid.uuid4(),
        interval_s=10.0,
        budget=None,
        total=1,
        initial_output=_post(),
        initial_expires_at=now,
    )
    resp.append(
        HeartbeatResponse(
            cancel=False, output_post=_post("evil", "x"), expires_at=now + timedelta(hours=1)
        )
    )
    await hb.beat()
    assert hb.output_post.fields["key"] == "k" and hb.expires_at == now
    resp.append(
        HeartbeatResponse(
            cancel=False, output_post=_post("k", "new"), expires_at=now + timedelta(hours=1)
        )
    )
    await hb.beat()
    assert hb.output_post.fields["policy"] == "new"
