import pytest
from minion_server.limits import BodyLimitMiddleware
from starlette.types import Message, Scope


async def test_app_raising_on_truncated_body_yields_413() -> None:
    chunks = [
        {"type": "http.request", "body": b"x" * 2_000_000, "more_body": False},
    ]
    sent: list[Message] = []

    async def receive() -> Message:
        return chunks.pop(0)

    async def send(msg: Message) -> None:
        sent.append(msg)

    scope: Scope = {"type": "http", "headers": [], "app": None}

    async def reading_app(scope: Scope, rcv: object, snd: object) -> None:
        await rcv()  # type: ignore[operator]
        raise ValueError("boom")

    mw = BodyLimitMiddleware(reading_app)
    await mw(scope, receive, send)
    assert sent[0]["status"] == 413


async def test_app_error_without_oversize_body_still_raises() -> None:
    async def app(scope: Scope, receive: object, send: object) -> None:
        raise ValueError("real bug")

    async def receive() -> Message:
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(msg: Message) -> None: ...

    with pytest.raises(ValueError, match="real bug"):
        await BodyLimitMiddleware(app)({"type": "http", "headers": [], "app": None}, receive, send)
