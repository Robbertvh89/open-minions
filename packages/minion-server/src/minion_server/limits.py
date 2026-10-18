"""Request body size cap (pure ASGI, runs before auth so unauthenticated floods are cut early)."""

import json
import time
from typing import Any

from starlette.types import ASGIApp, Message, Receive, Scope, Send

DEFAULT_MAX_BODY_BYTES = 1 << 20
_BODY = json.dumps(
    {"detail": {"code": "PAYLOAD_TOO_LARGE", "message": "request body too large"}}
).encode()


def _limit(scope: Scope) -> int:
    app: Any = scope.get("app")
    settings = getattr(getattr(app, "state", None), "settings", None)
    return int(getattr(settings, "max_request_body_bytes", DEFAULT_MAX_BODY_BYTES))


ARRIVAL_KEY = "minion_arrival"  # scope key: time.monotonic() when the request reached the app


class ArrivalStampMiddleware:
    """Stamp the arrival time so deadlines (acquire, TASK-7.4.4) start when the request reached
    the server, not when the handler finally runs behind a busy event loop."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "http":
            scope[ARRIVAL_KEY] = time.monotonic()
        await self.app(scope, receive, send)


class BodyLimitMiddleware:
    """413 when `Content-Length` exceeds the cap, or a chunked body streams past it."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        limit = _limit(scope)
        declared = dict(scope["headers"]).get(b"content-length")
        if declared is not None and declared.isdigit() and int(declared) > limit:
            await self._reject(send)
            return

        seen = 0
        exceeded = False
        responded = False

        async def counting_receive() -> Message:
            nonlocal seen, exceeded
            msg = await receive()
            if msg["type"] == "http.request" and not exceeded:
                seen += len(msg.get("body", b""))
                if seen > limit:
                    exceeded = True
            if exceeded and msg["type"] == "http.request":
                # Truncate: the app sees an (invalid) empty body; we replace its response.
                return {"type": "http.request", "body": b"", "more_body": False}
            return msg

        async def guarded_send(msg: Message) -> None:
            nonlocal responded
            if exceeded:
                return  # swallow the app's response; 413 is sent below
            responded = True
            await send(msg)

        try:
            await self.app(scope, counting_receive, guarded_send)
        except Exception:
            # The app saw a truncated body and may blow up on it: answer 413, not 500.
            if exceeded and not responded:
                await self._reject(send)
                return
            raise
        if exceeded and not responded:
            await self._reject(send)

    @staticmethod
    async def _reject(send: Send) -> None:
        await send(
            {
                "type": "http.response.start",
                "status": 413,
                "headers": [
                    (b"content-type", b"application/json"),
                    (b"content-length", str(len(_BODY)).encode()),
                    (b"connection", b"close"),
                ],
            }
        )
        await send({"type": "http.response.body", "body": _BODY})
