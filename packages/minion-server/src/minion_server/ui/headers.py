"""Security headers on every `/ui` response (SPEC §8 S-13, ADR-0010)."""

from starlette.datastructures import MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send

UI_PREFIX = "/ui"
CSP = (
    "default-src 'self'; script-src 'self'; frame-ancestors 'none'; "
    "form-action 'self'; base-uri 'none'"
)
# Sent only when the server itself sees https; behind a TLS-terminating proxy the proxy adds it.
HSTS = "max-age=31536000"
# `same-origin`, not `no-referrer`: with no-referrer browsers send `Origin: null` on same-origin
# form POSTs, which the CSRF check (rightly) rejects.
SECURITY_HEADERS = {
    "Content-Security-Policy": CSP,
    "X-Content-Type-Options": "nosniff",
    "Referrer-Policy": "same-origin",
    "Cache-Control": "no-store",
}


class UiHeadersMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        path = scope.get("path", "")
        if scope["type"] != "http" or not (path == UI_PREFIX or path.startswith(UI_PREFIX + "/")):
            await self.app(scope, receive, send)
            return

        async def send_with_headers(message: Message) -> None:
            if message["type"] == "http.response.start":
                headers = MutableHeaders(scope=message)
                for name, value in SECURITY_HEADERS.items():
                    headers[name] = value
                if scope.get("scheme") == "https":
                    headers["Strict-Transport-Security"] = HSTS
            await send(message)

        await self.app(scope, receive, send_with_headers)
