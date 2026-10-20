"""Operator preflight: does the S3 backend enforce the signed `Range` on input GET URLs?

Input GET URLs sign the subtask's `Range` header (SPEC S-2). That only slice-scopes the URL if the
backend enforces SigV4 signed headers (AWS S3, MinIO do). This puts a probe object under a
dedicated prefix, presigns exactly like production (`S3Storage.presign_get` with a byte range) and
checks what the backend answers. Output never contains credentials or a presigned URL.
"""

import urllib.error
import urllib.request
import uuid
from collections.abc import Callable
from dataclasses import dataclass

from minion_server.storage import S3Storage

PROBE_PREFIX = "_preflight/check-storage/"
PROBE_BODY = b"0123456789abcdef"
SLICE = (4, 10)  # [start, end) -> bytes=4-9
SLICE_BODY = PROBE_BODY[SLICE[0] : SLICE[1]]
HTTP_TIMEOUT_S = 10

# (url, extra request headers) -> (status, body). Raises OSError on connectivity problems.
Fetch = Callable[[str, dict[str, str]], tuple[int, bytes]]


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args: object, **kwargs: object) -> None:
        return None


def urllib_fetch(url: str, headers: dict[str, str]) -> tuple[int, bytes]:
    req = urllib.request.Request(url, headers=headers)  # noqa: S310 - presigned operator URL
    opener = urllib.request.build_opener(_NoRedirect)
    try:
        with opener.open(req, timeout=HTTP_TIMEOUT_S) as r:
            return r.status, bytes(r.read())
    except urllib.error.HTTPError as exc:
        return exc.code, b""


@dataclass(frozen=True)
class Check:
    name: str
    expected: str
    observed: tuple[int, int]  # (status, body length)
    ok: bool


def evaluate(fetch: Fetch, url: str) -> list[Check]:
    ranged = {"Range": f"bytes={SLICE[0]}-{SLICE[1] - 1}"}
    checks: list[Check] = []

    def add(name: str, expected: str, res: tuple[int, bytes], ok: bool) -> None:
        checks.append(Check(name, expected, (res[0], len(res[1])), ok))

    res = fetch(url, ranged)
    add("signed Range", f"(206, {len(SLICE_BODY)}) with the slice", res, res == (206, SLICE_BODY))
    res = fetch(url, {})
    add("no Range", "(403, 0)", res, res[0] == 403)
    res = fetch(url, {"Range": f"bytes=0-{SLICE[1] - SLICE[0] - 1}"})
    add("different Range", "(403, 0)", res, res[0] == 403)
    res = fetch(url, ranged | {"If-Range": '"bogus"'})
    # If-Range is unsigned: refusing (403) or serving the same slice is fine; a wider body is not.
    add(
        "If-Range does not widen",
        f"(403, 0) or (206, {len(SLICE_BODY)})",
        res,
        res[0] == 403 or res == (206, SLICE_BODY),
    )
    return checks


async def run_check(storage: S3Storage, fetch: Fetch = urllib_fetch) -> list[Check]:
    """Run the probe; always deletes the probe object. Connectivity errors propagate (OSError)."""
    import asyncio

    key = f"{PROBE_PREFIX}{uuid.uuid4().hex}"
    try:
        await storage.put(key, PROBE_BODY)
        url = await storage.presign_get(key, 60, SLICE)
        return await asyncio.to_thread(evaluate, fetch, url)
    finally:
        await storage.delete(key)
