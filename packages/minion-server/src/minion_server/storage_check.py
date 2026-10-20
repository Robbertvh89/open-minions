"""Operator preflight: does the S3 backend enforce the signed `Range` on input GET URLs?

Input GET URLs sign the subtask's `Range` header (SPEC S-2). That only slice-scopes the URL if the
backend enforces SigV4 signed headers (AWS S3, MinIO do). This puts a probe object under a
dedicated prefix, presigns exactly like production (`S3Storage.presign_get` with a byte range) and
checks what the backend answers. Output never contains credentials or a presigned URL.
"""

import asyncio
import urllib.error
import urllib.request
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from minion_common.schemas import range_header

from minion_server.storage import PROBE_PREFIX, S3Storage

STALE_PROBE_AGE_S = 3600
IF_RANGE_DATE = "Thu, 01 Jan 1970 00:00:00 GMT"
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


def _unsigned(url: str) -> str:
    return urlunsplit(urlsplit(url)._replace(query=""))


def _tampered(url: str) -> str:
    parts = urlsplit(url)
    q = [
        (k, ("0" * len(v)) if k == "X-Amz-Signature" else v)
        for k, v in parse_qsl(parts.query, keep_blank_values=True)
    ]
    return urlunsplit(parts._replace(query=urlencode(q)))


def evaluate(fetch: Fetch, url: str) -> list[Check]:
    ranged = {"Range": range_header(*SLICE)}
    other = {"Range": range_header(0, SLICE[1] - SLICE[0])}
    checks: list[Check] = []

    def add(name: str, expected: str, res: tuple[int, bytes], ok: bool) -> None:
        checks.append(Check(name, expected, (res[0], len(res[1])), ok))

    def refused(name: str, target: str, headers: dict[str, str]) -> None:
        res = fetch(target, headers)
        add(name, "(403, 0)", res, res[0] == 403)

    res = fetch(url, ranged)
    add("signed Range", f"(206, {len(SLICE_BODY)}) with the slice", res, res == (206, SLICE_BODY))
    refused("no Range", url, {})
    refused("different Range", url, other)
    # If-Range is unsigned: refusing (403) or serving the same slice is fine; a wider body is not.
    for name, value in (("If-Range (ETag)", '"bogus"'), ("If-Range (HTTP date)", IF_RANGE_DATE)):
        res = fetch(url, ranged | {"If-Range": value})
        add(
            f"{name} does not widen",
            f"(403, 0) or (206, {len(SLICE_BODY)})",
            res,
            res[0] == 403 or res == (206, SLICE_BODY),
        )
    refused("unsigned GET (no query)", _unsigned(url), ranged)
    refused("tampered signature", _tampered(url), ranged)
    return checks


async def run_check(
    storage: S3Storage,
    fetch: Fetch = urllib_fetch,
    warn: Callable[[str], None] = lambda _msg: None,
) -> list[Check]:
    """Run the probe; always tries to delete it. Connectivity errors propagate (OSError)."""
    await storage.discard_stale_probes(STALE_PROBE_AGE_S)
    key = f"{PROBE_PREFIX}{uuid.uuid4().hex}"
    try:
        await storage.put(key, PROBE_BODY)
        url = await storage.presign_get(key, 60, SLICE)
        return await asyncio.to_thread(evaluate, fetch, url)
    finally:
        try:
            await storage.delete(key)
        except Exception as exc:  # never replace the results; the key is random, so say where
            warn(f"probe object not deleted under {PROBE_PREFIX} ({type(exc).__name__})")
