"""Shared HTTP error helpers; body shape is parsed by minion_common.client."""

import random

from fastapi import HTTPException


def api_error(status: int, code: str, message: str) -> HTTPException:
    return HTTPException(status_code=status, detail={"code": code, "message": message})


def busy_error(code: str, message: str, *, retry_after: tuple[int, int] = (1, 3)) -> HTTPException:
    """503 + a jittered numeric Retry-After (default 1-3 s): the server shed this request, the
    executor waits and retries (TASK-7.4.4). The jitter spreads a herd that was shed together.
    A saturated DB pool needs a wider window (2-10 s) than a full acquire slot (1-3 s)."""
    return HTTPException(
        status_code=503,
        detail={"code": code, "message": message},
        headers={"Retry-After": str(random.randint(*retry_after))},  # noqa: S311
    )
