"""Shared HTTP error helpers; body shape is parsed by minion_common.client."""

import random

from fastapi import HTTPException


def api_error(status: int, code: str, message: str) -> HTTPException:
    return HTTPException(status_code=status, detail={"code": code, "message": message})


def busy_error(code: str, message: str) -> HTTPException:
    """503 + a jittered numeric Retry-After (1-3 s): the server shed this request, the executor
    waits and retries (TASK-7.4.4). The jitter spreads a herd that was shed together."""
    return HTTPException(
        status_code=503,
        detail={"code": code, "message": message},
        headers={"Retry-After": str(random.randint(1, 3))},  # noqa: S311
    )
