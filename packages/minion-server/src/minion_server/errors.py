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


def capped_error() -> HTTPException:
    """429 TOO_MANY_ACQUIREMENTS: the executor already holds the maximum number of open
    acquirements (TASK-33, ADR-0019). The executor backs off like for any 429."""
    return HTTPException(
        status_code=429,
        detail={
            "code": "TOO_MANY_ACQUIREMENTS",
            "message": "executor holds too many open acquirements",
        },
        headers={"Retry-After": "30"},
    )


class ExecutorRevokedError(Exception):
    """The authenticated executor token no longer matches a live executor row (revoked, rotated
    away): answered 401 by an app-wide handler, which also drops the token from the auth cache so
    the next request is refused without a DB call (TASK-32, ADR-0018)."""

    def __init__(self, token_hash: str) -> None:
        super().__init__("executor token is no longer valid")
        self.token_hash = token_hash
