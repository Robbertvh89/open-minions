"""Per-process executor token cache (TASK-32, ADR-0018). Pure, no DB.

Keys are SHA-256 digests of the bearer token, never the raw token. Memory is bounded: a positive
LRU (`max_entries`) and a separate negative LRU (`negative_max`), so a flood of random tokens only
churns the negative map and never evicts known executors. Event-loop only (no await between a
read and the following write), like `ratelimit.py`.
"""

import time
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any


class LookupState(StrEnum):
    FRESH = "fresh"  # positive entry younger than the TTL: no DB needed
    STALE = "stale"  # known digest, must be revalidated (main pool)
    NEGATIVE = "negative"  # recently found invalid: answer 401 without a DB call
    UNKNOWN = "unknown"  # never seen: look up on the dedicated auth pool


@dataclass(frozen=True)
class Lookup:
    state: LookupState
    principal: Any = None
    token_expires_at: datetime | None = None


@dataclass(frozen=True)
class _Positive:
    principal: Any
    token_expires_at: datetime | None
    checked_at: float


_UNKNOWN = Lookup(LookupState.UNKNOWN)
_NEGATIVE = Lookup(LookupState.NEGATIVE)


class ExecutorTokenCache:
    def __init__(
        self,
        ttl_s: float,
        max_entries: int,
        *,
        negative_max: int = 10_000,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.ttl_s = ttl_s
        self.max_entries = max_entries
        self.negative_max = negative_max
        self._clock = clock
        self._pos: OrderedDict[str, _Positive] = OrderedDict()
        self._neg: OrderedDict[str, float] = OrderedDict()

    def lookup(self, digest: str) -> Lookup:
        now = self._clock()
        pos = self._pos.get(digest)
        if pos is not None:
            self._pos.move_to_end(digest)
            fresh = now - pos.checked_at < self.ttl_s
            return Lookup(
                LookupState.FRESH if fresh else LookupState.STALE,
                pos.principal,
                pos.token_expires_at,
            )
        neg = self._neg.get(digest)
        if neg is not None:
            if now - neg < self.ttl_s:
                self._neg.move_to_end(digest)
                return _NEGATIVE
            del self._neg[digest]
        return _UNKNOWN

    def put_valid(self, digest: str, principal: Any, token_expires_at: datetime | None) -> None:
        self._neg.pop(digest, None)
        self._pos[digest] = _Positive(principal, token_expires_at, self._clock())
        self._pos.move_to_end(digest)
        while len(self._pos) > self.max_entries:
            self._pos.popitem(last=False)

    def put_invalid(self, digest: str) -> None:
        self._pos.pop(digest, None)
        self._neg[digest] = self._clock()
        self._neg.move_to_end(digest)
        while len(self._neg) > self.negative_max:
            self._neg.popitem(last=False)
