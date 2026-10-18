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


class LookupState(StrEnum):
    FRESH = "fresh"  # positive entry younger than the TTL: no DB needed
    STALE = "stale"  # known digest, must be revalidated (main pool)
    NEGATIVE = "negative"  # recently found invalid: answer 401 without a DB call
    UNKNOWN = "unknown"  # never seen: look up on the dedicated auth pool


@dataclass(frozen=True)
class Lookup[P]:
    state: LookupState
    principal: P | None = None
    token_expires_at: datetime | None = None


@dataclass(frozen=True)
class _Positive[P]:
    principal: P
    token_expires_at: datetime | None
    checked_at: float


class ExecutorTokenCache[P]:
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
        self._pos: OrderedDict[str, _Positive[P]] = OrderedDict()
        self._neg: OrderedDict[str, float] = OrderedDict()

    def now(self) -> float:
        """The cache clock; take it BEFORE a DB read and pass it to `put_valid` as `checked_at`."""
        return self._clock()

    def lookup(self, digest: str) -> Lookup[P]:
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
                return Lookup(LookupState.NEGATIVE)
            del self._neg[digest]
        return Lookup(LookupState.UNKNOWN)

    def put_valid(
        self,
        digest: str,
        principal: P,
        token_expires_at: datetime | None,
        checked_at: float | None = None,
    ) -> None:
        """Record a successful DB read. `checked_at` is `now()` taken before that read: a negative
        entry written after it (a concurrent revoke/401) wins over this older positive result."""
        neg = self._neg.get(digest)
        if neg is not None and checked_at is not None and neg > checked_at:
            return
        self._neg.pop(digest, None)
        self._pos[digest] = _Positive(
            principal, token_expires_at, self._clock() if checked_at is None else checked_at
        )
        self._pos.move_to_end(digest)
        while len(self._pos) > self.max_entries:
            self._pos.popitem(last=False)

    def preload(self, digest: str, principal: P, token_expires_at: datetime | None) -> None:
        """Startup warm-up: a known but stale entry, so the digest revalidates on the main pool
        instead of the small auth lane. Never evicts beyond `max_entries`."""
        if len(self._pos) < self.max_entries and digest not in self._pos:
            self._pos[digest] = _Positive(principal, token_expires_at, float("-inf"))

    def put_invalid(self, digest: str) -> None:
        self._pos.pop(digest, None)
        self._neg[digest] = self._clock()
        self._neg.move_to_end(digest)
        while len(self._neg) > self.negative_max:
            self._neg.popitem(last=False)
