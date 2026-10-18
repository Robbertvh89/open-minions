"""ExecutorTokenCache (TASK-32): fresh/stale/negative/unknown, TTL 0, LRU bounds."""

from datetime import UTC, datetime

from minion_server.tokencache import ExecutorTokenCache, LookupState


class Clock:
    def __init__(self) -> None:
        self.t = 100.0

    def __call__(self) -> float:
        return self.t


def _cache(ttl: float = 10.0, n: int = 3, neg: int = 5) -> tuple[ExecutorTokenCache[str], Clock]:
    clock = Clock()
    return ExecutorTokenCache[str](ttl, n, negative_max=neg, clock=clock), clock


def test_unknown_fresh_stale() -> None:
    c, clock = _cache()
    assert c.lookup("a").state is LookupState.UNKNOWN
    exp = datetime(2030, 1, 1, tzinfo=UTC)
    c.put_valid("a", "P", exp)
    hit = c.lookup("a")
    assert (hit.state, hit.principal, hit.token_expires_at) == (LookupState.FRESH, "P", exp)
    clock.t += 9.9
    assert c.lookup("a").state is LookupState.FRESH
    clock.t += 0.2
    assert c.lookup("a").state is LookupState.STALE  # known digest: revalidate, not unknown


def test_negative_expires_to_unknown_and_invalid_drops_positive() -> None:
    c, clock = _cache()
    c.put_valid("a", "P", None)
    c.put_invalid("a")
    assert c.lookup("a").state is LookupState.NEGATIVE
    clock.t += 10.0
    assert c.lookup("a").state is LookupState.UNKNOWN
    c.put_invalid("a")
    c.put_valid("a", "P", None)  # re-validated (rotated back): no longer negative
    assert c.lookup("a").state is LookupState.FRESH


def test_ttl_zero_never_fresh() -> None:
    c, _ = _cache(ttl=0)
    c.put_valid("a", "P", None)
    assert c.lookup("a").state is LookupState.STALE
    c.put_invalid("b")
    assert c.lookup("b").state is LookupState.UNKNOWN


def test_random_digest_flood_never_evicts_positives() -> None:
    c, _ = _cache(n=3, neg=5)
    for k in "abc":
        c.put_valid(k, k, None)
    for i in range(100):
        c.put_invalid(f"junk{i}")
    assert all(c.lookup(k).state is LookupState.FRESH for k in "abc")
    assert len(c._neg) == 5


def test_positive_lru_bound() -> None:
    c, _ = _cache(n=2)
    c.put_valid("a", "1", None)
    c.put_valid("b", "2", None)
    c.lookup("a")  # a is most recent
    c.put_valid("c", "3", None)
    assert c.lookup("b").state is LookupState.UNKNOWN
    assert c.lookup("a").state is LookupState.FRESH
    assert len(c._pos) == 2


def test_older_positive_read_does_not_overwrite_a_newer_negative() -> None:
    c, clock = _cache()
    checked_at = c.now()  # revalidation starts
    clock.t += 1.0
    c.put_invalid("a")  # a concurrent request saw the revocation
    clock.t += 1.0
    c.put_valid("a", "P", None, checked_at)  # the slow, older read finishes last
    assert c.lookup("a").state is LookupState.NEGATIVE
    c.put_valid("a", "P", None, c.now())  # a newer read may replace it
    assert c.lookup("a").state is LookupState.FRESH


def test_preload_is_stale_bounded_and_never_overrides() -> None:
    c, _ = _cache(n=2)
    c.put_valid("a", "live", None)
    c.preload("a", "other", None)
    c.preload("b", "B", None)
    c.preload("c", "C", None)  # full: skipped
    assert c.lookup("a").principal == "live"
    assert c.lookup("b").state is LookupState.STALE
    assert c.lookup("c").state is LookupState.UNKNOWN
