"""Stateless UI session cookie and CSRF tokens (ADR-0010).

The cookie carries `v2.<kid>.<issued_at>.<expires_at>.<mac>`. `kid` is HMAC(secret, "kid", key
sha256), so the cookie never carries the key hash (which is what the keys file stores); the mac is
HMAC-SHA256 over the first four fields. Whether the key still exists is decided on every request
against the KeyStore (kid is recomputed per configured key), so removing a key ends its sessions.
"""

import hashlib
import hmac
import re
import secrets
from dataclasses import dataclass
from datetime import datetime
from urllib.parse import urlsplit

SESSION_COOKIE = "minion_session"
LOGIN_COOKIE = "minion_login"  # anonymous nonce binding the login form to this browser
LOGIN_NONCE_TTL_S = 600
COOKIE_PATH = "/ui"
_VERSION = "v2"
_MAX_COOKIE_LEN = 512
_SHA_RE = re.compile(r"[0-9a-f]{64}")


@dataclass(frozen=True)
class SessionClaims:
    kid: str
    issued_at: int
    expires_at: int
    payload: str


def _mac(secret: bytes, purpose: str, message: str) -> str:
    return hmac.new(secret, f"{purpose}\0{message}".encode(), hashlib.sha256).hexdigest()


def key_id(secret: bytes, key_sha256: str) -> str:
    return _mac(secret, "kid", key_sha256)


def tokens_equal(a: str, b: str) -> bool:
    """Constant-time comparison that tolerates non-ASCII input (compare_digest on str does not)."""
    return hmac.compare_digest(a.encode("utf-8"), b.encode("utf-8"))


def issue_session(secret: bytes, key_sha256: str, now: datetime, ttl_s: int) -> str:
    issued = int(now.timestamp())
    payload = f"{_VERSION}.{key_id(secret, key_sha256)}.{issued}.{issued + ttl_s}"
    return f"{payload}.{_mac(secret, 'session', payload)}"


def verify_session(secret: bytes, value: str, now: datetime, ttl_s: int) -> SessionClaims | None:
    """Claims of a genuine, unexpired cookie; None for anything else (tampered, expired, junk)."""
    if len(value) > _MAX_COOKIE_LEN:
        return None
    parts = value.split(".")
    if len(parts) != 5 or parts[0] != _VERSION:
        return None
    payload = ".".join(parts[:4])
    if not tokens_equal(parts[4], _mac(secret, "session", payload)):
        return None
    if not _SHA_RE.fullmatch(parts[1]) or not (parts[2].isdecimal() and parts[3].isdecimal()):
        return None
    issued, expires = int(parts[2]), int(parts[3])
    now_ts = int(now.timestamp())
    # Age-based: a TTL lowered after issue shortens existing sessions (no revival if raised again
    # later, since the age check uses the current TTL only to cut, never to extend `expires`).
    if now_ts >= min(expires, issued + ttl_s) or issued > now_ts + 60:
        return None
    return SessionClaims(parts[1], issued, expires, payload)


def csrf_token(secret: bytes, claims: SessionClaims) -> str:
    """Per-session CSRF token: an HMAC of the signed payload, so it dies with the session."""
    return _mac(secret, "csrf", claims.payload)


def new_login_nonce() -> str:
    return secrets.token_urlsafe(16)


def login_csrf_token(secret: bytes, nonce: str) -> str:
    return _mac(secret, "login-csrf", nonce)


def origin_ok(origin: str | None, referer: str | None, host: str | None) -> bool:
    """The request came from a page on this host: `Origin` (else `Referer`) must match `Host`.

    Missing both, `Origin: null` and foreign hosts are rejected. Scheme is not compared: behind a
    TLS-terminating proxy the server sees http while the browser says https. The reverse proxy
    must pass the original `Host` header through.
    """
    source = origin if origin is not None else referer
    if not source or not host:
        return False
    try:
        parts = urlsplit(source)
    except ValueError:
        return False
    if parts.scheme not in ("http", "https") or not parts.netloc:
        return False
    return tokens_equal(parts.netloc.lower(), host.lower())
