"""S3 URI validation: configured bucket only, allowed prefixes, no traversal."""

import unicodedata
from dataclasses import dataclass
from urllib.parse import urlsplit


class InvalidUriError(ValueError):
    """Raised for any URI that violates the rules; handlers map it to 422."""


@dataclass(frozen=True)
class S3Uri:
    bucket: str
    key: str


def parse_s3_uri(uri: str, *, bucket: str, prefixes: tuple[str, ...]) -> S3Uri:
    if any(c.isspace() or unicodedata.category(c).startswith("C") for c in uri):
        raise InvalidUriError("whitespace and control characters not allowed")
    try:
        parts = urlsplit(uri)
    except ValueError as exc:
        raise InvalidUriError("malformed uri") from exc
    if parts.scheme != "s3":
        raise InvalidUriError("scheme must be s3")
    if parts.query or parts.fragment:
        raise InvalidUriError("query/fragment not allowed")
    if parts.netloc != bucket:
        raise InvalidUriError("bucket not allowed")
    key = parts.path.removeprefix("/") if parts.path.startswith("/") else parts.path
    if parts.path.startswith("//") or not key:
        raise InvalidUriError("invalid key")
    if "//" in key or "\\" in key or any(seg in ("..", ".") for seg in key.split("/")):
        raise InvalidUriError("invalid key path")
    if not any(key.startswith(p) for p in prefixes):
        raise InvalidUriError("key prefix not allowed")
    return S3Uri(bucket=parts.netloc, key=key)
