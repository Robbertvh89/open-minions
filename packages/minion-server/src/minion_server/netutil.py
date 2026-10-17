"""Small network helpers shared by the app (metrics warning) and the web UI (cookie `Secure`)."""

import ipaddress
from urllib.parse import urlsplit


def is_loopback(host: str) -> bool:
    """True for `localhost` and loopback IP literals (no port, no brackets)."""
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return host == "localhost"


def host_header_is_loopback(host_header: str | None) -> bool:
    """True when a `Host` header (may carry a port) names a loopback host."""
    if not host_header:
        return False
    try:
        name = urlsplit(f"//{host_header}").hostname
    except ValueError:
        return False
    return name is not None and is_loopback(name)
