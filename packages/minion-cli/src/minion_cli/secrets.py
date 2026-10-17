"""API key lookup: `$MINION_API_KEY`, else the macOS Keychain. Never printed or logged."""

from __future__ import annotations

import os
import subprocess
import sys

from pydantic import SecretStr

from minion_cli.errors import CliError

ENV_VAR = "MINION_API_KEY"
KEYCHAIN_SERVICE = "open-minions-api-key"


def keychain_api_key() -> SecretStr | None:
    """The Keychain item, or None (not darwin, absent, locked, timeout)."""
    if sys.platform != "darwin":
        return None
    try:
        out = subprocess.run(  # noqa: S603 - fixed argv, no shell
            ["/usr/bin/security", "find-generic-password", "-s", KEYCHAIN_SERVICE, "-w"],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
            env={"PATH": "/usr/bin:/bin", "HOME": os.environ.get("HOME", "")},
        )
    except (OSError, subprocess.SubprocessError):
        return None
    value = out.stdout.strip()
    return SecretStr(value) if out.returncode == 0 and value else None


def load_api_key() -> SecretStr:
    env = os.environ.get(ENV_VAR, "").strip()
    if env:
        return SecretStr(env)
    found = keychain_api_key()
    if found is not None:
        return found
    raise CliError(f"no API key: set ${ENV_VAR} or add Keychain item {KEYCHAIN_SERVICE!r}")
