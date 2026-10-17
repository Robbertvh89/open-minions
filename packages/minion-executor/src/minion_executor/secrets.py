"""Executor token lookup: `$MINION_EXECUTOR_TOKEN`, else macOS Keychain. Never logged."""

from __future__ import annotations

import os
import subprocess

from pydantic import SecretStr

from minion_executor.worker.process import scrub_env

ENV_VAR = "MINION_EXECUTOR_TOKEN"
KEYCHAIN_SERVICE = "open-minions-executor"


SECURITY_ITEM_NOT_FOUND = 44  # `security` exit status: errSecItemNotFound


def keychain_lookup() -> tuple[SecretStr | None, bool]:
    """(token, transient). `transient` is True when the Keychain could not be read right now
    (locked, timeout, no `security`): a retry may succeed. A missing item is permanent."""
    try:
        out = subprocess.run(  # noqa: S603 - fixed argv, no shell
            ["/usr/bin/security", "find-generic-password", "-s", KEYCHAIN_SERVICE, "-w"],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
            env=scrub_env(os.environ),
        )
    except (OSError, subprocess.SubprocessError):
        return None, True
    if out.returncode == 0 and out.stdout.strip():
        return SecretStr(out.stdout.strip()), False
    return None, out.returncode != SECURITY_ITEM_NOT_FOUND


def keychain_token() -> SecretStr | None:
    """The Keychain item only (never the environment); None if absent or unreadable."""
    return keychain_lookup()[0]


def keychain_present() -> bool:
    """Whether the Keychain item exists. Never reads the secret (no `-w`)."""
    try:
        out = subprocess.run(  # noqa: S603 - fixed argv, no shell
            ["/usr/bin/security", "find-generic-password", "-s", KEYCHAIN_SERVICE],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
            env=scrub_env(os.environ),
        )
    except (OSError, subprocess.SubprocessError):
        return False
    return out.returncode == 0


class TokenUnavailableError(RuntimeError):
    """No token. `transient`: the Keychain could not be read now (locked), a retry may work."""

    def __init__(self, message: str, *, transient: bool) -> None:
        super().__init__(message)
        self.transient = transient


def load_token() -> SecretStr:
    env = os.environ.get(ENV_VAR)
    if env:
        return SecretStr(env.strip())
    found, transient = keychain_lookup()
    if found is not None:
        return found
    raise TokenUnavailableError(
        f"no executor token: set ${ENV_VAR} or add Keychain item {KEYCHAIN_SERVICE!r}"
        + (" (the Keychain could not be read; locked?)" if transient else ""),
        transient=transient,
    )
