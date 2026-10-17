"""Executor token lookup: `$MINION_EXECUTOR_TOKEN`, else macOS Keychain. Never logged."""

from __future__ import annotations

import os
import subprocess

from pydantic import SecretStr

from minion_executor.worker.process import scrub_env

ENV_VAR = "MINION_EXECUTOR_TOKEN"
KEYCHAIN_SERVICE = "open-minions-executor"


def keychain_token() -> SecretStr | None:
    """The Keychain item only (never the environment); None if absent or unreadable."""
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
        return None
    if out.returncode == 0 and out.stdout.strip():
        return SecretStr(out.stdout.strip())
    return None


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


def load_token() -> SecretStr:
    env = os.environ.get(ENV_VAR)
    if env:
        return SecretStr(env.strip())
    found = keychain_token()
    if found is not None:
        return found
    raise RuntimeError(
        f"no executor token: set ${ENV_VAR} or add Keychain item {KEYCHAIN_SERVICE!r}"
    )
