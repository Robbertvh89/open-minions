"""Executor token lookup: `$MINION_EXECUTOR_TOKEN`, else macOS Keychain. Never logged."""

from __future__ import annotations

import os
import subprocess

from pydantic import SecretStr

ENV_VAR = "MINION_EXECUTOR_TOKEN"
KEYCHAIN_SERVICE = "open-minions-executor"


def load_token() -> SecretStr:
    env = os.environ.get(ENV_VAR)
    if env:
        return SecretStr(env.strip())
    try:
        out = subprocess.run(  # noqa: S603 - fixed argv, no shell
            ["/usr/bin/security", "find-generic-password", "-s", KEYCHAIN_SERVICE, "-w"],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        out = None
    if out is not None and out.returncode == 0 and out.stdout.strip():
        return SecretStr(out.stdout.strip())
    raise RuntimeError(
        f"no executor token: set ${ENV_VAR} or add Keychain item {KEYCHAIN_SERVICE!r}"
    )
