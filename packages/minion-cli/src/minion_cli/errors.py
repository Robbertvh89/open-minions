"""CLI failure with a process exit code (see `minion --help`)."""

from __future__ import annotations

import re

EXIT_OK = 0
EXIT_FAILED = 1  # task FAILED / CANCELLED
EXIT_USAGE = 2  # usage, config, auth, other 4xx
EXIT_PARTIAL = 3  # task PARTIAL
EXIT_UNREACHABLE = 4  # timeout / server or S3 unreachable
EXIT_INTERRUPTED = 130  # Ctrl-C; never cancels the task


class CliError(Exception):
    """A user-facing failure. The message never contains payload or secret content."""

    def __init__(self, message: str, exit_code: int = EXIT_USAGE) -> None:
        super().__init__(message)
        self.exit_code = exit_code


_CONTROL = re.compile(r"[\x00-\x1f\x7f-\x9f\u202a-\u202e\u2066-\u2069]")


def clean(text: object) -> str:
    """Strip C0/C1 control characters and Unicode bidi overrides from server-supplied text."""
    return _CONTROL.sub("", str(text))
