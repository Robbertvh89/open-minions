"""Runner implementations. Only this package may import inference engines."""

from __future__ import annotations

from minion_executor.runners.base import Runner, RunnerError


def create_runner(name: str) -> Runner:
    """Instantiate a runner by name. Engine imports stay lazy so the control process stays light."""
    if name == "fake":
        from minion_executor.runners.fake import FakeRunner

        return FakeRunner()
    raise ValueError(f"unknown runner: {name!r}")


__all__ = ["Runner", "RunnerError", "create_runner"]
