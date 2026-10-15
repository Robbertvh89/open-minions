"""Runner implementations. Only this package may import inference engines."""

from __future__ import annotations

from minion_executor.runners.base import Runner, RunnerError

KNOWN_RUNNERS = ("fake",)  # real engines (mlx, ollama, hf) land in M3


def create_runner(name: str, *, test_hooks: bool = False) -> Runner:
    """Instantiate a runner by name. Engine imports stay lazy so the control process stays light."""
    if name == "fake":
        from minion_executor.runners.fake import FakeRunner

        return FakeRunner(test_hooks=test_hooks)
    raise ValueError(f"unknown runner: {name!r}")


__all__ = ["KNOWN_RUNNERS", "Runner", "RunnerError", "create_runner"]
