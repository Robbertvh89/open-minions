"""ExecutorSettings validation (cache dir, disk reserve, dev-only sandbox switches)."""

from __future__ import annotations

from pathlib import Path

import pytest
from minion_executor.settings import ExecutorSettings
from pydantic import ValidationError


def test_cache_dir_is_expanded_and_must_be_absolute(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MINION_EXECUTOR_MODEL_CACHE_DIR", "~/models-x")
    assert ExecutorSettings().model_cache_dir == Path.home() / "models-x"
    monkeypatch.setenv("MINION_EXECUTOR_MODEL_CACHE_DIR", "relative/dir")
    with pytest.raises(ValidationError, match="absolute"):
        ExecutorSettings()


def test_min_free_disk_default_and_override(monkeypatch: pytest.MonkeyPatch) -> None:
    assert ExecutorSettings().min_free_disk_gb == 10.0
    monkeypatch.setenv("MINION_EXECUTOR_MIN_FREE_DISK_GB", "25")
    assert ExecutorSettings().min_free_disk_gb == 25.0
    monkeypatch.setenv("MINION_EXECUTOR_MIN_FREE_DISK_GB", "-1")
    with pytest.raises(ValidationError):
        ExecutorSettings()


def test_fetch_sandbox_off_needs_dev(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MINION_EXECUTOR_DEV", raising=False)
    assert ExecutorSettings().fetch_sandbox is True
    monkeypatch.setenv("MINION_EXECUTOR_FETCH_SANDBOX", "0")
    with pytest.raises(ValidationError, match="DEV"):
        ExecutorSettings()
    monkeypatch.setenv("MINION_EXECUTOR_DEV", "1")
    assert ExecutorSettings().fetch_sandbox is False


def test_config_path_expanded_absolute_and_empty_is_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MINION_EXECUTOR_CONFIG", "~/x.toml")
    assert ExecutorSettings().config_path == Path.home() / "x.toml"
    monkeypatch.setenv("MINION_EXECUTOR_CONFIG", "rel/x.toml")
    with pytest.raises(ValidationError, match="absolute"):
        ExecutorSettings()
    monkeypatch.setenv("MINION_EXECUTOR_CONFIG", "")
    assert ExecutorSettings().config_path is None


def test_stop_grace_default_and_bounds(monkeypatch: pytest.MonkeyPatch) -> None:
    assert ExecutorSettings().stop_grace_s == 30.0
    monkeypatch.setenv("MINION_EXECUTOR_STOP_GRACE_S", "0")
    assert ExecutorSettings().stop_grace_s == 0.0
    for bad in ("-1", "301", "nan"):
        monkeypatch.setenv("MINION_EXECUTOR_STOP_GRACE_S", bad)
        with pytest.raises(ValidationError):
            ExecutorSettings()


def test_state_dir_default_and_absolute(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MINION_EXECUTOR_STATE_DIR", raising=False)
    assert ExecutorSettings().state_dir == (
        Path.home() / "Library" / "Application Support" / "open-minions"
    )
    monkeypatch.setenv("MINION_EXECUTOR_STATE_DIR", "relative")
    with pytest.raises(ValidationError, match="absolute"):
        ExecutorSettings()


def test_fake_signals_needs_dev(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.delenv("MINION_EXECUTOR_DEV", raising=False)
    monkeypatch.setenv("MINION_EXECUTOR_FAKE_SIGNALS", str(tmp_path / "s.json"))
    with pytest.raises(ValidationError, match="DEV"):
        ExecutorSettings()
    monkeypatch.setenv("MINION_EXECUTOR_DEV", "1")
    assert ExecutorSettings().fake_signals == tmp_path / "s.json"
    monkeypatch.setenv("MINION_EXECUTOR_FAKE_SIGNALS", "")
    assert ExecutorSettings().fake_signals is None
    monkeypatch.setenv("MINION_EXECUTOR_FAKE_SIGNALS", "rel.json")
    with pytest.raises(ValidationError, match="absolute"):
        ExecutorSettings()


def test_governor_poll_and_presence_bounds(monkeypatch: pytest.MonkeyPatch) -> None:
    s = ExecutorSettings()
    assert (s.signal_poll_s, s.presence_interval_s) == (2.0, 60.0)
    for name, bad in (
        ("SIGNAL_POLL_S", "0.1"),
        ("SIGNAL_POLL_S", "30"),
        ("SIGNAL_POLL_S", "4.5"),  # NF-9: detection + one engine step must stay < 5 s
        ("PRESENCE_INTERVAL_S", "0"),
        ("PRESENCE_INTERVAL_S", "300"),
    ):
        monkeypatch.setenv(f"MINION_EXECUTOR_{name}", bad)
        with pytest.raises(ValidationError):
            ExecutorSettings()
        monkeypatch.delenv(f"MINION_EXECUTOR_{name}")


@pytest.mark.parametrize(
    "name", ["PAUSE_RELEASE_AFTER_S", "RESUME_AFTER_S", "ESCALATE_AFTER_S", "DOWNGRADE_BATCH_S"]
)
def test_governor_timers_are_dev_only(monkeypatch: pytest.MonkeyPatch, name: str) -> None:
    monkeypatch.delenv("MINION_EXECUTOR_DEV", raising=False)
    monkeypatch.setenv(f"MINION_EXECUTOR_{name}", "3")
    with pytest.raises(ValidationError, match="MINION_EXECUTOR_DEV"):
        ExecutorSettings()
    monkeypatch.setenv("MINION_EXECUTOR_DEV", "1")
    assert getattr(ExecutorSettings(), name.lower()) == 3.0
