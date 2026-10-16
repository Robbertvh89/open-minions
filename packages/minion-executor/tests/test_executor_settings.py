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
