"""Watchdog liveness file (TASK-6.11): touched after good ticks only; never stops the loop."""

import asyncio
import os
import stat
from pathlib import Path
from typing import Any

import pytest
from minion_watchdog import main as wd
from minion_watchdog.settings import WatchdogSettings

URL = "postgresql+asyncpg://u:p@localhost/db"


def _cfg(**kw: Any) -> WatchdogSettings:
    return WatchdogSettings(database_url=URL, watchdog_interval_s=0.01, **kw)


def _names(d: Path) -> list[str]:
    return [p.name for p in d.iterdir()]


async def _run(cfg: WatchdogSettings, done: asyncio.Event) -> None:
    """Run the loop until `done` is set by the test's fake tick, then stop it."""
    stop = asyncio.Event()
    task = asyncio.create_task(wd.run_watchdog(None, cfg, stop))  # type: ignore[arg-type]
    try:
        await asyncio.wait_for(done.wait(), 5.0)
    finally:
        stop.set()
        await asyncio.wait_for(task, 2.0)


def test_setting_defaults_to_none(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("MINION_WATCHDOG_HEALTH_FILE", raising=False)
    assert _cfg().watchdog_health_file is None


async def test_touched_at_start_and_after_good_tick(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    f = tmp_path / "h"
    calls = {"n": 0}
    done = asyncio.Event()
    seen: list[float] = []

    async def tick(*a: Any, **kw: Any) -> None:
        calls["n"] += 1
        if calls["n"] == 1:
            assert f.exists()  # created before the first tick
            os.utime(f, (1, 1))
        else:
            seen.append(f.stat().st_mtime)  # bumped after the successful first tick
            done.set()

    monkeypatch.setattr(wd, "tick", tick)
    await _run(_cfg(watchdog_health_file=str(f)), done)
    assert seen[0] > 1
    assert stat.S_IMODE(f.stat().st_mode) == 0o600


async def test_not_touched_after_failed_tick(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    f = tmp_path / "h"
    calls = {"n": 0}
    done = asyncio.Event()

    async def tick(*a: Any, **kw: Any) -> None:
        calls["n"] += 1
        if calls["n"] == 1:
            os.utime(f, (1, 1))
        elif calls["n"] >= 2:
            done.set()
        raise ConnectionError("db down")

    monkeypatch.setattr(wd, "tick", tick)
    await _run(_cfg(watchdog_health_file=str(f)), done)
    assert f.stat().st_mtime == 1


async def test_unset_creates_no_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {"n": 0}
    done = asyncio.Event()

    async def tick(*a: Any, **kw: Any) -> None:
        calls["n"] += 1
        if calls["n"] >= 2:
            done.set()

    monkeypatch.setattr(wd, "tick", tick)
    monkeypatch.chdir(tmp_path)
    await _run(_cfg(), done)
    assert _names(tmp_path) == []


async def test_unwritable_path_does_not_stop_loop(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = {"n": 0}
    done = asyncio.Event()

    async def tick(*a: Any, **kw: Any) -> None:
        calls["n"] += 1
        if calls["n"] >= 3:
            done.set()

    monkeypatch.setattr(wd, "tick", tick)
    bad = tmp_path / "missing-dir" / "h"
    await _run(_cfg(watchdog_health_file=str(bad)), done)
    assert calls["n"] >= 3


async def test_symlink_is_refused_and_loop_continues(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = tmp_path / "target"
    target.write_text("x")
    os.utime(target, (1, 1))
    link = tmp_path / "h"
    link.symlink_to(target)
    calls = {"n": 0}
    done = asyncio.Event()

    async def tick(*a: Any, **kw: Any) -> None:
        calls["n"] += 1
        if calls["n"] >= 3:
            done.set()

    monkeypatch.setattr(wd, "tick", tick)
    await _run(_cfg(watchdog_health_file=str(link)), done)
    assert target.stat().st_mtime == 1  # never followed
    assert target.read_text() == "x"
