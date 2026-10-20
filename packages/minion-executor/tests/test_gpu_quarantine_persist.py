"""Persisted GPU quarantine (TASK-43.1): survives a restart, 0600, safe-dir rules, corrupt files."""

from __future__ import annotations

import json
import stat
import sys
from pathlib import Path

import pytest

# importlib import mode: make the sibling helper module importable by name
sys.path.insert(0, str(Path(__file__).parent))

from loop_fakes import BUDGET, FakeClient, FakeS3, FakeWorker, make_loop
from minion_common.enums import Engine
from minion_executor.capacity.gpu_quarantine import QUARANTINE_FILE
from minion_executor.capacity.runtime import FixedGovernor

WALL0 = 1_800_000_000.0


def _loop(tmp_path: Path, state: Path, mono: list[float], wall: list[float]):  # type: ignore[no-untyped-def]
    return make_loop(
        FakeClient([]), FakeWorker(), FakeS3(), tmp_path, [], governor=FixedGovernor(BUDGET),
        clock=lambda: mono[0], wall_clock=lambda: wall[0], status_dir=state,
    )  # fmt: skip


def _state(tmp_path: Path) -> Path:
    d = tmp_path / "state"
    d.mkdir(mode=0o700)
    return d


async def test_quarantine_survives_restart_with_remaining_time(tmp_path: Path) -> None:
    state = _state(tmp_path)
    mono, wall = [1000.0], [WALL0]
    a = _loop(tmp_path, state, mono, wall)
    for _ in range(3):
        a._note_gpu_kill(Engine.MLX)
    assert a._acquirable_engines() == []
    f = state / QUARANTINE_FILE
    assert stat.S_IMODE(f.stat().st_mode) == 0o600
    # restart 30 min later: new process, new monotonic origin
    wall[0] += 1800
    b = _loop(tmp_path, state, [5.0], wall)
    assert b._quarantined() == [Engine.MLX]
    assert b._acquirable_engines() == []
    # the oldest kill ages out on the original schedule (30 more minutes)
    wall[0] += 1801
    c = _loop(tmp_path, state, [9.0], wall)
    assert c._acquirable_engines() == [Engine.MLX]


async def test_meter_kills_persist_separately(tmp_path: Path) -> None:
    state = _state(tmp_path)
    mono, wall = [1000.0], [WALL0]
    a = _loop(tmp_path, state, mono, wall)
    for _ in range(6):
        a._note_gpu_kill(Engine.MLX, meter=True)
    b = _loop(tmp_path, state, [1.0], wall)
    assert b._gpu_kills == {} and len(b._gpu_meter_kills[Engine.MLX]) == 6
    assert b._quarantined() == [Engine.MLX]


@pytest.mark.parametrize(
    "content",
    [
        b"not json",
        b"[]",
        b'{"version":1,"engines":{"mlx":{"duty":["x"]}}}',
        b'{"version":1,"engines":{"mlx":{"duty":[1],"bogus":1}}}',
        b'{"version":1,"engines":{"mlx":{"duty":[' + b"1," * 100 + b"1]}}}",
    ],
)
async def test_corrupt_file_is_empty_not_a_crash(tmp_path: Path, content: bytes) -> None:
    state = _state(tmp_path)
    (state / QUARANTINE_FILE).write_bytes(content)
    (state / QUARANTINE_FILE).chmod(0o600)
    loop = _loop(tmp_path, state, [100.0], [WALL0])
    assert loop._quarantined() == []
    assert loop._gpu_kills == {} and loop._gpu_meter_kills == {}


async def test_future_dated_and_old_entries_never_extend(tmp_path: Path) -> None:
    state = _state(tmp_path)
    body = {
        "version": 1,
        "engines": {
            "mlx": {
                "duty": [WALL0 + 10, WALL0 + 10**9, WALL0 - 7200, WALL0 - 7200],
                "meter": [WALL0 + 5] * 10,
            },
            "nonsense-engine": {"duty": [WALL0 - 1] * 5},
        },
    }
    (state / QUARANTINE_FILE).write_text(json.dumps(body))
    (state / QUARANTINE_FILE).chmod(0o600)
    loop = _loop(tmp_path, state, [100.0], [WALL0])
    assert loop._quarantined() == []
    assert loop._gpu_kills == {} and loop._gpu_meter_kills == {}


async def test_loose_file_mode_is_ignored(tmp_path: Path) -> None:
    state = _state(tmp_path)
    body = {"version": 1, "engines": {"mlx": {"duty": [WALL0 - 5] * 3}}}
    (state / QUARANTINE_FILE).write_text(json.dumps(body))
    (state / QUARANTINE_FILE).chmod(0o666)
    assert _loop(tmp_path, state, [100.0], [WALL0])._quarantined() == []


async def test_unsafe_state_dir_is_refused(tmp_path: Path) -> None:
    state = _state(tmp_path)
    a = _loop(tmp_path, state, [100.0], [WALL0])
    for _ in range(3):
        a._note_gpu_kill(Engine.MLX)
    assert (state / QUARANTINE_FILE).exists()
    state.chmod(0o777)
    try:
        assert _loop(tmp_path, state, [100.0], [WALL0])._quarantined() == []  # not read
        a._note_gpu_kill(Engine.MLX)  # not written, not raised
        assert a._quarantine_note is not None
    finally:
        state.chmod(0o700)


async def test_symlink_file_is_not_followed(tmp_path: Path) -> None:
    state = _state(tmp_path)
    target = tmp_path / "elsewhere.json"
    target.write_text(json.dumps({"version": 1, "engines": {"mlx": {"duty": [WALL0 - 5] * 3}}}))
    (state / QUARANTINE_FILE).symlink_to(target)
    assert _loop(tmp_path, state, [100.0], [WALL0])._quarantined() == []


async def test_no_state_dir_writes_nothing(tmp_path: Path) -> None:
    loop = make_loop(
        FakeClient([]), FakeWorker(), FakeS3(), tmp_path, [], governor=FixedGovernor(BUDGET)
    )
    for _ in range(3):
        loop._note_gpu_kill(Engine.MLX)
    assert loop._quarantined() == [Engine.MLX]
    assert not (tmp_path / QUARANTINE_FILE).exists()


async def test_unknown_version_is_invalid(tmp_path: Path) -> None:
    state = _state(tmp_path)
    body = {"version": 2, "engines": {"mlx": {"duty": [WALL0 - 5] * 3}}}
    (state / QUARANTINE_FILE).write_text(json.dumps(body))
    (state / QUARANTINE_FILE).chmod(0o600)
    assert _loop(tmp_path, state, [100.0], [WALL0])._quarantined() == []
