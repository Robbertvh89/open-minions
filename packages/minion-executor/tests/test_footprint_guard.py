"""The pytest footprint guard's pure decision (root conftest.py, TASK-44)."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

from conftest import _check

_GB = 1024**3


def test_below_limit_does_not_trip() -> None:
    codes: list[int] = []
    assert not _check(2 * _GB, 16 * _GB, "t::a", codes.append)
    assert codes == []


def test_breach_exits_3_and_names_the_test(capsys: pytest.CaptureFixture[str]) -> None:
    codes: list[int] = []
    assert _check(20 * _GB, 16 * _GB, "t::runaway", codes.append)
    assert codes == [3]
    err = capsys.readouterr().err
    assert "t::runaway" in err
    assert "20.00 GB" in err


def test_unknown_footprint_or_disabled_limit_never_trips() -> None:
    codes: list[int] = []
    assert not _check(None, 16 * _GB, "t::a", codes.append)
    assert not _check(99 * _GB, 0, "t::a", codes.append)
    assert codes == []


def test_guard_aborts_a_real_pytest_run_visibly(tmp_path: Path) -> None:
    """End to end under default fd capture: rc 3 and the message reaches the real stderr."""
    from minion_executor.worker.footprint import process_footprint_bytes

    if process_footprint_bytes(os.getpid()) is None:
        pytest.skip("no footprint reader on this OS")
    (tmp_path / "test_slow.py").write_text(
        "import time\n\n"
        "def test_slow():\n"
        "    b = b'\\x01' * (700 << 20)\n"
        "    time.sleep(3)\n"
        "    assert b\n"
    )
    root = Path(__file__).resolve().parents[3]
    env = {
        k: v for k, v in os.environ.items() if k not in ("MINION_TEST_RSS_LOG", "PYTEST_ADDOPTS")
    }
    env["MINION_TEST_MAX_FOOTPRINT_GB"] = "0.5"  # trips only inside the test's 700 MB allocation
    proc = subprocess.run(  # noqa: S603
        [
            sys.executable, "-m", "pytest", "-p", "no:cacheprovider", "--rootdir", str(root),
            "--confcutdir", str(root), "-c", str(root / "pyproject.toml"),
            "--import-mode=importlib", "-p", "conftest", str(tmp_path / "test_slow.py"),
        ],
        cwd=root, env=env, capture_output=True, text=True, timeout=120,
    )  # fmt: skip
    out = proc.stdout + proc.stderr
    assert proc.returncode == 3, out
    assert "FOOTPRINT GUARD" in proc.stderr and "::test_slow reached" in proc.stderr, out
