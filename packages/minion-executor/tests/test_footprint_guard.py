"""The pytest footprint guard's pure decision (root conftest.py, TASK-44)."""

from __future__ import annotations

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
    assert "20.0 GB" in err


def test_unknown_footprint_or_disabled_limit_never_trips() -> None:
    codes: list[int] = []
    assert not _check(None, 16 * _GB, "t::a", codes.append)
    assert not _check(99 * _GB, 0, "t::a", codes.append)
    assert codes == []
