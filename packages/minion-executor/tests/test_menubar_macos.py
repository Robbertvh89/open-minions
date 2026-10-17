"""Builds the real status item and menu (no run loop). Auto-skipped off macOS."""

from __future__ import annotations

from pathlib import Path

import pytest
from minion_executor.capacity.control import OwnerControl, read_control
from minion_executor.capacity.safefile import ensure_private_dir

pytestmark = pytest.mark.macos


def test_status_item_and_menu_build_and_act(tmp_path: Path) -> None:
    from minion_executor.menubar.app import MenuBarApp

    sd = tmp_path / "state"
    ensure_private_dir(sd)
    app = MenuBarApp(sd)
    try:
        n = app._menu.numberOfItems()
        titles = [str(app._menu.itemAtIndex_(i).title()) for i in range(n)]
        assert "Pause" in titles and "Resume" in titles and "Profile" in titles
        assert len(app._actions) == 4 + 1 + 5  # pause choices, resume, profiles
        app.perform(0)  # "30 minutes"
        ctl = read_control(sd)
        assert isinstance(ctl, OwnerControl) and ctl.paused and ctl.paused_until is not None
    finally:
        app.stop()
