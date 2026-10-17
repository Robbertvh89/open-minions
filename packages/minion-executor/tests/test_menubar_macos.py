"""Builds the real status item and menu (no run loop). Auto-skipped off macOS."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from minion_common.enums import CapacityProfile
from minion_executor.capacity.control import OwnerControl, read_control
from minion_executor.capacity.safefile import ensure_private_dir

pytestmark = pytest.mark.macos


def _titles(app: Any) -> list[str]:
    menu = app._menu
    return [str(menu.itemAtIndex_(i).title()) for i in range(menu.numberOfItems())]


def _empty_config(tmp_path: Path) -> Path:
    cfg = tmp_path / "executor.toml"  # not the real user's config
    cfg.write_text("")
    cfg.chmod(0o600)
    return cfg


def test_status_item_and_menu_build_and_act(tmp_path: Path) -> None:
    from minion_executor.menubar.app import MenuBarApp

    sd = tmp_path / "state"
    ensure_private_dir(sd)
    app = MenuBarApp(sd, _empty_config(tmp_path))
    try:
        titles = _titles(app)
        assert "Pause" in titles and "Resume" in titles and "Profile" in titles
        assert len(app._actions) == 4 + 1 + 4  # pause choices, resume, automatic + 3 built-ins
        app.perform(0)  # "30 minutes"
        ctl = read_control(sd)
        assert isinstance(ctl, OwnerControl) and ctl.paused and ctl.paused_until is not None
        assert any("Paused by you until" in t for t in _titles(app))  # no executor running
    finally:
        app.stop()


def test_unconfigured_profile_is_refused_with_a_notice(tmp_path: Path) -> None:
    from minion_executor.menubar.app import MenuBarApp
    from minion_executor.menubar.model import Action, Kind

    sd = tmp_path / "state"
    ensure_private_dir(sd)
    app = MenuBarApp(sd, _empty_config(tmp_path))
    try:
        app._actions.append(Action(Kind.PROFILE, profile=CapacityProfile.CUSTOM))
        app.perform(len(app._actions) - 1)
        assert read_control(sd) is None  # nothing written
        assert any("not configured" in t for t in _titles(app))
    finally:
        app.stop()
