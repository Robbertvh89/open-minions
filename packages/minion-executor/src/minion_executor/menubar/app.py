"""AppKit glue of the menu-bar app (macOS only; the logic lives in `model.py`).

A status item with a menu: status lines, Pause (30 min / 1 h / 2 h / until resume), Resume,
Profile (automatic / light / balanced / full / custom) and Quit. It reads `status.json` every
`POLL_INTERVAL_S` and writes only through `update_control`. No network, no token, no subprocess,
no power assertion: the app never keeps the Mac awake (invariant 6). `NSTimer` with a tolerance
lets the system coalesce wake-ups; the menu is rebuilt when the view changed (the "updated"
age line is excluded; volatile running fields such as progress still cause rebuilds, which is
cheap and the timer does not fire while the menu is tracking) and in `menuNeedsUpdate_`, just
before the menu opens, so the age is fresh when shown.
The guarantee is about this package's own code only (no network, token or subprocess code,
checked by a source test); the `minion-executor` entry point that starts it loads httpx anyway.
"""

from __future__ import annotations

import signal
from pathlib import Path
from typing import Any

import objc
from AppKit import (
    NSApplication,
    NSApplicationActivationPolicyAccessory,
    NSControlStateValueOn,
    NSMenu,
    NSMenuItem,
    NSObject,
    NSStatusBar,
    NSVariableStatusItemLength,
)
from Foundation import NSTimer

from minion_executor.capacity.control import read_control, update_control
from minion_executor.capacity.safefile import SafeFileError
from minion_executor.capacity.status import read_status, safe_text
from minion_executor.menubar.model import (
    PAUSE_CHOICES,
    POLL_INTERVAL_S,
    RESUME,
    Action,
    View,
    action_error,
    available_profiles,
    build_view,
    control_kwargs,
    profile_choices,
)


class _Target(NSObject):  # type: ignore[misc]
    """Objective-C target for menu items and the timer; forwards to the Python owner."""

    owner: MenuBarApp | None = None

    def act_(self, sender: Any) -> None:
        if self.owner is not None:
            self.owner.perform(int(sender.tag()))

    def tick_(self, _timer: Any) -> None:
        if self.owner is not None:
            self.owner.refresh()

    def menuNeedsUpdate_(self, _menu: Any) -> None:  # noqa: N802 (NSMenuDelegate selector)
        # called by AppKit just before the menu is shown (also while it is tracking)
        if self.owner is not None:
            self.owner.refresh(force=True)

    def quit_(self, _sender: Any) -> None:
        NSApplication.sharedApplication().terminate_(None)


class MenuBarApp:
    """Builds the status item and menu; `refresh` re-reads the status file."""

    def __init__(self, state_dir: Path, config_path: Path | None = None) -> None:
        self.state_dir = state_dir
        self.config_path = config_path
        self._actions: list[Action] = []
        self._view: View | None = None
        self._notice: str | None = None
        self._target = _Target.alloc().init()
        self._target.owner = self
        self._item = NSStatusBar.systemStatusBar().statusItemWithLength_(NSVariableStatusItemLength)
        self._menu = NSMenu.alloc().init()
        self._menu.setAutoenablesItems_(False)
        self._menu.setDelegate_(self._target)
        self._item.setMenu_(self._menu)
        self._timer: Any = None
        self.refresh()

    def start_timer(self) -> None:
        self._timer = NSTimer.scheduledTimerWithTimeInterval_target_selector_userInfo_repeats_(
            POLL_INTERVAL_S,
            self._target,
            objc.selector(self._target.tick_, signature=b"v@:@"),
            None,
            True,
        )
        self._timer.setTolerance_(POLL_INTERVAL_S / 2)

    def stop(self) -> None:
        if self._timer is not None:
            self._timer.invalidate()
            self._timer = None
        NSStatusBar.systemStatusBar().removeStatusItem_(self._item)

    def refresh(self, force: bool = False) -> None:
        control = read_control(self.state_dir)  # never raises: absent / valid / unusable
        try:
            view = build_view(read_status(self.state_dir), control=control)
        except (ValueError, SafeFileError, OSError) as exc:
            view = build_view(None, error=str(exc), control=control)
        if force or view != self._view:
            self._view = view
            self._rebuild(view)

    def perform(self, index: int) -> None:
        if not 0 <= index < len(self._actions):
            return
        self._notice = None
        action = self._actions[index]
        problem = action_error(action, available_profiles(self.config_path))
        try:
            if problem is not None:
                raise ValueError(problem)
            update_control(
                self.state_dir,
                warn=lambda m: setattr(self, "_notice", safe_text(m)[:200]),
                **control_kwargs(action),  # type: ignore[arg-type]
            )
        except (ValueError, OSError) as exc:  # SafeFileError is a ValueError
            self._notice = safe_text(f"Action failed: {exc}")[:200]
        self.refresh(force=True)  # shows the notice

    # menu construction -------------------------------------------------------------------

    def _add(
        self,
        menu: Any,
        title: str,
        action: Action | None = None,
        *,
        enabled: bool = True,
        checked: bool = False,
    ) -> Any:
        sel = objc.selector(self._target.act_, signature=b"v@:@") if action else None
        item = NSMenuItem.alloc().initWithTitle_action_keyEquivalent_(title, sel, "")
        if action is not None:
            item.setTag_(len(self._actions))
            self._actions.append(action)
            item.setTarget_(self._target)
        item.setEnabled_(enabled and action is not None)
        if checked:
            item.setState_(NSControlStateValueOn)
        menu.addItem_(item)
        return item

    def _submenu(self, title: str, enabled: bool) -> Any:
        parent = NSMenuItem.alloc().initWithTitle_action_keyEquivalent_(title, None, "")
        sub = NSMenu.alloc().init()
        sub.setAutoenablesItems_(False)
        parent.setSubmenu_(sub)
        parent.setEnabled_(enabled)
        self._menu.addItem_(parent)
        return sub

    def _rebuild(self, view: View) -> None:
        self._menu.removeAllItems()
        self._actions = []
        self._item.button().setTitle_(view.title)
        for line in (*view.lines, *((view.updated,) if view.updated else ())):
            self._add(self._menu, line)
        if self._notice:
            self._add(self._menu, self._notice)
        self._menu.addItem_(NSMenuItem.separatorItem())
        sub = self._submenu("Pause", view.can_pause)
        for label, action in PAUSE_CHOICES:
            self._add(sub, label, action)
        self._add(self._menu, "Resume", RESUME, enabled=view.can_resume)
        sub = self._submenu("Profile", True)
        for label, action in profile_choices(available_profiles(self.config_path)):
            self._add(sub, label, action, checked=label == view.profile_checked)
        self._menu.addItem_(NSMenuItem.separatorItem())
        quit_item = NSMenuItem.alloc().initWithTitle_action_keyEquivalent_(
            "Quit menu bar", objc.selector(self._target.quit_, signature=b"v@:@"), "q"
        )
        quit_item.setTarget_(self._target)
        self._menu.addItem_(quit_item)


def run(state_dir: Path, config_path: Path | None = None) -> None:
    """Run the menu-bar app until Quit (blocks)."""
    app = NSApplication.sharedApplication()
    app.setActivationPolicy_(NSApplicationActivationPolicyAccessory)  # no Dock icon
    bar = MenuBarApp(state_dir, config_path)
    bar.start_timer()
    signal.signal(signal.SIGINT, signal.SIG_DFL)  # the Cocoa loop never runs Python handlers
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
    app.run()  # Quit terminates the process; the status item goes away with it
