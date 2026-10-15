"""Sandbox profile (pure, runs everywhere) and `apply` failure handling."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
from minion_executor.runners.base import RunnerError
from minion_executor.settings import ExecutorSettings
from minion_executor.worker import sandbox
from minion_executor.worker.sandbox import build_profile, resolve_paths, sbpl_quote

HOME = "/Users/alice"
CACHE = "/Users/alice/Library/Caches/open-minions/models"
MODEL = CACHE + "/hf/o--m/" + "a" * 40
TMP = "/private/var/folders/xx/T/minion-worker-abc"


def _profile(port: int | None = None) -> str:
    return build_profile(HOME, [CACHE, MODEL, "/Users/alice/proj/.venv"], [TMP], port)


def test_home_denied_before_allow_list() -> None:
    lines = _profile().splitlines()
    deny = f'(deny file-read* file-write* (subpath "{HOME}"))'
    assert deny in lines
    allow = f'(allow file-read* (subpath "{CACHE}"))'
    assert lines.index(deny) < lines.index(allow)  # last match wins in SBPL


def test_cache_is_read_only() -> None:
    lines = _profile().splitlines()
    assert f'(allow file-read* (subpath "{CACHE}"))' in lines
    assert f'(deny file-write* (subpath "{CACHE}"))' in lines
    assert not any("file-write*" in ln and "allow" in ln and CACHE in ln for ln in lines)
    # the worker tmp dir is the one writable place, and it comes after the write denies
    rw = f'(allow file-read* file-write* (subpath "{TMP}"))'
    assert lines.index(rw) > lines.index(f'(deny file-write* (subpath "{CACHE}"))')


def test_network_denied_and_ollama_port_only_when_requested() -> None:
    assert "(deny network*)" in _profile().splitlines()
    assert "network-outbound" not in _profile()
    with_port = _profile(11434)
    assert '(allow network-outbound (remote tcp "localhost:11434"))' in with_port
    assert with_port.index("(deny network*)") < with_port.index("network-outbound")
    assert "(allow network*" not in with_port


@pytest.mark.parametrize("port", [0, -1, 70000])
def test_bad_port_rejected(port: int) -> None:
    with pytest.raises(ValueError, match="port"):
        _profile(port)


def test_exec_and_securityd_denied() -> None:
    p = _profile()
    assert "(deny process-exec*)" in p
    assert '(global-name "com.apple.SecurityServer")' in p
    assert '(global-name "com.apple.securityd.xpc")' in p


def test_paths_with_quotes_and_backslashes_are_escaped() -> None:
    nasty = '/Users/alice/a"b\\c'
    p = build_profile(HOME, [nasty], [])
    assert '(subpath "/Users/alice/a\\"b\\\\c")' in p
    assert nasty not in p  # never appears unescaped
    assert sbpl_quote('a"b\\') == '"a\\"b\\\\"'


def test_control_characters_rejected() -> None:
    for bad in ("/x\n(allow default)", "/x\x00", "/x\r"):
        with pytest.raises(ValueError, match="control"):
            build_profile(HOME, [bad], [])


def test_ancestors_of_allowed_paths_get_metadata_only() -> None:
    p = _profile()
    assert f'(allow file-read-metadata (literal "{HOME}/Library"))' in p
    assert f'(allow file-read-metadata (literal "{HOME}"))' in p
    assert f'(allow file-read* (subpath "{HOME}/Library"))' not in p  # metadata, not contents


def test_multiple_homes_all_denied() -> None:
    p = build_profile(["/Users/a", "/private/x/a"], [], [])
    assert '(deny file-read* file-write* (subpath "/Users/a"))' in p
    assert '(deny file-read* file-write* (subpath "/private/x/a"))' in p


def test_resolve_paths_drops_home_ancestors_and_root(tmp_path: Path) -> None:
    home = tmp_path / "home"
    (home / "venv").mkdir(parents=True)
    out = resolve_paths([home, tmp_path, "/", home / "venv"], home=[str(home)])
    assert out == [str((home / "venv").resolve())]


def test_apply_without_libsandbox_fails_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "platform", "darwin")

    def boom() -> object:
        raise OSError("no libsandbox")

    monkeypatch.setattr(sandbox, "_load_libsandbox", boom)
    with pytest.raises(RunnerError) as ei:
        sandbox.apply("(version 1)")
    assert ei.value.code == sandbox.SANDBOX_FAILED


def test_apply_error_from_sandbox_init_fails_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "platform", "darwin")
    freed: list[object] = []

    class Lib:
        def sandbox_init(self, _profile: bytes, _flags: int, err: object) -> int:
            err._obj.value = b"syntax error"  # type: ignore[attr-defined]
            return -1

        def sandbox_free_error(self, e: object) -> None:
            freed.append(e)

    monkeypatch.setattr(sandbox, "_load_libsandbox", lambda: Lib())
    with pytest.raises(RunnerError) as ei:
        sandbox.apply("(bad")
    assert ei.value.code == sandbox.SANDBOX_FAILED and "syntax error" in ei.value.message
    assert len(freed) == 1


def test_apply_off_macos_refuses(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "platform", "linux")
    with pytest.raises(RunnerError):
        sandbox.apply("(version 1)")


def test_sandbox_setting_default_on_and_off_needs_dev(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MINION_EXECUTOR_DEV", raising=False)
    monkeypatch.delenv("MINION_EXECUTOR_SANDBOX", raising=False)
    assert ExecutorSettings().sandbox is True
    monkeypatch.setenv("MINION_EXECUTOR_SANDBOX", "0")
    with pytest.raises(ValueError, match="DEV"):
        ExecutorSettings()
    monkeypatch.setenv("MINION_EXECUTOR_DEV", "1")
    assert ExecutorSettings().sandbox is False
