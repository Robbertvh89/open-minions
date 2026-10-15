"""Sandbox profile (pure, runs everywhere) and `apply` failure handling."""

from __future__ import annotations

import os
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


METAL = "/private/var/folders/xx/yyyy/C"


def _profile(port: int | None = None) -> str:
    return build_profile(HOME, [MODEL, "/Users/alice/proj/.venv"], [TMP], port, metal_cache=METAL)


def _lines(p: str | None = None) -> list[str]:
    return (p or _profile()).splitlines()


def test_home_denied_before_allow_list() -> None:
    lines = _lines()
    deny = f'(deny file-read* (subpath "{HOME}"))'
    assert deny in lines
    allow = f'(allow file-read* (subpath "{MODEL}"))'
    assert lines.index(deny) < lines.index(allow)  # last match wins in SBPL


def test_read_roots_denied_before_grants() -> None:
    lines = _lines()
    for root in ("/Users", "/Volumes", "/private/tmp", "/private/var/folders", "/Library"):
        deny = f'(deny file-read* (subpath "{root}"))'
        assert deny in lines, root
        assert lines.index(deny) < lines.index(f'(allow file-read* (subpath "{MODEL}"))')
    # only the model dir is granted, never the cache root
    assert f'(allow file-read* (subpath "{CACHE}"))' not in lines


def test_writes_denied_everywhere_except_tmp_dev_and_metal_cache() -> None:
    lines = _lines()
    deny = '(deny file-write* (subpath "/"))'
    assert deny in lines
    allows = [ln for ln in lines if ln.startswith("(allow") and "file-write*" in ln]
    for ln in allows:
        assert lines.index(deny) < lines.index(ln)
    granted = " ".join(allows)
    assert TMP in granted and "/dev/null" in granted and "com\\.apple\\.metal" in granted
    assert MODEL not in granted and "/Users/alice" not in granted.replace(TMP, "")


def test_metal_cache_regex_is_scoped_to_python_owner() -> None:
    (rx,) = [ln for ln in _lines() if "regex" in ln]
    assert METAL in rx and "org\\.python\\.python" in rx and "com\\.apple\\.metal" in rx
    assert "[^/]+" not in rx  # not any app's cache
    assert "regex" not in build_profile(HOME, [], [TMP])  # absent without a cache dir


def test_escapes_denied() -> None:
    lines = _lines()
    for rule in (
        "(deny lsopen)",
        "(deny appleevent-send)",
        "(deny distributed-notification-post)",
        "(deny user-preference-read user-preference-write)",
        "(deny signal)",
        "(allow signal (target self))",
        "(deny mach-lookup)",
    ):
        assert rule in lines, rule
    assert lines.index("(deny signal)") < lines.index("(allow signal (target self))")


def test_mach_lookup_is_deny_default_with_small_allow_list() -> None:
    lines = _lines()
    (allow,) = [ln for ln in lines if ln.startswith("(allow mach-lookup")]
    assert lines.index("(deny mach-lookup)") < lines.index(allow)
    for svc in sandbox.MACH_ALLOW:
        assert f'(global-name "{svc}")' in allow
    for bad in (
        "launchservicesd",
        "appleevents",
        "pasteboard",
        "nsurlsessiond",
        "windowserver",
        "metadata.mds",
        "tccd",
        "SecurityServer",
        "secd",
        "trustd",
    ):
        assert not any(bad in svc for svc in sandbox.MACH_ALLOW), bad
    assert "global-name-prefix" not in allow


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


def test_exec_denied() -> None:
    assert "(deny process-exec*)" in _profile()


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
    assert '(deny file-read* (subpath "/Users/a"))' in p
    assert '(deny file-read* (subpath "/private/x/a"))' in p


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


def _spec(policy: sandbox.SandboxPolicy, model: Path | None, tmp: Path) -> dict[str, object]:
    return sandbox.sandbox_spec(policy, model_path=model, tmp_dir=tmp, engines=["fake"])


def test_spec_grants_model_dir_not_cache_root(tmp_path: Path) -> None:
    cache = tmp_path / "cache"
    model = cache / "hf" / "o--m" / ("a" * 40)
    model.mkdir(parents=True)
    spec = _spec(sandbox.SandboxPolicy(cache_root=cache), model, tmp_path / "t")
    read = spec["read"]
    assert isinstance(read, list)
    assert os.path.realpath(model) in read and os.path.realpath(cache) not in read


def test_spec_rejects_cache_dirs_that_expose_home(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    home = tmp_path / "home"
    (home / "Library" / "Keychains").mkdir(parents=True)
    monkeypatch.setattr(sandbox, "home_dirs", lambda: [str(home.resolve())])
    bad_dirs = [home, tmp_path, home.parent, home / "Library", home / "Library" / "Keychains"]
    for bad in [*bad_dirs, Path("/")]:
        with pytest.raises(RunnerError) as ei:
            _spec(sandbox.SandboxPolicy(cache_root=bad), None, tmp_path)
        assert ei.value.code == sandbox.SANDBOX_FAILED, bad
        with pytest.raises(RunnerError):
            _spec(sandbox.SandboxPolicy(), bad, tmp_path)
    good = home / "Library" / "Caches" / "open-minions"
    good.mkdir(parents=True)
    _spec(sandbox.SandboxPolicy(cache_root=good), good, tmp_path)


def test_spec_ollama_port_passthrough(tmp_path: Path) -> None:
    spec = sandbox.sandbox_spec(
        sandbox.SandboxPolicy(),
        model_path=None,
        tmp_dir=tmp_path,
        engines=["ollama"],
        ollama_port=11434,
    )
    assert spec["ollama_port"] == 11434 and spec["engines"] == ["ollama"]
    assert _spec(sandbox.SandboxPolicy(), None, tmp_path)["ollama_port"] is None


def test_python_read_paths_are_limited(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import sysconfig

    site_dir = tmp_path / "site"
    src = tmp_path / "editable-src"
    cwd = tmp_path / "cwd"
    for d in (site_dir, src, cwd):
        d.mkdir()
    (site_dir / "x.pth").write_text(f"# c\nimport os\n{src}\n/does/not/exist\n")
    monkeypatch.chdir(cwd)
    monkeypatch.setenv("PYTHONPATH", str(cwd))
    monkeypatch.setattr(sys, "path", ["", str(cwd), *sys.path])
    real = sysconfig.get_paths()
    monkeypatch.setattr(sysconfig, "get_paths", lambda *a, **k: {**real, "purelib": str(site_dir)})
    out = [os.path.realpath(p) for p in sandbox.python_read_paths()]
    assert os.path.realpath(src) in out and os.path.realpath(site_dir) in out
    assert os.path.realpath(cwd) not in out
    assert os.path.realpath(sys.prefix) in out and len(out) == len(set(out))


def test_apply_from_spec_off_macos_allows_fake_only(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "platform", "linux")
    sandbox.apply_from_spec({"engines": ["fake"]})
    for engines in (["mlx"], ["fake", "ollama"], []):
        with pytest.raises(RunnerError) as ei:
            sandbox.apply_from_spec({"engines": engines})
        assert ei.value.code == sandbox.SANDBOX_FAILED


def test_self_test_exits_when_sandbox_is_ineffective(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    class ExitCalledError(Exception):
        pass

    def fake_exit(code: int) -> None:
        raise ExitCalledError(code)

    monkeypatch.setattr(os, "_exit", fake_exit)
    monkeypatch.setattr(os, "open", lambda *_a, **_k: 99)  # the outside write "succeeds"
    monkeypatch.setattr(os, "close", lambda _fd: None)
    monkeypatch.setattr(Path, "unlink", lambda *_a, **_k: None)
    with pytest.raises(ExitCalledError) as ei:  # HOME is listable too (tmp_path)
        sandbox._self_test([str(tmp_path)])
    assert ei.value.args == (sandbox.SELF_TEST_EXIT_CODE,)


def test_self_test_passes_when_both_fail(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    def deny(*_a: object, **_k: object) -> None:
        raise PermissionError

    def no_exit(_c: int) -> None:
        pytest.fail("must not exit")

    monkeypatch.setattr(os, "listdir", deny)
    monkeypatch.setattr(os, "open", deny)
    monkeypatch.setattr(os, "_exit", no_exit)
    sandbox._self_test([str(tmp_path)])
