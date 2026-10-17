import subprocess
from typing import Any

import pytest
from minion_executor import secrets


def _fake_run(returncode: int, stdout: str, calls: list[list[str]]) -> Any:
    def run(argv: list[str], **kw: Any) -> subprocess.CompletedProcess[str]:
        calls.append(argv)
        return subprocess.CompletedProcess(argv, returncode, stdout, "")

    return run


def test_env_token_wins_without_keychain(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[list[str]] = []
    monkeypatch.setenv(secrets.ENV_VAR, " envtok \n")
    monkeypatch.setattr(subprocess, "run", _fake_run(0, "kc\n", calls))
    assert secrets.load_token().get_secret_value() == "envtok"
    assert calls == []


def test_keychain_token_used_when_env_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[list[str]] = []
    monkeypatch.delenv(secrets.ENV_VAR, raising=False)
    monkeypatch.setattr(subprocess, "run", _fake_run(0, "kc-token\n", calls))
    tok = secrets.load_token()
    assert tok.get_secret_value() == "kc-token"
    assert calls == [
        ["/usr/bin/security", "find-generic-password", "-s", secrets.KEYCHAIN_SERVICE, "-w"]
    ]
    assert "kc-token" not in repr(tok)


@pytest.mark.parametrize("rc,out", [(44, ""), (0, "  \n")])
def test_keychain_miss_raises(monkeypatch: pytest.MonkeyPatch, rc: int, out: str) -> None:
    monkeypatch.delenv(secrets.ENV_VAR, raising=False)
    monkeypatch.setattr(subprocess, "run", _fake_run(rc, out, []))
    with pytest.raises(RuntimeError, match="no executor token"):
        secrets.load_token()


def test_security_binary_missing_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(secrets.ENV_VAR, raising=False)

    def boom(*a: Any, **k: Any) -> Any:
        raise FileNotFoundError

    monkeypatch.setattr(subprocess, "run", boom)
    with pytest.raises(RuntimeError, match="no executor token"):
        secrets.load_token()


@pytest.mark.parametrize(("rc", "transient"), [(44, False), (51, True), (36, True)])
def test_token_unavailable_flag(monkeypatch: pytest.MonkeyPatch, rc: int, transient: bool) -> None:
    monkeypatch.delenv(secrets.ENV_VAR, raising=False)
    monkeypatch.setattr(subprocess, "run", _fake_run(rc, "", []))
    with pytest.raises(secrets.TokenUnavailableError) as ei:
        secrets.load_token()
    assert ei.value.transient is transient
