from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
from botocore.exceptions import ClientError
from minion_cli import secrets
from minion_cli.errors import EXIT_UNREACHABLE, EXIT_USAGE, CliError
from minion_cli.settings import CliSettings
from minion_cli.store import ObjectStore, S3Store, _wrap
from minion_cli.testing import FakeStore


def test_settings_defaults_and_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MINION_CLI_SERVER", "http://localhost:8080")
    monkeypatch.setenv("MINION_CLI_S3_SECRET_KEY", "hunter2")
    s = CliSettings()
    assert s.server == "http://localhost:8080"
    assert s.project == "default"
    assert s.input_prefix == "inputs/"
    assert "hunter2" not in repr(s)


@pytest.mark.parametrize("field", ["SERVER", "S3_ENDPOINT_URL"])
def test_settings_refuse_insecure_urls(monkeypatch: pytest.MonkeyPatch, field: str) -> None:
    monkeypatch.setenv(f"MINION_CLI_{field}", "http://example.com")
    with pytest.raises(ValueError, match="insecure"):
        CliSettings()


def test_settings_empty_is_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MINION_CLI_SERVER", "")
    assert CliSettings().server is None


def test_settings_ignore_env_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (tmp_path / ".env").write_text("MINION_CLI_TEAM=leaked\n")
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("MINION_CLI_TEAM", raising=False)
    assert CliSettings().team is None


def test_api_key_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MINION_API_KEY", " k-123 ")
    assert secrets.load_api_key().get_secret_value() == "k-123"


def test_api_key_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MINION_API_KEY", raising=False)
    monkeypatch.setattr(secrets, "keychain_api_key", lambda: None)
    with pytest.raises(CliError) as ei:
        secrets.load_api_key()
    assert ei.value.exit_code == EXIT_USAGE


def test_keychain_uses_fixed_argv_and_minimal_env(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: dict[str, object] = {}

    def fake_run(argv: list[str], **kw: object) -> subprocess.CompletedProcess[str]:
        seen["argv"], seen["env"], seen["timeout"] = argv, kw["env"], kw["timeout"]
        return subprocess.CompletedProcess(argv, 0, stdout="from-keychain\n")

    monkeypatch.setattr(sys, "platform", "darwin")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "nope")
    monkeypatch.setattr(subprocess, "run", fake_run)
    key = secrets.keychain_api_key()
    assert key is not None and key.get_secret_value() == "from-keychain"
    assert seen["argv"] == [
        "/usr/bin/security",
        "find-generic-password",
        "-s",
        "open-minions-api-key",
        "-w",
    ]
    assert "AWS_SECRET_ACCESS_KEY" not in seen["env"]  # type: ignore[operator]
    assert seen["timeout"] == 10


def test_keychain_skipped_off_darwin(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "platform", "linux")
    assert secrets.keychain_api_key() is None


def test_keychain_failure_is_none(monkeypatch: pytest.MonkeyPatch) -> None:
    def boom(*a: object, **k: object) -> None:
        raise subprocess.TimeoutExpired("security", 10)

    monkeypatch.setattr(sys, "platform", "darwin")
    monkeypatch.setattr(subprocess, "run", boom)
    assert secrets.keychain_api_key() is None


def _client_error(status: int) -> ClientError:
    return ClientError({"ResponseMetadata": {"HTTPStatusCode": status}}, "Op")  # type: ignore[typeddict-item]


@pytest.mark.parametrize(
    ("exc", "code"),
    [
        (_client_error(403), EXIT_USAGE),
        (_client_error(404), EXIT_USAGE),
        (_client_error(503), EXIT_UNREACHABLE),
        (OSError("conn"), EXIT_UNREACHABLE),
    ],
)
def test_store_error_mapping(exc: Exception, code: int) -> None:
    assert _wrap(exc, "x").exit_code == code


def test_s3_store_builds_path_style_client() -> None:
    s = CliSettings(
        s3_endpoint_url="http://localhost:8333",
        s3_access_key="a",
        s3_secret_key="b",
    )
    store = S3Store(s)
    cfg: Any = store._s3.meta.config
    assert cfg.s3 == {"addressing_style": "path"}
    assert cfg.signature_version == "s3v4"
    assert cfg.request_checksum_calculation == "when_required"


async def test_fake_store_roundtrip(tmp_path: Path) -> None:
    store: ObjectStore = FakeStore()
    f = tmp_path / "a.jsonl"
    f.write_bytes(b'{"a":1}\n')
    await store.upload_file(f, "p/a")
    assert [(o.key, o.size) for o in await store.list_objects("p/")] == [("p/a", 8)]
    await store.download_file("p/a", tmp_path / "out" / "a")
    assert (tmp_path / "out" / "a").read_bytes() == f.read_bytes()
