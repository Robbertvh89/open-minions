"""Per-team live-task cap: settings, keys-file policy, precedence (TASK-30, FR-9, ADR-0020)."""

import hashlib

import pytest
from minion_server.auth import TeamPolicy, parse_keys_config, resolve_pool
from minion_server.services.tasks import live_task_cap
from minion_server.settings import ServerSettings
from pydantic import SecretStr, ValidationError

H = hashlib.sha256(b"k").hexdigest()
HEAD = f"keys:\n  - {{name: a, sha256: {H}, teams: [t1]}}\n"


def _settings(**kw: object) -> ServerSettings:
    return ServerSettings(
        database_url=SecretStr("postgresql://x"),
        s3_access_key=SecretStr("a"),
        s3_secret_key=SecretStr("b"),
        **kw,  # type: ignore[arg-type]
    )


def test_default_and_bounds() -> None:
    assert _settings().max_live_tasks_per_team == 500
    for bad in (0, 100_001):
        with pytest.raises(ValidationError):
            _settings(max_live_tasks_per_team=bad)
    assert _settings(max_live_tasks_per_team=100_000).max_live_tasks_per_team == 100_000


def test_env_name(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MINION_MAX_LIVE_TASKS_PER_TEAM", "7")
    monkeypatch.setenv("MINION_DATABASE_URL", "postgresql://x")
    monkeypatch.setenv("MINION_S3_ACCESS_KEY", "a")
    monkeypatch.setenv("MINION_S3_SECRET_KEY", "b")
    assert ServerSettings().max_live_tasks_per_team == 7


def test_keys_file_override_parses() -> None:
    cfg = parse_keys_config(HEAD + "teams:\n  t1: {max_live_tasks: 3}\n")
    assert cfg.teams["t1"].max_live_tasks == 3


@pytest.mark.parametrize("bad", ["true", "0", "-1", "'5'", "2.5", "100001"])
def test_keys_file_rejects_bad_value(bad: str) -> None:
    with pytest.raises(ValueError):
        parse_keys_config(HEAD + f"teams:\n  t1: {{max_live_tasks: {bad}}}\n")


def test_pool_only_entry_unchanged_and_cap_only_keeps_pool_semantics() -> None:
    cfg = parse_keys_config(HEAD + "teams:\n  t1: {default_pool: p}\n  t2: {max_live_tasks: 9}\n")
    assert cfg.teams["t1"].max_live_tasks is None
    assert resolve_pool(cfg.teams["t2"], "anything") == "anything"
    assert resolve_pool(cfg.teams["t2"], None) is None


def test_live_task_cap_precedence() -> None:
    s = _settings(max_live_tasks_per_team=10)
    assert live_task_cap(None, s) == 10
    assert live_task_cap(TeamPolicy(), s) == 10
    assert live_task_cap(TeamPolicy(max_live_tasks=3), s) == 3
