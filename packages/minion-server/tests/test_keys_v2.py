"""Keys file v2: live reload, fail-closed, expiry, operator role, pool policy (TASK-6.1)."""

import hashlib
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from fastapi import Depends, FastAPI, HTTPException
from httpx import ASGITransport, AsyncClient
from minion_server.auth import (
    KeyStore,
    TeamPolicy,
    User,
    current_user,
    parse_keys_config,
    require_operator,
    resolve_pool,
)
from pydantic import ValidationError
from structlog.testing import capture_logs

K1, K2 = "key-one-secret", "key-two-secret"


def _h(k: str) -> str:
    return hashlib.sha256(k.encode()).hexdigest()


def _write(path: Path, body: str, bump: int = 0) -> None:
    import os

    path.write_text(body)
    st = path.stat()
    os.utime(path, ns=(st.st_atime_ns, st.st_mtime_ns + bump))


def _entry(name: str, key: str, extra: str = "") -> str:
    return f"  - {{name: {name}, sha256: {_h(key)}, teams: [t1]{extra}}}\n"


class Clock:
    def __init__(self) -> None:
        self.t = 0.0

    def __call__(self) -> float:
        return self.t


def _app(store: KeyStore) -> FastAPI:
    app = FastAPI()
    app.state.api_keys = store

    @app.get("/u")
    async def u(user: User = Depends(current_user)) -> dict[str, str]:  # noqa: B008
        return {"name": user.name}

    @app.get("/op")
    async def op(user: User = Depends(require_operator)) -> dict[str, str]:  # noqa: B008
        return {"name": user.name}

    return app


async def _status(app: FastAPI, key: str, path: str = "/u") -> int:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        return (await c.get(path, headers={"Authorization": f"Bearer {key}"})).status_code


async def test_removed_key_rejected_after_reload_interval(tmp_path: Path) -> None:
    f = tmp_path / "k.yaml"
    f.write_text("keys:\n" + _entry("a", K1) + _entry("b", K2))
    clock = Clock()
    app = _app(KeyStore(f, reload_s=5, clock=clock))
    assert await _status(app, K1) == 200
    _write(f, "keys:\n" + _entry("b", K2), bump=1_000_000)
    clock.t = 4.9
    assert await _status(app, K1) == 200  # not checked yet
    clock.t = 6.0  # reload_s + 1
    assert await _status(app, K1) == 401
    assert await _status(app, K2) == 200


async def test_malformed_rewrite_fails_closed_and_recovers(tmp_path: Path) -> None:
    f = tmp_path / "k.yaml"
    good = "keys:\n" + _entry("a", K1)
    f.write_text(good)
    clock = Clock()
    app = _app(KeyStore(f, reload_s=5, clock=clock))
    assert await _status(app, K1) == 200
    _write(f, f"keys:\n  - {{name: a, sha256: {_h(K1)}, teams: [Bad Team]}}\n", bump=1)
    clock.t = 6
    with capture_logs() as logs:
        assert await _status(app, K1) == 401
        assert await _status(app, K2) == 401
    errors = [e for e in logs if e["log_level"] == "error"]
    assert errors and errors[0]["event"] == "api_keys_reload_failed"
    assert K1 not in str(errors) and _h(K1) not in str(errors)
    clock.t = 12
    assert await _status(app, K1) == 401  # unchanged invalid file: still closed
    _write(f, good, bump=2)
    clock.t = 18
    assert await _status(app, K1) == 200


async def test_garbage_yaml_and_missing_file_fail_closed(tmp_path: Path) -> None:
    f = tmp_path / "k.yaml"
    f.write_text("keys:\n" + _entry("a", K1))
    clock = Clock()
    app = _app(KeyStore(f, reload_s=1, clock=clock))
    _write(f, "keys: [unclosed\n", bump=1)
    clock.t = 2
    assert await _status(app, K1) == 401
    f.unlink()
    clock.t = 4
    assert await _status(app, K1) == 401
    f.write_text("keys:\n" + _entry("a", K1))
    clock.t = 6
    assert await _status(app, K1) == 200


def test_first_load_failure_raises(tmp_path: Path) -> None:
    f = tmp_path / "k.yaml"
    f.write_text("keys: nope\n")
    with pytest.raises(ValidationError):
        KeyStore(f)
    with pytest.raises(OSError):
        KeyStore(tmp_path / "missing.yaml")


def test_non_regular_file_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="regular"):
        KeyStore(tmp_path)  # a directory


async def test_symlinked_file_is_read(tmp_path: Path) -> None:
    real = tmp_path / "real.yaml"
    real.write_text("keys:\n" + _entry("a", K1))
    link = tmp_path / "link.yaml"
    link.symlink_to(real)
    assert await _status(_app(KeyStore(link)), K1) == 200


async def test_expired_key_rejected(tmp_path: Path) -> None:
    f = tmp_path / "k.yaml"
    past = (datetime.now(UTC) - timedelta(seconds=5)).isoformat()
    future = (datetime.now(UTC) + timedelta(days=1)).isoformat()
    f.write_text(
        "keys:\n"
        + _entry("old", K1, f", expires_at: '{past}'")
        + _entry("new", K2, f", expires_at: '{future}'")
    )
    app = _app(KeyStore(f))
    assert await _status(app, K1) == 401
    assert await _status(app, K2) == 200


def test_naive_expiry_rejected() -> None:
    with pytest.raises(ValidationError):
        parse_keys_config("keys:\n" + _entry("a", K1, ", expires_at: '2030-01-01T00:00:00'"))


async def test_require_operator(tmp_path: Path) -> None:
    f = tmp_path / "k.yaml"
    f.write_text("keys:\n" + _entry("op", K1, ", operator: true") + _entry("user", K2))
    app = _app(KeyStore(f))
    assert await _status(app, K1, "/op") == 200
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        r = await c.get("/op", headers={"Authorization": f"Bearer {K2}"})
        assert r.status_code == 403
        assert r.json()["detail"]["code"] == "OPERATOR_REQUIRED"
    assert await _status(app, "nope", "/op") == 401


def test_v1_file_parses_without_policies() -> None:
    cfg = parse_keys_config("keys:\n" + _entry("a", K1))
    assert cfg.teams == {}
    user = cfg.keys[_h(K1)]
    assert (user.operator, user.expires_at) == (False, None)


@pytest.mark.parametrize(
    "teams",
    [
        "t1: {default_pool: a, allowed_pools: [b]}",
        "t1: {allowed_pools: []}",
        "t1: {bogus: 1}",
        "Bad Team: {default_pool: a}",
    ],
)
def test_bad_team_policies_rejected(teams: str) -> None:
    with pytest.raises(ValidationError):
        parse_keys_config(f"teams:\n  {teams}\n")


def test_resolve_pool() -> None:
    both = TeamPolicy("a", frozenset({"a", "b"}))
    only_allowed = TeamPolicy(None, frozenset({"a"}))
    only_default = TeamPolicy("a", None)
    assert resolve_pool(None, None) is None
    assert resolve_pool(None, "x") == "x"
    assert resolve_pool(both, None) == "a"
    assert resolve_pool(both, "b") == "b"
    assert resolve_pool(only_default, "z") == "z"
    assert resolve_pool(only_default, None) == "a"
    for policy, req, status, code in [
        (both, "c", 403, "POOL_NOT_ALLOWED"),
        (only_allowed, None, 422, "POOL_REQUIRED"),
    ]:
        with pytest.raises(HTTPException) as ei:
            resolve_pool(policy, req)
        assert ei.value.status_code == status
        assert ei.value.detail["code"] == code  # type: ignore[index]


def test_store_config_teams_follow_reload(tmp_path: Path) -> None:
    f = tmp_path / "k.yaml"
    f.write_text("keys:\n" + _entry("a", K1) + "teams:\n  t1: {default_pool: p1}\n")
    clock = Clock()
    store = KeyStore(f, reload_s=1, clock=clock)
    assert store.config is not None
    pol = store.config.teams.get("t1")
    assert pol is not None and pol.default_pool == "p1"
    assert "other" not in store.config.teams
    _write(f, "keys:\n" + _entry("a", K1), bump=5)
    clock.t = 2
    assert store.config is not None and "t1" not in store.config.teams


async def test_transient_read_error_retries(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import minion_server.auth as auth

    f = tmp_path / "k.yaml"
    f.write_text("keys:\n" + _entry("a", K1))
    clock = Clock()
    app = _app(KeyStore(f, reload_s=1, clock=clock))
    real = auth._read_file
    calls = {"n": 0}

    def flaky(path: str | Path) -> bytes:
        calls["n"] += 1
        if calls["n"] == 1:
            raise OSError("EIO")
        return real(path)

    monkeypatch.setattr(auth, "_read_file", flaky)
    clock.t = 2
    assert await _status(app, K1) == 401
    clock.t = 4
    assert await _status(app, K1) == 200


async def test_policy_uses_single_snapshot_per_request(tmp_path: Path) -> None:
    from minion_server.auth import KeysConfig, request_keys_config

    f = tmp_path / "k.yaml"
    f.write_text("keys:\n" + _entry("a", K1) + "teams:\n  t1: {default_pool: p1}\n")
    store = KeyStore(f, reload_s=0)
    app = _app(store)

    @app.get("/snap")
    async def snap(cfg: KeysConfig = Depends(request_keys_config)) -> dict[str, str | None]:  # noqa: B008
        _write(f, "keys: nope\n", bump=7)  # file turns invalid after auth
        assert store.config is None  # a second reload would fail closed
        pol = cfg.teams.get("t1")
        return {"pool": pol.default_pool if pol else None}

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        r = await c.get("/snap", headers={"Authorization": f"Bearer {K1}"})
        assert (r.status_code, r.json()) == (200, {"pool": "p1"})
        r = await c.get("/snap", headers={"Authorization": f"Bearer {K1}"})
        assert r.status_code == 401

    @app.get("/nosnap")
    async def nosnap(cfg: KeysConfig = Depends(request_keys_config)) -> dict[str, str]:  # noqa: B008
        return {}

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        assert (await c.get("/nosnap")).status_code == 401


async def test_same_size_rewrite_with_restored_mtime_detected(tmp_path: Path) -> None:
    import os

    f = tmp_path / "k.yaml"
    f.write_text("keys:\n" + _entry("a", K1))
    st = f.stat()
    clock = Clock()
    app = _app(KeyStore(f, reload_s=1, clock=clock))
    assert await _status(app, K1) == 200
    other = _h(K2)
    with f.open("r+b") as fh:  # in place, same size
        fh.write(("keys:\n" + _entry("a", K2)).encode())
    os.utime(f, ns=(st.st_atime_ns, st.st_mtime_ns))
    assert f.stat().st_size == st.st_size and other
    clock.t = 2
    assert await _status(app, K1) == 401
    assert await _status(app, K2) == 200


def test_unsafe_mode_refused(tmp_path: Path) -> None:
    from minion_server.auth import KeysFileUnsafeError

    f = tmp_path / "k.yaml"
    f.write_text("keys:\n" + _entry("a", K1))
    f.chmod(0o666)
    with pytest.raises(KeysFileUnsafeError):
        KeyStore(f)
    f.chmod(0o664)
    with pytest.raises(KeysFileUnsafeError):
        KeyStore(f)
    f.chmod(0o644)
    KeyStore(f)


def test_foreign_owner_refused(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import os

    from minion_server.auth import KeysFileUnsafeError

    if os.geteuid() == 0:
        pytest.skip("root owns everything")
    f = tmp_path / "k.yaml"
    f.write_text("keys:\n" + _entry("a", K1))
    monkeypatch.setattr("minion_server.auth.os.geteuid", lambda: os.getuid() + 12345)
    with pytest.raises(KeysFileUnsafeError, match="owned"):
        KeyStore(f)


def test_writable_directory_refused(tmp_path: Path) -> None:
    from minion_server.auth import KeysFileUnsafeError

    d = tmp_path / "d"
    d.mkdir()
    f = d / "k.yaml"
    f.write_text("keys:\n" + _entry("a", K1))
    d.chmod(0o777)
    try:
        with pytest.raises(KeysFileUnsafeError, match="directory"):
            KeyStore(f)
        d.chmod(0o755)
        KeyStore(f)
    finally:
        d.chmod(0o755)


async def test_reload_fails_closed_on_unsafe_then_recovers(tmp_path: Path) -> None:
    f = tmp_path / "k.yaml"
    f.write_text("keys:\n" + _entry("a", K1))
    clock = Clock()
    app = _app(KeyStore(f, reload_s=1, clock=clock))
    assert await _status(app, K1) == 200
    f.chmod(0o666)
    clock.t = 2
    assert await _status(app, K1) == 401
    f.chmod(0o644)  # content unchanged: must recover (read failures are not remembered)
    clock.t = 4
    assert await _status(app, K1) == 200


async def test_change_during_parse_is_discarded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import minion_server.auth as auth

    f = tmp_path / "k.yaml"
    f.write_text("keys:\n" + _entry("a", K1))
    clock = Clock()
    app = _app(KeyStore(f, reload_s=1, clock=clock))
    _write(f, "keys:\n" + _entry("a", K2), bump=3)
    real_parse = auth.parse_keys_config

    def racing(text: str) -> auth.KeysConfig:
        cfg = real_parse(text)
        _write(f, "keys:\n" + _entry("a", K1) + _entry("b", K2), bump=9)
        return cfg

    monkeypatch.setattr(auth, "parse_keys_config", racing)
    clock.t = 2
    assert await _status(app, K2) == 401  # discarded; old config still serves
    monkeypatch.setattr(auth, "parse_keys_config", real_parse)
    clock.t = 4
    assert await _status(app, K2) == 200


def test_safe_detail_drops_user_chosen_keys() -> None:
    from minion_server.auth import _safe_detail

    secret = "pasted-secret-key"
    with pytest.raises(ValidationError) as ei:
        parse_keys_config(f"keys:\n  - {{name: a, sha256: {_h(K1)}, teams: [t1], {secret}: 1}}\n")
    detail = _safe_detail(ei.value)
    assert secret not in detail and "keys.0" in detail
    with pytest.raises(ValidationError) as ei2:
        parse_keys_config(f"teams:\n  t1: {{{secret}: 1}}\n")
    assert secret not in _safe_detail(ei2.value)


@pytest.mark.parametrize("val", ["'yes'", "1", "'true'"])
def test_operator_is_strict_bool(val: str) -> None:
    with pytest.raises(ValidationError):
        parse_keys_config("keys:\n" + _entry("a", K1, f", operator: {val}"))


def test_policy_for_unused_team_warns() -> None:
    with capture_logs() as logs:
        parse_keys_config("keys:\n" + _entry("a", K1) + "teams:\n  t1: {}\n  typo: {}\n")
    w = [e for e in logs if e["event"] == "api_keys_policy_for_unused_teams"]
    assert w and w[0]["teams"] == ["typo"]


async def test_concurrent_requests_during_rewrites(tmp_path: Path) -> None:
    import asyncio

    f = tmp_path / "k.yaml"
    f.write_text("keys:\n" + _entry("a", K1))
    app = _app(KeyStore(f, reload_s=0))
    stop = False

    async def writer() -> None:
        i = 0
        while not stop:
            i += 1
            tmp = tmp_path / "tmp.yaml"
            tmp.write_text("keys:\n" + _entry("a", K1) + _entry(f"n{i}", K2))
            tmp.replace(f)  # atomic
            await asyncio.sleep(0)

    w = asyncio.create_task(writer())
    results = await asyncio.gather(*[_status(app, K1) for _ in range(150)])
    stop = True
    await w
    # atomic rewrites always keep K1 valid; refresh is single-threaded by design
    assert set(results) == {200}
    f.write_text("keys:\n" + _entry("a", K2))
    assert await _status(app, K1) == 401
    assert await _status(app, K2) == 200


def test_fifo_fails_closed_without_hanging(tmp_path: Path) -> None:
    import os

    fifo = tmp_path / "k.yaml"
    os.mkfifo(fifo)
    with pytest.raises(ValueError, match="regular"):
        KeyStore(fifo)


def test_duplicate_yaml_keys_rejected() -> None:
    from minion_server.auth import DuplicateKeyError

    with pytest.raises(DuplicateKeyError):
        parse_keys_config("keys: []\nkeys: []\n")
    with pytest.raises(DuplicateKeyError):
        parse_keys_config("teams:\n  t1: {default_pool: a}\n  t1: {default_pool: b}\n")
    with pytest.raises(DuplicateKeyError):
        parse_keys_config(f"keys:\n  - {{name: a, name: b, sha256: {_h(K1)}, teams: [t1]}}\n")


def test_content_failure_logged_again_when_hash_changes(tmp_path: Path) -> None:
    f = tmp_path / "k.yaml"
    f.write_text("keys:\n" + _entry("a", K1))
    clock = Clock()
    store = KeyStore(f, reload_s=1, clock=clock)
    with capture_logs() as logs:
        for i, body in enumerate(["keys: nope\n", "keys: nope\n\n", "keys: nope\n\n"]):
            _write(f, body, bump=i + 1)
            clock.t += 2
            assert store.config is None
    assert [e["event"] for e in logs].count("api_keys_reload_failed") == 2


def _good(tmp_path: Path) -> Path:
    f = tmp_path / "k.yaml"
    f.write_text("keys:\n" + _entry("a", K1))
    return f


def test_world_writable_directory_rejected(tmp_path: Path) -> None:
    from minion_server.auth import KeysFileUnsafeError

    d = tmp_path / "d"
    d.mkdir()
    f = _good(d)
    KeyStore(f)
    d.chmod(0o777)
    try:
        with pytest.raises(KeysFileUnsafeError):
            KeyStore(f)
    finally:
        d.chmod(0o755)


def test_symlink_in_writable_dir_rejected(tmp_path: Path) -> None:
    from minion_server.auth import KeysFileUnsafeError

    safe = tmp_path / "safe"
    safe.mkdir()
    real = _good(safe)
    bad = tmp_path / "bad"
    bad.mkdir()
    (bad / "link.yaml").symlink_to(real)
    KeyStore(bad / "link.yaml")
    bad.chmod(0o777)
    try:
        with pytest.raises(KeysFileUnsafeError):
            KeyStore(bad / "link.yaml")
    finally:
        bad.chmod(0o755)


def test_symlinked_intermediate_directory_checked(tmp_path: Path) -> None:
    from minion_server.auth import KeysFileUnsafeError

    bad = tmp_path / "bad"
    bad.mkdir()
    _good(bad)
    bad.chmod(0o777)
    holder = tmp_path / "holder"
    holder.mkdir()
    (holder / "dirlink").symlink_to(bad)
    try:
        with pytest.raises(KeysFileUnsafeError):
            KeyStore(holder / "dirlink" / "k.yaml")
    finally:
        bad.chmod(0o755)


def test_symlink_loop_rejected(tmp_path: Path) -> None:
    (tmp_path / "a").symlink_to(tmp_path / "b")
    (tmp_path / "b").symlink_to(tmp_path / "a")
    with pytest.raises(OSError):
        KeyStore(tmp_path / "a")


def test_k8s_secret_layout_passes(tmp_path: Path) -> None:
    ts = tmp_path / "..2024_01_01"
    ts.mkdir()
    _good(ts)
    (tmp_path / "..data").symlink_to("..2024_01_01")
    (tmp_path / "keys.yaml").symlink_to("..data/k.yaml")
    assert KeyStore(tmp_path / "keys.yaml").config is not None


def test_dir_ownership_and_sticky_rules(monkeypatch: pytest.MonkeyPatch) -> None:
    import os
    import stat

    from minion_server.auth import KeysFileUnsafeError, _check_dir

    monkeypatch.setattr(os, "geteuid", lambda: 1000)

    def st(uid: int, mode: int) -> os.stat_result:
        return os.stat_result((stat.S_IFDIR | mode, 1, 1, 1, uid, 0, 0, 0, 0, 0))

    _check_dir("/", st(0, 0o755))  # Docker /run/secrets
    _check_dir("/", st(1000, 0o700))
    _check_dir("/", st(0, 0o1777))  # K8s sticky root-owned mount
    for bad in (st(1001, 0o755), st(1000, 0o775), st(1000, 0o1777), st(0, 0o770)):
        with pytest.raises(KeysFileUnsafeError):
            _check_dir("/", bad)


def test_foreign_owned_directory_rejected(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import os

    from minion_server.auth import KeysFileUnsafeError

    if os.getuid() == 0:
        pytest.skip("root owns everything")
    f = _good(tmp_path)
    monkeypatch.setattr(os, "geteuid", lambda: os.getuid() + 1)
    with pytest.raises(KeysFileUnsafeError):
        KeyStore(f)


def test_dotdot_after_symlink_in_configured_path_is_physical(tmp_path: Path) -> None:
    real = tmp_path / "real"
    (real / "sub").mkdir(parents=True)
    _good(real)  # real/k.yaml; there is no tmp_path/k.yaml
    (tmp_path / "link").symlink_to(real / "sub")
    # the kernel resolves link/.. to real/, not lexically to tmp_path/
    assert KeyStore(f"{tmp_path}/link/../k.yaml").config is not None


def test_symlink_not_owned_by_root_or_server_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import os

    from minion_server.auth import KeysFileUnsafeError

    if os.getuid() == 0:
        pytest.skip("root owns everything")
    _good(tmp_path)
    link = tmp_path / "keys.yaml"
    link.symlink_to("k.yaml")
    real_lstat = os.lstat

    def foreign_link(p: str | os.PathLike[str]) -> os.stat_result:
        st = real_lstat(p)
        if os.fspath(p) == str(link):
            fields = list(st)
            fields[4] = os.getuid() + 1  # st_uid of a planted link
            return os.stat_result(fields)
        return st

    monkeypatch.setattr(os, "lstat", foreign_link)
    with pytest.raises(KeysFileUnsafeError):
        KeyStore(link)


def test_teams_per_key_capped() -> None:
    def body(n: int) -> str:
        teams = ", ".join(f"t{i}" for i in range(n))
        return f"keys:\n  - {{name: a, sha256: {_h(K1)}, teams: [{teams}]}}\n"

    assert len(parse_keys_config(body(64)).keys[_h(K1)].teams) == 64
    with pytest.raises(ValidationError):
        parse_keys_config(body(65))
