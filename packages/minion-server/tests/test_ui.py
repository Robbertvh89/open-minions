"""Web UI part 1: session auth, CSRF, security headers, task pages (TASK-6.6, ADR-0010)."""

import asyncio
import hashlib
import re
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from httpx import ASGITransport, AsyncClient
from minion_server.app import create_app
from minion_server.netutil import host_header_is_loopback
from minion_server.settings import ServerSettings
from minion_server.ui import session as sess
from minion_server.ui.headers import CSP
from pydantic import SecretStr, ValidationError
from sqlalchemy import text

from conftest import _settings, seed_task

ALICE, BOB, EXPIRED, GONE = "k-alice", "k-bob", "k-expired", "k-gone"
SECRET = "s3cret-" * 6
BASE = "https://ui.example"
ORIGIN = {"Origin": BASE}
HTMX_VERSION = "2.0.4"
HTMX_SHA256 = "e209dda5c8235479f3166defc7750e1dbcd5a5c1808b7792fc2e6733768fb447"


def _h(k: str) -> str:
    return hashlib.sha256(k.encode()).hexdigest()


def _keys_text(*, with_gone: bool = True) -> str:
    past = (datetime.now(UTC) - timedelta(days=1)).isoformat()
    lines = [
        "keys:",
        f"  - {{name: alice, sha256: {_h(ALICE)}, teams: [ra, rb]}}",
        f"  - {{name: bob, sha256: {_h(BOB)}, teams: [rc]}}",
        f"  - {{name: old, sha256: {_h(EXPIRED)}, teams: [ra], expires_at: '{past}'}}",
    ]
    if with_gone:
        lines.append(f"  - {{name: gone, sha256: {_h(GONE)}, teams: [ra]}}")
    return "\n".join(lines) + "\n"


# --- unit ---------------------------------------------------------------------------------------


def test_htmx_is_vendored_and_pinned() -> None:
    import minion_server.ui as ui

    data = (Path(ui.__file__).parent / "static" / "htmx.min.js").read_bytes()
    assert hashlib.sha256(data).hexdigest() == HTMX_SHA256  # htmx.org@2.0.4 dist/htmx.min.js
    assert f'version:"{HTMX_VERSION}"' in data.decode()


def test_session_roundtrip_and_rejections() -> None:
    secret = b"k" * 32
    now = datetime(2026, 1, 1, 12, tzinfo=UTC)
    value = sess.issue_session(secret, _h(ALICE), now, 3600)
    claims = sess.verify_session(secret, value, now + timedelta(seconds=10), 3600)
    assert claims is not None and claims.kid == sess.key_id(secret, _h(ALICE))
    assert _h(ALICE) not in value and claims.kid != _h(ALICE)  # cookie never carries the key hash
    # expired, wrong secret, tampered field, truncated, junk, TTL lowered after issue
    assert sess.verify_session(secret, value, now + timedelta(hours=1), 3600) is None
    assert sess.verify_session(b"j" * 32, value, now, 3600) is None
    parts = value.split(".")
    parts[1] = _h(BOB)
    assert sess.verify_session(secret, ".".join(parts), now, 3600) is None
    parts = value.split(".")
    parts[3] = str(int(parts[3]) + 99999)
    assert sess.verify_session(secret, ".".join(parts), now, 3600) is None
    for bad in ["", "v1", value[:-4], value + "x", "é" * 10, "a.b.c.d.e", "x" * 1000]:
        assert sess.verify_session(secret, bad, now, 3600) is None
    assert sess.verify_session(secret, value, now + timedelta(minutes=2), 60) is None
    # csrf token is per session and not valid for another one
    other = sess.issue_session(secret, _h(BOB), now, 3600)
    c1 = sess.verify_session(secret, value, now, 3600)
    c2 = sess.verify_session(secret, other, now, 3600)
    assert c1 and c2
    assert sess.csrf_token(secret, c1) != sess.csrf_token(secret, c2)


def test_session_ttl_is_age_based_without_revival() -> None:
    secret = b"k" * 32
    t0 = datetime(2026, 1, 1, 12, tzinfo=UTC)
    value = sess.issue_session(secret, _h(ALICE), t0, 8 * 3600)  # issued under an 8 h TTL

    def ok(dt: timedelta, ttl: int) -> bool:
        return sess.verify_session(secret, value, t0 + dt, ttl) is not None

    # TTL later lowered to 1 h
    assert ok(timedelta(hours=0.5), 3600)
    assert not ok(timedelta(hours=2), 3600)
    assert not ok(timedelta(hours=7.5), 3600)  # no revival
    # a later raise never extends beyond the signed expiry
    assert not ok(timedelta(hours=9), 24 * 3600)
    # issued in the future beyond skew is rejected
    assert sess.verify_session(secret, value, t0 - timedelta(minutes=5), 8 * 3600) is None
    assert sess.verify_session(secret, value, t0 - timedelta(seconds=30), 8 * 3600) is not None


def test_referrer_policy_header_and_meta_are_same_origin() -> None:
    import minion_server.ui as ui
    from minion_server.ui.headers import SECURITY_HEADERS

    assert SECURITY_HEADERS["Referrer-Policy"] == "same-origin"
    html = (Path(ui.__file__).parent / "templates" / "base.html").read_text()
    assert '<meta name="referrer" content="same-origin">' in html
    assert '"historyCacheSize": 0' in html


def test_origin_check() -> None:
    ok = sess.origin_ok
    assert ok("https://h.example", None, "h.example")
    assert ok("http://localhost:8080", None, "localhost:8080")
    assert ok(None, "https://h.example/ui/login", "h.example")
    assert not ok(None, None, "h.example")
    assert not ok("null", None, "h.example")
    assert not ok("null", "https://h.example/ui/login", "h.example")
    assert not ok(None, None, "h.example")
    assert not ok("https://evil.example", None, "h.example")
    assert not ok("https://h.example.evil.example", None, "h.example")
    assert not ok("https://evil@h.example", None, "h.example")
    assert not ok("https://h.example", None, None)
    assert not ok("javascript://h.example", None, "h.example")
    # Origin wins over a matching Referer
    assert not ok("https://evil.example", "https://h.example/", "h.example")


def test_loopback_detection() -> None:
    assert host_header_is_loopback("localhost:8080")
    assert host_header_is_loopback("127.0.0.1")
    assert host_header_is_loopback("[::1]:8080")
    assert not host_header_is_loopback("ui.example")
    assert not host_header_is_loopback("10.0.0.5:8080")
    assert not host_header_is_loopback(None)


def test_settings_ttl_cap_and_allowed_hosts_and_secret_charset() -> None:
    base: dict[str, Any] = {"database_url": "x", "s3_access_key": "a", "s3_secret_key": "b"}
    with pytest.raises(ValidationError):
        ServerSettings.model_validate({**base, "ui_session_ttl_s": 7 * 24 * 3600 + 1})
    with pytest.raises(ValidationError):
        ServerSettings.model_validate({**base, "ui_session_secret": "a" * 40})
    cfg = ServerSettings.model_validate({**base, "ui_allowed_hosts": " A.example:8443, b.example "})
    assert cfg.ui_allowed_host_set == {"a.example:8443", "b.example"}


def test_generate_ui_secret_command() -> None:
    from minion_server.admin import app as admin_app
    from typer.testing import CliRunner

    out = CliRunner().invoke(admin_app, ["generate-ui-secret"]).output.strip()
    assert out.startswith("MINION_UI_SESSION_SECRET=")
    assert len(out.split("=", 1)[1]) >= 48


def test_settings_secret_validation() -> None:
    def make(v: str | None) -> ServerSettings:
        return ServerSettings.model_validate(
            {
                "database_url": "x",
                "s3_access_key": "a",
                "s3_secret_key": "b",
                "ui_session_secret": v,
            }
        )

    assert make(None).ui_session_secret is None
    assert make("  ").ui_session_secret is None
    s = make("ab" * 16).ui_session_secret
    assert s is not None and s.get_secret_value() == "ab" * 16
    with pytest.raises(ValidationError):
        make("short")


async def test_ui_is_404_when_secret_unset() -> None:  # AC5
    app = create_app()  # no lifespan: ui_secret never set
    async with AsyncClient(transport=ASGITransport(app=app), base_url=BASE) as c:
        for path in ["/ui", "/ui/login", "/ui/tasks", "/ui/static/htmx.min.js", "/ui/tasks/a/b/c"]:
            r = await c.get(path)
            assert r.status_code == 404, path
        assert (await c.post("/ui/login", data={"api_key": "x"}, headers=ORIGIN)).status_code == 404
        assert (await c.get("/healthz")).status_code == 200


# --- integration --------------------------------------------------------------------------------


@pytest.fixture
async def ui(fresh_url: str, tmp_path: Path) -> AsyncIterator[tuple[AsyncClient, Any, Path]]:
    keys = tmp_path / "keys.yaml"
    keys.write_text(_keys_text())
    cfg = _settings(fresh_url).model_copy(
        update={
            "api_keys_file": str(keys),
            "api_keys_reload_s": 0,
            "ui_session_secret": SecretStr(SECRET),
        }
    )
    app = create_app(cfg)
    async with app.router.lifespan_context(app):
        app.state.splitter_task.cancel()
        await asyncio.gather(app.state.splitter_task, return_exceptions=True)
        async with AsyncClient(transport=ASGITransport(app=app), base_url=BASE) as c:
            yield c, app, keys


async def _csrf(c: AsyncClient, path: str = "/ui/login") -> str:
    m = re.search(r'name="csrf" value="([^"]+)"', (await c.get(path)).text)
    assert m
    return m.group(1)


async def _login(c: AsyncClient, key: str = ALICE) -> Any:
    token = await _csrf(c)
    return await c.post("/ui/login", data={"api_key": key, "csrf": token}, headers=ORIGIN)


@pytest.mark.integration
async def test_login_sets_hardened_cookie_and_rejects_bad_keys(
    ui: tuple[AsyncClient, Any, Path],
) -> None:  # AC1
    c, _, _ = ui
    r = await _login(c)
    assert r.status_code == 303 and r.headers["location"] == "/ui/tasks"
    cookie = next(v for v in r.headers.get_list("set-cookie") if v.startswith("minion_session="))
    low = cookie.lower()
    assert "httponly" in low and "samesite=strict" in low and "secure" in low
    assert "path=/ui" in low and "max-age=28800" in low
    assert ALICE not in cookie
    assert (await c.get("/ui/tasks")).status_code == 200

    # loopback host: no Secure flag
    async with AsyncClient(
        transport=ASGITransport(app=ui[1]), base_url="http://localhost:8080"
    ) as lb:
        token = await _csrf(lb)
        r = await lb.post(
            "/ui/login",
            data={"api_key": ALICE, "csrf": token},
            headers={"Origin": "http://localhost:8080"},
        )
        assert r.status_code == 303
        session_cookie = next(
            v for v in r.headers.get_list("set-cookie") if v.startswith("minion_session=")
        )
        assert "secure" not in session_cookie.lower()

    # wrong, expired, unknown keys: one generic 401, no cookie, key never reflected
    for bad in ["nope", EXPIRED, "", "x" * 300]:
        async with AsyncClient(transport=ASGITransport(app=ui[1]), base_url=BASE) as fresh:
            token = await _csrf(fresh)
            r = await fresh.post("/ui/login", data={"api_key": bad, "csrf": token}, headers=ORIGIN)
            assert r.status_code == 401, bad
            if bad:
                assert bad not in r.text
            assert not any(
                v.startswith("minion_session=") for v in r.headers.get_list("set-cookie")
            )
            assert (await fresh.get("/ui/tasks")).status_code == 303


@pytest.mark.integration
async def test_tampered_expired_cookie_redirects_and_removed_key_ends_session(
    ui: tuple[AsyncClient, Any, Path],
) -> None:  # AC2
    c, _app, keys = ui
    assert (await _login(c, GONE)).status_code == 303
    assert (await c.get("/ui/tasks")).status_code == 200
    value = c.cookies["minion_session"]

    # tampered
    c.cookies.set("minion_session", value[:-3] + "000", path="/ui")
    r = await c.get("/ui/tasks")
    assert r.status_code == 303 and r.headers["location"] == "/ui/login"
    # expired (signed correctly but in the past)
    old = sess.issue_session(
        SECRET.encode(), _h(GONE), datetime.now(UTC) - timedelta(hours=9), 8 * 3600
    )
    c.cookies.set("minion_session", old, path="/ui")
    assert (await c.get("/ui/tasks")).status_code == 303
    # htmx fragment request gets HX-Redirect instead of a followed redirect
    r = await c.get("/ui/tasks", headers={"HX-Request": "true"})
    assert r.status_code == 401 and r.headers["hx-redirect"] == "/ui/login"

    # a genuine cookie stops working once the key leaves the file
    c.cookies.set("minion_session", value, path="/ui")
    assert (await c.get("/ui/tasks")).status_code == 200
    tmp = keys.with_suffix(".new")
    tmp.write_text(_keys_text(with_gone=False))
    tmp.replace(keys)
    r = await c.get("/ui/tasks")
    assert r.status_code == 303 and r.headers["location"] == "/ui/login"
    # and the removed key cannot log in again
    assert (await _login(c, GONE)).status_code == 401


@pytest.mark.integration
async def test_team_scoping_and_csrf(ui: tuple[AsyncClient, Any, Path]) -> None:  # AC3
    c, app, _ = ui
    await seed_task(app.state.sessionmaker, "t1", 2, team="ra")
    await seed_task(app.state.sessionmaker, "t2", 1, team="rc")
    assert (await _login(c, BOB)).status_code == 303
    r = await c.get("/ui/tasks/ra/p/t1")
    assert r.status_code == 403 and "t1" not in r.text.split("<h1>")[1]
    assert (await c.get("/ui/tasks?team=ra")).status_code == 403
    r = await c.get("/ui/tasks")
    assert "t2" in r.text and "t1" not in r.text
    assert (await c.get("/ui/tasks/rc/p/t2")).status_code == 200
    assert (await c.get("/ui/tasks/rc/p/missing")).status_code == 404

    csrf = await _csrf(c, "/ui/tasks")
    assert csrf == await _csrf(c, "/ui/tasks")  # stable per session
    form = {"csrf": csrf}
    # POST without CSRF, with a wrong one, with a foreign / null / missing Origin
    assert (await c.post("/ui/logout", data={}, headers=ORIGIN)).status_code == 403
    assert (await c.post("/ui/logout", data={"csrf": "0" * 64}, headers=ORIGIN)).status_code == 403
    for hdrs in [{"Origin": "https://evil.example"}, {"Origin": "null"}, {}]:
        assert (await c.post("/ui/logout", data=form, headers=hdrs)).status_code == 403
    assert (await c.get("/ui/tasks")).status_code == 200  # still logged in
    # Referer is an acceptable stand-in for a missing Origin
    r = await c.post("/ui/logout", data=form, headers={"Referer": BASE + "/ui/tasks"})
    assert r.status_code == 303 and r.headers["location"] == "/ui/login"
    cleared = [v for v in r.headers.get_list("set-cookie") if v.startswith("minion_session=")]
    assert cleared and "Max-Age=0" in cleared[0]
    assert (await c.get("/ui/tasks")).status_code == 303

    # login itself: foreign origin, missing/forged csrf, csrf of another browser's nonce
    token = await _csrf(c)
    body = {"api_key": ALICE, "csrf": token}
    assert (
        await c.post("/ui/login", data=body, headers={"Origin": "https://evil.example"})
    ).status_code == 403
    assert (await c.post("/ui/login", data=body)).status_code == 403
    assert (await c.post("/ui/login", data={"api_key": ALICE}, headers=ORIGIN)).status_code == 403
    async with AsyncClient(transport=ASGITransport(app=app), base_url=BASE) as other:
        await other.get("/ui/login")  # its own nonce cookie
        r = await other.post("/ui/login", data=body, headers=ORIGIN)
        assert r.status_code == 403


@pytest.mark.integration
async def test_security_headers_and_escaping(ui: tuple[AsyncClient, Any, Path]) -> None:  # AC4
    c, app, _ = ui
    sm = app.state.sessionmaker
    await seed_task(sm, "xss", 2, team="ra")
    payload = "<script>alert(1)</script>"
    async with sm() as s, s.begin():
        await s.execute(text("UPDATE minion_task SET error = :e"), {"e": payload})
    assert (await _login(c)).status_code == 303
    pages = [
        "/ui/login",
        "/ui/tasks",
        "/ui/tasks/ra/p/xss",
        "/ui/tasks/ra/p/xss?partial=1",
        "/ui/tasks/rc/p/xss",
        "/ui/static/htmx.min.js",
        "/ui/static/ui.css",
        "/ui/static/..%2f..%2fauth.py",
        "/ui",
    ]
    for path in pages:
        r = await c.get(path)
        assert r.headers["content-security-policy"] == CSP, path
        assert r.headers["x-content-type-options"] == "nosniff", path
        assert r.headers["referrer-policy"] == "same-origin", path
        assert r.headers["strict-transport-security"] == "max-age=31536000", path  # https client
        assert r.headers["cache-control"] == "no-store", path
    assert "default-src 'self'" in CSP and "frame-ancestors 'none'" in CSP
    # the API is not affected
    assert "content-security-policy" not in (await c.get("/healthz")).headers

    for path in ["/ui/tasks/ra/p/xss", "/ui/tasks/ra/p/xss?partial=1"]:
        body = (await c.get(path)).text
        assert payload not in body
        assert "&lt;script&gt;alert(1)&lt;/script&gt;" in body
    # no inline script anywhere (CSP forbids it): only the vendored file is referenced
    for path in ["/ui/login", "/ui/tasks", "/ui/tasks/ra/p/xss"]:
        scripts = re.findall(r"<script\b[^>]*>", (await c.get(path)).text)
        assert scripts == ['<script src="/ui/static/htmx.min.js">'], path
        assert not re.search(r"\son\w+\s*=|javascript:", (await c.get(path)).text)


@pytest.mark.integration
async def test_hsts_only_on_https_and_allowed_hosts(ui: tuple[AsyncClient, Any, Path]) -> None:
    _c, app, _ = ui
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://localhost") as lb:
        r = await lb.get("/ui/login")
        assert r.status_code == 200 and "strict-transport-security" not in r.headers
    app.state.settings = app.state.settings.model_copy(update={"ui_allowed_hosts": "ui.example"})
    async with AsyncClient(transport=ASGITransport(app=app), base_url=BASE) as ok:
        assert (await ok.get("/ui/login")).status_code == 200
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="https://evil.example"
    ) as bad:
        assert (await bad.get("/ui/login")).status_code == 400
        assert (await bad.get("/ui/tasks")).status_code == 400


@pytest.mark.integration
async def test_task_list_filters_pagination_and_detail(
    ui: tuple[AsyncClient, Any, Path],
) -> None:
    c, app, _ = ui
    sm = app.state.sessionmaker
    for i in range(5):
        await seed_task(sm, f"n{i}", 3, team="ra", project="alpha")
    await seed_task(sm, "other", 1, team="rb", project="beta")
    assert (await _login(c)).status_code == 303
    r = await c.get("/ui/tasks?limit=2")
    assert r.status_code == 200
    m = re.search(r'href="(/ui/tasks\?[^"]*cursor=[^"]+)"', r.text)
    assert m
    seen = set(re.findall(r'/ui/tasks/ra/alpha/(\w+)"', r.text))
    nxt = m.group(1).replace("&amp;", "&")
    while nxt:
        r = await c.get(nxt)
        seen |= set(re.findall(r'/ui/tasks/ra/alpha/(\w+)"', r.text))
        m = re.search(r'href="(/ui/tasks\?[^"]*cursor=[^"]+)"', r.text)
        nxt = m.group(1).replace("&amp;", "&") if m else ""
    assert seen == {f"n{i}" for i in range(5)}
    r = await c.get("/ui/tasks?team=rb&status=PENDING")
    assert "other" in r.text and "n0" not in r.text
    assert "other" not in (await c.get("/ui/tasks?project=alpha")).text
    for bad in ["status=bogus", "limit=0", "limit=101", "limit=x", "team=A!", "cursor=zzz"]:
        assert (await c.get(f"/ui/tasks?{bad}")).status_code == 422, bad

    r = await c.get("/ui/tasks/ra/alpha/n0")
    assert r.status_code == 200
    assert "0 / 3 subtasks succeeded" in r.text
    assert 'hx-trigger="every 5s"' in r.text
    assert 'hx-get="/ui/tasks/ra/alpha/n0?partial=1"' in r.text
    part = await c.get("/ui/tasks/ra/alpha/n0?partial=1")
    assert part.status_code == 200 and "<html" not in part.text and "Subtasks" in part.text
    assert (await c.get("/ui/tasks/ra/alpha/n0?status=bogus")).status_code == 422
    # terminal tasks stop polling
    async with sm() as s, s.begin():
        await s.execute(text("UPDATE minion_task SET status = 'SUCCEEDED' WHERE task_name='n1'"))
    assert "hx-trigger" not in (await c.get("/ui/tasks/ra/alpha/n1")).text
    assert (await c.get("/ui/static/nope.js")).status_code == 404
    assert (await c.get("/ui")).headers["location"] == "/ui/tasks"
