# ADR-0010: Web UI authentication with API-key login and a stateless session cookie

- Status: **Accepted** (TASK-6.6)
- Deviates from paper: no (the paper only mentions a web UI). Answers SPEC Q4.

## Context

The server gets a read-mostly web UI (task list and detail now; submit/cancel and the fleet view
later). Users already authenticate with API keys from a file (ADR-0007), and SPEC §7 caps the
data model at 7 tables, so a user or session table is out. SSO/OIDC would be a large dependency
surface (flows, discovery, JWKS caching) for a tool that is deployed per team or org.

## Decision

1. **Login with the existing API key.** The login form posts the key; it is checked exactly like
   an API request (SHA-256, `hmac.compare_digest` over all stored hashes, `expires_at`). Wrong,
   expired and removed keys get the same 401 page. The key is never logged, echoed or stored.
2. **Stateless session cookie** `minion_session`:
   `v2.<kid>.<issued_at>.<expires_at>.<HMAC-SHA256>`, where `kid` = HMAC(secret, "kid", key sha256) so the cookie never carries the key hash, keyed by `MINION_UI_SESSION_SECRET`
   (at least 32 bytes). It holds no secret (the key hash cannot be inverted into the key). Flags:
   `HttpOnly`, `SameSite=Strict`, `Path=/ui`, `Secure` unless the request addressed a loopback host.
   Lifetime `MINION_UI_SESSION_TTL_S` (default 8 h, max 7 days). Validity is age-based: valid while `now < min(expires, issued + TTL)`, so a lowered TTL shortens old cookies and a raised one never revives them.
3. **Re-resolved on every request** against the live KeyStore snapshot: removing or expiring the
   key in the file, or a file that turns invalid (fail closed), ends the session within
   `MINION_API_KEYS_RELOAD_S`. Team scoping is the same code path as the REST API (shared helpers
   `list_tasks_page`, `get_task_for`, `list_subtasks_page`), so another team's task is `403`.
   Rotating the secret logs everyone out. There is no server-side revocation list beyond that
   (logout clears the browser cookie only); the key file is the revocation mechanism.
4. **CSRF**: every `POST` needs (a) an `Origin` header (or `Referer` when absent) whose host equals
   the `Host` header, and (b) a CSRF token. The token is an HMAC of the signed session payload
   (per session). The login form, which has no session yet, uses an HMAC of a random nonce held in
   a short-lived `HttpOnly` `minion_login` cookie. Both checks fail with `403`. The reverse proxy
   must forward `Host` including the port (nginx `$http_host`, not `$host`). A non-loopback UI
   requires TLS (cookies are `Secure`). Optional `MINION_UI_ALLOWED_HOSTS` restricts `Host` (else `400`); bare `host[:port]` entries, validated at startup. Production should set it (DNS rebinding) and generate the secret with `minion-server-admin generate-ui-secret` (hand-picked phrases are weak).
5. **Headers** on every `/ui` response: `Content-Security-Policy: default-src 'self'; script-src
   'self'; frame-ancestors 'none'; form-action 'self'; base-uri 'none'`, `X-Content-Type-Options:
   nosniff`, `Referrer-Policy: same-origin`, `Cache-Control: no-store`, plus `Strict-Transport-Security:
   max-age=31536000` when the server itself sees https (behind a TLS proxy the proxy adds it).
   `no-referrer` was rejected: browsers then send `Origin: null` on same-origin form POSTs and every
   login would fail the CSRF check, which deliberately rejects `Origin: null`. htmx runs with
   `historyCacheSize: 0`.
6. **Rendering**: Jinja2 with autoescape on for every template and no `|safe`; owner, hostname,
   error text and other executor-supplied strings are untrusted. No inline script or handler (CSP).
   htmx 2.0.4 is vendored (`ui/static/htmx.min.js`, sha256 pinned in a test), configured by a
   `<meta name="htmx-config">` tag with `allowEval`, `allowScriptTags` and inline indicator styles
   off, so CSP needs no `unsafe-*`. Polling is a plain GET fragment (`?partial=1`, every 5 s).
7. **Off by default**: with `MINION_UI_SESSION_SECRET` unset every `/ui` route is a plain `404`.
8. **OIDC/SSO is deferred to a reverse proxy**: deployments that need it put oauth2-proxy (or
   similar) in front of the UI paths; the API-key login then remains a second factor or can be
   limited to the proxy's allow-list. No OIDC code lives in the server.

## Consequences

- No new table, no session store; any replica can serve any session (same secret on all replicas).
- A stolen cookie works until expiry or key removal; `HttpOnly` + `Strict` + `Secure` + 8 h bound
  the exposure. Shorten the TTL for stricter deployments.
- No login rate limiting in the server (as with S-12): limit `POST /ui/login` per IP at the proxy.
  Keys are 256-bit random, so online guessing is not feasible, but the hash scan costs CPU.
- Login CSRF is mitigated by the Origin check and the nonce cookie, not by a server-side store.
- The pages are English-only and need no JavaScript except htmx's auto-refresh.
