---
id: TASK-6.6
title: >-
  M5.6 Web UI part 1: session auth, security headers, task list and detail
  (ADR-0010)
status: To Do
assignee: []
created_date: '2026-10-17 03:33'
updated_date: '2026-10-17 07:22'
labels:
  - M5
dependencies:
  - TASK-6.4
parent_task_id: TASK-6
ordinal: 37000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
minion_server/ui/{routes.py, session.py, templates/, static/htmx.min.js vendored, sha256 pinned in a test}; deps jinja2, python-multipart. Stateless cookie minion_session: HMAC-SHA256 with MINION_UI_SESSION_SECRET (>=32 bytes; UI 404 when unset) over key sha256, issued_at, expires_at (MINION_UI_SESSION_TTL_S, 8 h); re-resolved against the KeyStore each request; HttpOnly, SameSite=Strict, Secure unless loopback. CSRF per-session HMAC token on every POST + Origin/Referer check incl. login. Headers: CSP (default-src 'self'; script-src 'self'; frame-ancestors 'none'; form-action 'self'; base-uri 'none'), nosniff, Referrer-Policy no-referrer, Cache-Control no-store. Pages: login/logout, task list, task detail (htmx refresh 5 s). Reuses M5.4 repositories. ADR-0010 (answers SPEC Q4; OIDC via reverse proxy).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Login with a valid key sets a cookie HttpOnly, SameSite=Strict, Secure (non-loopback); wrong/expired/removed keys rejected
- [ ] #2 Tampered/expired cookie redirects to login; a key removed from the file ends the session
- [ ] #3 Another team's task 403; POST without valid CSRF or with foreign Origin 403
- [ ] #4 CSP header present; an error string with <script> renders escaped
- [ ] #5 UI returns 404 when MINION_UI_SESSION_SECRET is unset
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Owner and hostname are free-form executor-supplied strings: the UI must autoescape them (Jinja autoescape on, no |safe). From TASK-6.4 review.
<!-- SECTION:NOTES:END -->
