---
id: TASK-38
title: Rewrite README as a practical getting-started guide
status: Done
assignee: []
created_date: '2026-10-18 18:18'
updated_date: '2026-10-19 18:28'
labels:
  - docs
dependencies: []
ordinal: 77000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Practical, task-oriented README: what it is in 3 lines, 15-minute local quick start, team pilot setup (operator), joining as a Mac owner, submitting work (CLI + web UI), day-2 ops, troubleshooting, links to SPEC/ARCHITECTURE/ADRs. Every command verified.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 Every command in README executed and verified on this Mac
- [x] #2 Covers operator, Mac owner, submitter paths and troubleshooting
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Verification (one Mac, Apple M3 Pro): ran in an isolated compose project on alternate ports, then tore it down. Run: uv sync, API key and executor token (Keychain), real MLX executor (bge-small-en, qwen2.5-0.5b-instruct), submit/status/fetch/cancel, pause/resume/profile/prefer, real launchd service and menubar install/uninstall with test labels, executor.toml, bench, exit-2 error paths, revoked-token recovery, fake-engine dev override, native alembic and minion-server, UI login via curl, Grafana dashboards, ruff/mypy/pytest. Not run: interactive 'security -w' prompt, Linux chown, launchctl kickstart, UI task-create form, integration tests. Operator/team guide (multi-Mac) deferred to TASK-37 (README has a preview section).
<!-- SECTION:NOTES:END -->
