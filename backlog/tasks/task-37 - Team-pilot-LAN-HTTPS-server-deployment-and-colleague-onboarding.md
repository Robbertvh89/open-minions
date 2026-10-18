---
id: TASK-37
title: 'Team pilot: LAN/HTTPS server deployment and colleague onboarding'
status: To Do
assignee: []
created_date: '2026-10-18 18:18'
labels:
  - pilot
dependencies: []
ordinal: 76000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
End state: something usable and testable with colleagues in a working setting. One operator machine runs the compose stack reachable by colleagues' Macs over HTTPS (executors refuse insecure non-loopback URLs); TLS via reverse proxy (e.g. Caddy with internal CA or provided cert); per-person API keys and executor tokens; Mac-owner install path (uv/pipx or packaged), Keychain token, LaunchAgent service + menubar; submitter path via minion submit and web UI; a small sample job (embeddings + completion on small public models); smoke script operator can run end-to-end; troubleshooting.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Operator can bring up server reachable over HTTPS from another Mac following docs only
- [ ] #2 Colleague Mac can install, enrol (token in Keychain), run as service, and complete a sample job
- [ ] #3 Submitter can run a sample job via CLI and web UI and fetch results
- [ ] #4 Pilot smoke script/checklist verified on this Mac (loopback + TLS path)
<!-- AC:END -->
