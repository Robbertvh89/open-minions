---
id: TASK-7.5
title: >-
  M6.5 Menu-bar app: profile/pause/resume/status over the local control/status
  files
status: To Do
assignee: []
created_date: '2026-10-17 20:36'
labels:
  - executor
dependencies: []
parent_task_id: TASK-7
ordinal: 56000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
minion-executor menubar (darwin only). AppKit via pyobjc (NSStatusItem, accessory policy, NSTimer), already installed via pyobjc-framework-Quartz -> cocoa: zero new deps (rumps rejected). Pure menubar/model.py (status -> view model, action -> update_control kwargs) + menubar/app.py (AppKit glue). Reads status.json every 5 s; writes only via update_control (pause 30m/1h/2h/until resume, resume, profile auto/light/balanced/full/custom). No network, no token/Keychain, no server URL, no subprocess, no power assertions; refuses root; safefile checks. flock control.lock inside update_control so CLI and menu-bar writes cannot lose updates.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 model.py unit-tested on Linux (all status states, unknown enums, stale/stopped, control_invalid)
- [ ] #2 Test: minion_executor.menubar imports no httpx/minion_common.client/secrets/keyring and reads no token env
- [ ] #3 Concurrent update_control calls never lose a field (unit test)
- [ ] #4 Idle CPU of the menubar process < 0.5 % measured on this Mac (in PR)
- [ ] #5 macos-marked smoke test builds the status item/menu without running the loop
<!-- AC:END -->
