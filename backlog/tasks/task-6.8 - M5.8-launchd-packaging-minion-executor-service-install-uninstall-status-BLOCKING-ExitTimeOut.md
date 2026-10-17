---
id: TASK-6.8
title: >-
  M5.8 launchd packaging: minion-executor service install/uninstall/status
  (BLOCKING ExitTimeOut)
status: In Progress
assignee: []
created_date: '2026-10-17 03:33'
updated_date: '2026-10-17 03:33'
labels:
  - M5
dependencies: []
parent_task_id: TASK-6
ordinal: 39000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
minion_executor/service.py: pure render_plist() (plistlib, deterministic), exit_timeout_for(grace)=ceil(grace)+15; typer service sub-app. deploy/launchd/com.open-minions.executor.plist equals render() (test). Keys: ProgramArguments (absolute minion-executor path, run, --server, optional --profile/--engines); EnvironmentVariables = STOP_GRACE_S, LAUNCHD_EXIT_TIMEOUT_S + allow-list of non-secret MINION_EXECUTOR_* (never TOKEN, DEV, TEST_HOOKS, SANDBOX, FETCH_SANDBOX, FAKE_SIGNALS, HF_TOKEN, proxies); ExitTimeOut; RunAtLoad; KeepAlive {SuccessfulExit:false}; ThrottleInterval 60; LimitLoadToSessionType Aqua; ProcessType Standard (D1); logs ~/Library/Logs/open-minions/executor.log; nothing preventing sleep. install: preflight like run; refuses DEV=1 and env-only token; plist 0644; launchctl bootstrap gui/UID; --dry-run, --no-load. uninstall: bootout + remove. status: launchctl print (state, pid, last exit, exit timeout), warn if < grace+15, then executor status. run exits 2 if LAUNCHD_EXIT_TIMEOUT_S < stop_grace_s + 15. Docs: docs/ops/executor-install.md, ARCHITECTURE 2.2/6 (drop 'signed plist'), SPEC FR-17, ADR-0011, ADR-0009 note.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 render_plist with grace 0/30/300 gives ExitTimeOut 15/45/315
- [ ] #2 No secret in the plist even with MINION_EXECUTOR_TOKEN and HF_TOKEN set
- [ ] #3 plutil -lint passes (macos marker)
- [ ] #4 install refuses DEV=1 and env-only token; run exits 2 on inconsistent launchd exit timeout
- [ ] #5 Live on this Mac: install a test label, uninstall during a subtask; exits within grace + 10 s, subtask RELEASED, no SIGKILL; nothing left installed afterwards
- [ ] #6 Worker sandbox self-test passes when installed via uv tool install (or documented venv fallback)
<!-- AC:END -->
