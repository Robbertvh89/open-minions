---
id: TASK-7.6
title: M6.6 Menu-bar launchd agent + docs
status: Done
assignee: []
created_date: '2026-10-17 20:36'
updated_date: '2026-10-18 00:46'
labels:
  - executor
dependencies: []
parent_task_id: TASK-7
ordinal: 57000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
minion-executor menubar install|uninstall|status: per-user LaunchAgent com.open-minions.menubar (LimitLoadToSessionType=Aqua, RunAtLoad, KeepAlive SuccessfulExit=false, ProcessType=Interactive), reusing service.py helpers; no secrets in the plist; --dry-run. Independent of the executor agent. Docs: SPEC FR-56, ADR-0011 addendum, docs/ops/executor-install.md, CLAUDE.md, README.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 Rendered plist golden test: Aqua session, no secrets, paths outside TCC roots
- [x] #2 install/uninstall idempotent; refuses MINION_EXECUTOR_DEV=1 like service install
- [x] #3 Docs updated (FR-56, ADR-0011, executor-install.md, CLAUDE.md, README)
<!-- AC:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Merged #45: minion-executor menubar install|uninstall|status LaunchAgent (Aqua, KeepAlive on crash, no secrets), single-instance lock, shared launchd.py with hardened service+menubar install/uninstall (root refused, exact label/program match, install-chain ownership with admin-group exception). CI run 39083266520: 1903 passed.
<!-- SECTION:FINAL_SUMMARY:END -->
