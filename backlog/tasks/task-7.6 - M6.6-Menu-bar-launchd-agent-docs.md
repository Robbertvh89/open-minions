---
id: TASK-7.6
title: M6.6 Menu-bar launchd agent + docs
status: In Progress
assignee: []
created_date: '2026-10-17 20:36'
updated_date: '2026-10-17 23:49'
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
- [ ] #1 Rendered plist golden test: Aqua session, no secrets, paths outside TCC roots
- [ ] #2 install/uninstall idempotent; refuses MINION_EXECUTOR_DEV=1 like service install
- [ ] #3 Docs updated (FR-56, ADR-0011, executor-install.md, CLAUDE.md, README)
<!-- AC:END -->
