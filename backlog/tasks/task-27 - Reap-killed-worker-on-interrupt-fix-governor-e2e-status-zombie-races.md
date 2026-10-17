---
id: TASK-27
title: Reap killed worker on interrupt; fix governor e2e status/zombie races
status: In Progress
assignee: []
created_date: '2026-10-17 22:57'
updated_date: '2026-10-17 22:57'
labels:
  - bug
dependencies: []
ordinal: 58000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
WorkerHandle.interrupt()/kill() SIGKILL the worker but don't reap it; when the pause verdict lands between _call invocations nothing joins it, leaving a <defunct> child until the next ensure_model/_reset or idle unload. test_e2e_governor's descendants() counts zombies, so test_long_pause_releases_and_rerun_has_no_duplicate_output fails intermittently on Linux CI (worker killed on release not met within 15s). test_memory_critical_releases_immediately asserts status.json right after release, before _status_loop rewrites it (failed on main run 39037156908 and TASK-7.5 CI). Fix: join(timeout=2) after kill in interrupt()/kill() (called via asyncio.to_thread); poll status in the test with eventually(); descendants() skips zombie (Z) processes.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 interrupt() and kill() reap the worker (unit test: no zombie child left after interrupt with no in-flight call)
- [ ] #2 test_memory_critical_releases_immediately polls status.json instead of asserting once
- [ ] #3 descendants() ignores zombie processes
- [ ] #4 Governor e2e tests green on CI
<!-- AC:END -->
