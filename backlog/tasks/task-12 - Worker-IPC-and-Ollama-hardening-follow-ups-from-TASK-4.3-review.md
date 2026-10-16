---
id: TASK-12
title: Worker IPC and Ollama hardening follow-ups (from TASK-4.3 review)
status: To Do
assignee: []
created_date: '2026-10-16 03:29'
updated_date: '2026-10-16 13:09'
labels:
  - executor
  - security
dependencies: []
priority: low
ordinal: 22000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Leftover MINOR/LOW findings from TASK-4.3 review and security rounds 4:
- Ollama release_model: real overall wall-clock cap (httpx timeouts are per-phase; a dripping local listener can hang kill/interrupt/_reset >10 s).
- WorkerHandle._send has no watchdog: a worker that stops reading blocks the control thread once a batch exceeds the pipe buffer.
- Watchdog creates one Timer thread per received frame; use one reusable deadline thread per _call and/or rate-limit ticks control-side.
- Timer may fire between _recv returning a final reply and cancel(): reply returned with a dead worker; check proc.is_alive() after cancel.
- stats() and probe() results not type-checked (list result crashes probe_available at startup).
- kill()/interrupt() with a call in flight send the Ollama release twice (up to ~3 s each).
- After a successful worker unload the control side skips its own release even if the runner's keep_alive:0 POST failed.
- run() validates its reply outside the _call critical section; move into _call (expect_list).
- Dev-only (sandbox off): a forked grandchild holding the pipe fd defeats the watchdog kill; shutdown the socket or kill the process group on fire.
- Ollama completions: no PROMPT_TOO_LONG detection (daemon truncates silently).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Each item fixed with a test or explicitly accepted in ADR-0009
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
TASK-4.4 (M3 bench): the worker sandbox denies writes to the com.apple.MetalPerformanceShadersGraph cache dir. Runs succeed, but stderr is noisy and there is no MPSGraph cache reuse, so bench numbers may be slightly pessimistic. Evaluate a narrowly scoped write allow for that cache dir (ADR-0009), or accept it explicitly.
<!-- SECTION:NOTES:END -->
