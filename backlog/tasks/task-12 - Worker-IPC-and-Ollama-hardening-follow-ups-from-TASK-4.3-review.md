---
id: TASK-12
title: Worker IPC and Ollama hardening follow-ups (from TASK-4.3 review)
status: To Do
assignee: []
created_date: '2026-10-16 03:29'
updated_date: '2026-10-17 01:56'
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

Narrowed (TASK-5.3): ctl sends are now bounded by a kill watchdog (CTL_SEND_TIMEOUT_S). Still open: _call's initial request send (up to 64 MiB, under _lock/_send_lock) has no time limit; a non-reading worker blocks the call thread until kill/interrupt. Fix with the same kill watchdog.

TASK-5.3 final audit: (a) _call takes _send_lock and sends the request with no timeout; with the sandbox off (DEV) a worker can fork a child that holds the pipe open and stops reading, wedging _send_lock then _lock. Fix: acquire(timeout=CTL_SEND_TIMEOUT_S+1) -> WorkerCrashed, and the same kill watchdog on the _call write. (b) Ack 'paused' is not checked against the delivered generation's state and acks may go backwards; keep delivered gen->paused, reject mismatch and ack < previous. Acks are advisory (ADR-0006); enforcement is control-side.

From TASK-5.5 review: WorkerHandle.ensure_model treats a warm worker as reusable when model_id matches (worker/process.py:675), ignoring revision; the loop now compares (model_id, revision). If the catalogue bumps a model's revision under the same id, a warm worker keeps the old weights. Compare revision (or the cache path) in ensure_model too.

From TASK-5.6 round 3: Ollama is paced per request only (the daemon owns the GPU queue). Warn at startup / in status when the Ollama engine is enabled and duty < 1 (or force concurrency 1).
<!-- SECTION:NOTES:END -->
