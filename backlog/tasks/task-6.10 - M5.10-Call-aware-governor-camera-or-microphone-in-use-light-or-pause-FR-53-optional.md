---
id: TASK-6.10
title: >-
  M5.10 Call-aware governor: camera or microphone in use -> light or pause
  (FR-53 optional)
status: To Do
assignee: []
created_date: '2026-10-17 03:33'
labels:
  - M5
dependencies: []
parent_task_id: TASK-6
ordinal: 41000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
MacSignals reads kAudioDevicePropertyDeviceIsRunningSomewhere (default input) and kCMIODevicePropertyDeviceIsRunningSomewhere (cameras) via ctypes (public APIs, no TCC prompt). SignalSnapshot.call_active: bool | None; FileSignals/NullSignals updated. config on_call = light | pause | ignore (default light). Governor: during a call drop at once to min(current, light) or pause with PausedReason.IN_CALL; back up after the usual 60 s hold. Status shows it. SPEC FR-53/S-11 (profile drop reveals 'probably in a call'; owners can set ignore), ADR-0006 amendment.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Pure governor tests: call on drops to light within one poll; call off keeps light >= 60 s; pause mode pauses with in_call; ignore changes nothing
- [ ] #2 NF-9 reaction test includes a call event, reacts < 5 s
- [ ] #3 MacSignals returns a bool on this Mac (macos marker); real mic/camera activation NOT FULLY VERIFIABLE HERE
- [ ] #4 SPEC FR-53/S-11 and ADR-0006 updated
<!-- AC:END -->
