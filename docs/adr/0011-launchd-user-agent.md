# ADR-0011: launchd packaging as a per-user LaunchAgent

- Status: **Accepted** (TASK-6.8)
- Deviates from paper: no (the paper does not cover packaging)

## Context

Owners want the executor to start at login and restart after a crash, without admin rights, and
without weakening invariant 6 (yield to the user, never block sleep) or the Keychain/HID access the
governor needs.

## Decision

- **Per-user LaunchAgent**, `~/Library/LaunchAgents/<label>.plist`, loaded into `gui/$UID` with
  `launchctl bootstrap`. Not a LaunchDaemon: idle detection (HID) and the login Keychain need the GUI
  session (`LimitLoadToSessionType Aqua`). No separate macOS user (ADR-0009 alternative stays deferred).
- **`ProcessType Standard`.** `Background` would clamp the whole process tree, including the `full`
  profile overnight, and coalesce the governor's timers (NF-9). Throttling stays per worker through
  `cpu_priority`.
- **`KeepAlive {SuccessfulExit: false}`**, `ThrottleInterval 60`: restart after a crash or exit 1/2,
  not after a clean stop. Preflight at install catches configuration errors so a crash loop is not
  the way misconfiguration shows up. No network or wake keys, no `caffeinate`: the service never
  prevents sleep.
- **`ExitTimeOut = ceil(grace) + 15`** (FR-17). launchd's default 20 s would SIGKILL a stopping
  executor in the middle of releasing its subtask. The value is also exported as
  `MINION_EXECUTOR_LAUNCHD_EXIT_TIMEOUT_S`, and `run` exits 2 when it is below grace + 15, so an
  edited plist cannot silently reintroduce the kill. `service status` warns on the loaded value.
- **No secrets in the plist.** The token comes from the Keychain; install refuses an env-only token and
  `MINION_EXECUTOR_DEV=1`. Only an allow-list of non-secret `MINION_EXECUTOR_*` variables is copied
  (state/config/cache dirs, cache size, heartbeat, idle unload). Never TOKEN, DEV, TEST_HOOKS,
  SANDBOX, FETCH_SANDBOX, FAKE_SIGNALS, HF_TOKEN or proxy/CA variables. The plist is 0644, written with
  `O_EXCL|O_NOFOLLOW`; `launchctl` is called by absolute path without a shell.
- **No code signing or notarisation.** It needs an Apple Developer ID; the plist is generated locally
  and not signed. This replaces the earlier "signed plist" wording.

## Consequences

- Changing grace or any setting means `service uninstall` + `service install`.
- Reboot and login behaviour is launchd's; the executor still takes its single-instance lock.
- Windows/Linux are unsupported (macOS only).
