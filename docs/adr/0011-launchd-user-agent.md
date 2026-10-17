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
- **`KeepAlive {SuccessfulExit: false}`**, `ThrottleInterval 60`: restart after a crash (non-zero
  exit or a signal), not after a clean stop. Failures a restart cannot fix must not become a 60 s
  crash loop, so under the service (`MINION_EXECUTOR_LAUNCHD_EXIT_TIMEOUT_S` set) `run` exits **0**
  with one log line `permanent_failure reason=...` for: invalid configuration (including a
  too-short launchd timeout), a held instance lock, no usable engine / probe failure, and a server
  answer of 401/403 (`credential_rejected`) or another 4xx except 429 (`request_rejected`) at
  register, acquire or presence. Genuine crashes, `GovernorStoppedError` (exit 1), 429/5xx and
  network errors stay non-zero. Outside the service the same cases exit 2 (config, startup) or 1
  (API). To see why a service stopped: `minion-executor service status` (last exit code, reason)
  and `~/Library/Logs/open-minions/executor.log`. After fixing the cause, `launchctl kickstart
  gui/$UID/<label>` or reinstall.
- **Install preflight.** `service install` (not `--dry-run`) runs the same sandboxed engine probe as
  `run` and refuses (exit 2) when no engine works or the Seatbelt self-test fails, so a broken Mac
  does not install a service that exits at once. It also refuses a non-macOS host, URLs with
  credentials/query/fragment, and an unsafe `~/Library/LaunchAgents` (symlink, not ours, or
  group/other-writable). Uninstall only touches a plist whose `ProgramArguments` are
  `[.../minion-executor, run, ...]`.
- **Logs.** Directory 0700, file 0600 (pre-created), plist `Umask 077`. Not rotated.
- **`ExitTimeOut = ceil(grace) + 15`** (FR-17). launchd's default 20 s would SIGKILL a stopping
  executor in the middle of releasing its subtask. The value is also exported as
  `MINION_EXECUTOR_LAUNCHD_EXIT_TIMEOUT_S`, and `run` refuses to start when it is below grace + 15:
  a hand-edited grace is caught; a hand-edited `ExitTimeOut` is only reported by `service status`,
  which reads both from the installed plist.
- **No secrets in the plist.** The token comes from the Keychain; install refuses an env-only token and
  `MINION_EXECUTOR_DEV=1`. Only an allow-list of non-secret `MINION_EXECUTOR_*` variables is copied
  (state/config/cache dirs, resolved to absolute paths, cache size, min free disk, download timeout,
  heartbeat, idle unload; other `MINION_EXECUTOR_*` variables set at install produce a warning). Never TOKEN, DEV, TEST_HOOKS,
  SANDBOX, FETCH_SANDBOX, FAKE_SIGNALS, HF_TOKEN or proxy/CA variables. The plist is 0644, written with
  `O_EXCL|O_NOFOLLOW`; `launchctl` is called by absolute path without a shell.
- **No code signing or notarisation.** It needs an Apple Developer ID; the plist is generated locally
  and not signed. This replaces the earlier "signed plist" wording.

## Consequences

- `bootout` can answer 36 (stop in progress): uninstall polls `launchctl print` until the service is
  gone (bounded by `ExitTimeOut` + 10 s) and only then removes the plist.
- Changing grace or any setting means `service uninstall` + `service install`.
- Reboot and login behaviour is launchd's; the executor still takes its single-instance lock.
- Windows/Linux are unsupported (macOS only).
