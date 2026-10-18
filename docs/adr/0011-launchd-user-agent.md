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
  crash loop, so under the service `run` exits **0** with one log line
  `permanent_failure ts=<UTC> reason=...` for: invalid configuration (including a too-short launchd
  timeout), an unsafe lock file, no usable engine or a failed sandbox self-test, a missing Keychain
  item, and a server answer of 401/403 *carrying the server's own error code* (`UNAUTHORIZED`,
  `FORBIDDEN`; `credential_rejected`) or 400/422 (`request_rejected`). Everything else exits **1**
  and is relaunched: another executor holding the instance lock, a probe crash or timeout, a locked
  Keychain, a 4xx without a server code (proxy/WAF 403) or 404/408, 429/5xx, network errors,
  `GovernorStoppedError`. Outside the service the same cases exit 2 (config, startup) or 1 (API).
  "Service" means the plist's exit-timeout variable is set *and* `XPC_SERVICE_NAME` equals the
  plist's label (or the parent is launchd, pid 1), so a shell that exports the variable keeps
  normal exit codes. A permanent failure is also written (0600, UTC timestamp) to
  `service_failure.json` in the state dir; `service status` prints it, and the next service start
  clears it. `ErrorBody.code` is constrained to `^[A-Z0-9_]{1,64}$` in `minion-common`, and
  anything echoed from the server is sanitised and truncated. To see why a service stopped:
  `minion-executor service status` and `~/Library/Logs/open-minions/executor.log`. After fixing the
  cause, `launchctl kickstart gui/$UID/<label>` or reinstall.
- **TCC-protected folders.** A LaunchAgent cannot read `~/Documents`, `~/Desktop`, `~/Downloads`,
  iCloud Drive (`~/Library/Mobile Documents`) or `~/Library/CloudStorage` without a Files & Folders
  grant; Python then blocks in `open()` forever. `service install` therefore refuses (exit 2) when
  the executable, `sys.prefix`, any `sys.path` entry or `.pth` target, the state dir, the model cache
  dir or the config file resolves into one of them. The supported install is `uv tool install`
  (non-editable copy, outside those folders); `uv sync` and `uv pip install ./packages/...` create
  editable links into the checkout and are refused.
- **Install preflight.** `service install` (not `--dry-run`) runs the same sandboxed engine probe as
  `run` and refuses (exit 2) when no engine works or the Seatbelt self-test fails, so a broken Mac
  does not install a service that exits at once. It also refuses a non-macOS host, URLs with
  credentials/query/fragment, and an unsafe `~/Library/LaunchAgents` (symlink, not ours, or
  group/other-writable). Uninstall only touches a plist whose `ProgramArguments` are
  `[.../minion-executor, run, ...]`.
- **Admin-group exception.** The install-chain check tolerates group write for the macOS `admin`
  group only: admins can sudo anyway, and stock Homebrew's Cellar is admin-group writable.
- **Logs.** Directory 0700 (an existing directory of ours is tightened), file 0600 (pre-created),
  plist `Umask 077`. Not rotated.
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

- `bootout` answers 0 or 36 (EINPROGRESS) *before* the job has exited (measured: rc 0 after 0.6 s,
  process gone about 5 s later). After either, uninstall polls `launchctl print` until it answers
  113 (bounded by `ExitTimeOut` + 10 s), reports the stop time, and only then removes the plist. A
  missing or unreadable plist with the job still loaded exits 2 with the manual commands.
- Changing grace or any setting means `service uninstall` + `service install`.
- Reboot and login behaviour is launchd's; the executor still takes its single-instance lock.
- Windows/Linux are unsupported (macOS only).

## Addendum (TASK-7.6): menu-bar agent

`minion-executor menubar install|uninstall|status` installs a second, separate LaunchAgent
(`com.open-minions.menubar`) for the menu-bar app, reusing the shared `launchd` module (executable
resolution, install-chain ownership check, agents-dir vetting, `O_EXCL|O_NOFOLLOW` plist write,
`launchctl` wrapper, one `uninstall_agent`); both installs and uninstalls refuse euid 0.
Differences: `LimitLoadToSessionType=Aqua` and `ProcessType=Interactive` (it is UI, not background
work), `ThrottleInterval` 30 s, `KeepAlive {SuccessfulExit: false}` so Quit stays quit until the next login but a crash,
SIGTERM or kill restarts it (a persistent start-up error retries every 30 s; a second instance exits 0
on `menubar.lock`), no `ExitTimeOut` tuning (nothing to release), and no Keychain requirement or engine probe
because it never touches the server. The plist carries no secret; only an explicitly set
`MINION_EXECUTOR_STATE_DIR` / `MINION_EXECUTOR_CONFIG` is copied. It refuses `MINION_EXECUTOR_DEV=1`
and the executor's label; each uninstall only removes a plist whose `ProgramArguments` are its own
(`[..., run, ...]` vs `[..., menubar]`), so neither agent can remove the other. Re-installing an
identical plist is a no-op (loads it if not loaded).
