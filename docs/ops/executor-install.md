# Installing the executor as a service (macOS)

## 1. Install the CLI

```bash
uv tool install './packages/minion-executor[mlx]'
```
Run from a repo checkout (the package is not on PyPI). Extras: `[mlx]`, `[hf]`. Re-run with
`--reinstall` after you update the checkout: the install is a copy, not a link to the checkout.

**The install must not reference ~/Documents, ~/Desktop, ~/Downloads or iCloud Drive.** macOS (TCC)
blocks a LaunchAgent from reading those folders without a Files & Folders grant; the executor then
hangs forever at Python start-up with no error. `service install` refuses (exit 2) when the
executable, `sys.prefix`, any `sys.path` entry or `.pth` target, the state dir, the model cache dir
or the config file is under one of them (`~/Library/Mobile Documents` and `~/Library/CloudStorage`
too). The checkout itself may live there: only the installed copy matters.

Why `uv tool install` works: it builds `minion-executor` and its workspace dependency
`minion-common` as regular (non-editable) packages into `~/.local/share/uv/tools/minion-executor`
(checked: `direct_url.json` says `"editable": false`, there are no `.pth` files pointing at the
checkout, `minion_common` imports from the tool's `site-packages`). Do **not** use `uv sync` or
`uv pip install ./packages/...` for the service: both leave editable `.pth` links into the checkout
(the repo `.venv` is refused by `service install`).

Fallback without `uv tool` (wheels, then a venv outside the protected folders):

```bash
uv build --package minion-common --package minion-executor --out-dir /tmp/minion-wheels
uv venv ~/.local/share/minion-venv
uv pip install --python ~/.local/share/minion-venv/bin/python --no-sources \
    /tmp/minion-wheels/minion_common-*.whl /tmp/minion-wheels/minion_executor-*.whl
~/.local/share/minion-venv/bin/minion-executor service install ...
```
(`--no-sources` keeps uv from resolving `minion-common` to the workspace path.) The service uses
the script next to the Python that runs `service install`, so run it with that binary. The engine
extras (`mlx`, `hf`) are added by installing their dependencies into the same venv.

## 2. Put the token in the Keychain

Use the interactive prompt so the token never reaches shell history or `ps`:

```bash
security add-generic-password -s open-minions-executor -a "$USER" -w
```
(You are asked for the password: paste the token.) launchd cannot see `$MINION_EXECUTOR_TOKEN`, so
install refuses an env-only token. To replace the token later, add `-U` to the same command.

## 3. Install

```bash
minion-executor service install --server https://minions.example.com --dry-run   # print the plist
minion-executor service install --server https://minions.example.com
minion-executor service status
```
Install checks like `run` does, including a sandboxed engine probe (it fails with exit 2 if no
engine works or the sandbox self-test fails; `--dry-run` skips the probe).

Options: `--profile`, `--engines`, `--label` (default `com.open-minions.executor`), `--no-load`.
Non-secret settings exported in the installing shell (`MINION_EXECUTOR_STATE_DIR`, `_CONFIG`,
`_MODEL_CACHE_DIR`, `_MODEL_CACHE_GB`, `_MIN_FREE_DISK_GB`, `_DOWNLOAD_TIMEOUT_S`,
`_HEARTBEAT_INTERVAL_S`, `_IDLE_UNLOAD_S`, `_STOP_GRACE_S`) are copied into the plist, paths as
absolute; any other `MINION_EXECUTOR_*` variable gets a warning and is not carried.

Not carried into the service: `HF_TOKEN` and the proxy / CA variables (`HTTPS_PROXY`,
`SSL_CERT_FILE`, ...). Models that need a Hugging Face token or a proxy cannot be fetched by the
service; pre-seed the cache or run in the foreground.

`pause`, `resume`, `profile` and `status` read the state directory: run them with the same
`MINION_EXECUTOR_STATE_DIR` as at install, or they talk to a different (empty) directory.

`ExitTimeOut` is `ceil(MINION_EXECUTOR_STOP_GRACE_S) + 15` s. Change the grace by reinstalling.

## Why did it stop?

```bash
minion-executor service status          # state, last exit code / reason, ExitTimeOut
tail -n 50 ~/Library/Logs/open-minions/executor.log
```
A permanent failure (bad config, no usable engine, failed sandbox self-test, missing Keychain item,
unsafe lock file, the server's own 401/403 `UNAUTHORIZED`/`FORBIDDEN`, 400/422) is logged as
`permanent_failure ts=<UTC> reason=...` and the process exits 0, so launchd does not relaunch it
every 60 s. `service status` shows the last one (`last permanent failure: ...`, kept in the state
dir until the service starts again). Fix the cause (for a rejected token:
`security add-generic-password -U ...`), then
`launchctl kickstart gui/$(id -u)/com.open-minions.executor`.

Transient problems exit 1 and are relaunched after 60 s: another executor holding the state-dir
lock, a probe crash or timeout, a locked Keychain, a 403/404/408 without a server error code (a
proxy or WAF), 429/5xx, network errors, crashes. 429 at register/acquire is retried in the loop with
backoff (honouring `Retry-After`, capped at 300 s).

The log is not rotated. To shrink it: `: > ~/Library/Logs/open-minions/executor.log` (safe while the
service runs; the file is opened for append).

## 4. Uninstall

```bash
minion-executor service uninstall
```
Sends SIGTERM; the in-flight subtask is finished or released within the grace period. `launchctl
bootout` can return before the job has exited, so uninstall polls `launchctl print` until the
service is gone (at most `ExitTimeOut` + 10 s), prints how long that took, and only then removes
the plist.

If the plist is missing or unreadable, uninstall checks `launchctl print`: when the job is still
loaded (or launchd cannot be queried, or an unreadable file is there) it exits 2 and leaves
everything alone. Remove it by hand:

```bash
launchctl bootout gui/$(id -u)/com.open-minions.executor
rm ~/Library/LaunchAgents/com.open-minions.executor.plist
```

## 5. Menu bar at login (optional)

```bash
minion-executor menubar install --dry-run   # preview the plist
minion-executor menubar install             # ~/Library/LaunchAgents/com.open-minions.menubar.plist
minion-executor menubar status
minion-executor menubar uninstall
```
A second LaunchAgent, independent of the executor service (own label and plist; uninstalling one never
touches the other). It starts `minion-executor menubar` in your GUI (Aqua) session only, as
`ProcessType=Interactive`, restarts it after a crash or SIGTERM/kill, but not after Quit (`KeepAlive`
`SuccessfulExit=false`; Quit stays quit until your next login, or `launchctl kickstart`), retries a
persistent start-up error every 30 s, runs only one instance (`menubar.lock` in the state dir; a second
start exits 0), and logs to `~/Library/Logs/open-minions/com.open-minions.menubar.log`.
The plist holds no token or other secret, only `MINION_EXECUTOR_STATE_DIR` / `MINION_EXECUTOR_CONFIG`
when you set them (other `MINION_EXECUTOR_*` variables are warned about, not carried). The same rules as
the service apply, plus: not as root, and the executable, its Python and every parent directory must be
owned by you or root and not group/other writable (shared `/tmp` is refused); install the tool outside Documents, Desktop,
Downloads and iCloud Drive, and not with `MINION_EXECUTOR_DEV=1`. It re-runs idempotently (an identical
plist is left alone); a different plist needs `menubar uninstall` first.

See ADR-0011 for the design. `pause`/`resume`/`profile` keep working against the service.
