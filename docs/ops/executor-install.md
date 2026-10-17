# Installing the executor as a service (macOS)

## 1. Install the CLI

```bash
uv tool install './packages/minion-executor[mlx]'
```
Run from a repo checkout (the package is not on PyPI). Extras: `[mlx]`, `[hf]`.

Fallback from a repo checkout: `uv sync --package minion-executor --extra mlx` and use
`.venv/bin/minion-executor` (the service uses the script next to the Python that runs the command).
Run `service install` with that same binary.

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
A permanent failure (bad config, no engine, token rejected with 401/403, other 4xx) is logged as
`permanent_failure reason=...` and the process exits 0, so launchd does not relaunch it every 60 s.
Fix the cause (for a rejected token: `security add-generic-password -U ...`), then
`launchctl kickstart gui/$(id -u)/com.open-minions.executor`. A crash (non-zero exit or signal) is
restarted after 60 s.

The log is not rotated. To shrink it: `: > ~/Library/Logs/open-minions/executor.log` (safe while the
service runs; the file is opened for append).

## 4. Uninstall

```bash
minion-executor service uninstall
```
Sends SIGTERM; the in-flight subtask is finished or released within the grace period.

See ADR-0011 for the design. `pause`/`resume`/`profile` keep working against the service.
