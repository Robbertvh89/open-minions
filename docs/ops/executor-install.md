# Installing the executor as a service (macOS)

## 1. Install the CLI

```bash
uv tool install ./packages/minion-executor
```
For the MLX/HF engines add the extras, e.g. `uv tool install 'minion-executor[mlx]'`.

Fallback from a repo checkout: `uv sync --package minion-executor --extra mlx` and use
`.venv/bin/minion-executor` (the service uses the script next to the Python that runs the command).
Run `service install` with that same binary.

## 2. Put the token in the Keychain

Use the interactive prompt so the token never reaches shell history or `ps`:

```bash
security add-generic-password -s open-minions-executor -a "$USER" -w
```
(You are asked for the password: paste the token.) launchd cannot see `$MINION_EXECUTOR_TOKEN`, so
install refuses an env-only token.

## 3. Install

```bash
minion-executor service install --server https://minions.example.com --dry-run   # print the plist
minion-executor service install --server https://minions.example.com
minion-executor service status
```
Options: `--profile`, `--engines`, `--label` (default `com.open-minions.executor`), `--no-load`.
Non-secret settings exported in the installing shell (`MINION_EXECUTOR_STATE_DIR`, `_CONFIG`,
`_MODEL_CACHE_DIR`, `_MODEL_CACHE_GB`, `_HEARTBEAT_INTERVAL_S`, `_IDLE_UNLOAD_S`, `_STOP_GRACE_S`) are
copied into the plist; nothing else is. Logs: `~/Library/Logs/open-minions/executor.log`.

`ExitTimeOut` is `ceil(MINION_EXECUTOR_STOP_GRACE_S) + 15` s. Change the grace by reinstalling.

## 4. Uninstall

```bash
minion-executor service uninstall
```
Sends SIGTERM; the in-flight subtask is finished or released within the grace period.

See ADR-0011 for the design. `pause`/`resume`/`profile` keep working against the service.
