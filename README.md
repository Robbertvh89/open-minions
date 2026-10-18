# open-minions

Open-source implementation of *Minions: A Dynamic and Distributed Virtual GPU Cloud Built on Idle
Apple Silicon Devices* (Li et al., [arXiv:2610.12970](https://arxiv.org/abs/2610.12970)).

Idle Apple Silicon Macs run an **executor** that *pulls* batch inference subtasks (embeddings, LLM
completions) from a central **server**, runs them on the local GPU with [MLX](https://github.com/ml-explore/mlx)
and writes the results to S3. The owner of the Mac chooses a **capacity budget** (memory, GPU duty
cycle, batch size, CPU priority), and the executor yields to them: it pauses under memory pressure,
thermal stress or low battery, stops cleanly on SIGTERM and never keeps the Mac awake.

There is no central scheduler. Coordination happens through PostgreSQL (behind a REST API) and a
unique-constraint acquisition protocol: whichever executor inserts the row into
`minion_subtask_acquirement` first owns the subtask. A **watchdog** reclaims subtasks from executors
that stop sending heartbeats. Large payloads never pass through the server: executors only hold
short-lived presigned S3 URLs and never talk to PostgreSQL.

```
 submitter (minion CLI / web UI / REST)
        |  HTTPS + user API key              uploads input JSONL, downloads results
        v                                         |
  +-------------+   SQL   +------------+          v
  | server/API  |<------->| PostgreSQL |     +---------+
  | (FastAPI)   |         +------------+     |   S3    |
  +------+------+              ^             +----+----+
         ^                     | SQL              ^
         | REST + executor     |                  | presigned GET / POST
         | token (pull)   +----+-------+          |
         |                | watchdog   |          |
         |                | (reclaim)  |          |
   +-----+----------------+------------+----------+-----+
   |  executor (Mac)   executor (Mac)   executor (Mac)  ...
   |  poll -> acquire -> run in sandboxed worker -> upload -> report
   +-----------------------------------------------------+
```

Docs: [Specification](docs/SPEC.md) (what it must do) | [Architecture](docs/ARCHITECTURE.md) (how it
is built) | [Decision records](docs/adr/).

## Status and limitations

Milestones M0 to M5 (protocol, fault tolerance, real engines, capacity governor, ops usability and
the `minion` submitter CLI) are implemented and tested, except the human NF-7 study (see below). The scale test and a menu-bar app (M6) are
not. Some things are measured only on one Mac, or not at all; read [Known limitations](#known-limitations)
before you rely on it, in particular if you plan to run it on people's work laptops.

## Quick start (local, about 10 minutes, no GPU needed)

This runs the whole stack on one machine: PostgreSQL, S3 (SeaweedFS), Grafana, the server and the
watchdog in Docker, and one executor natively with the **fake engine** (deterministic, no model, no
GPU). You can follow it on Linux or macOS; real engines need an Apple Silicon Mac
([next section](#using-real-models-on-a-mac)).

**Prerequisites:** Docker with Compose v2, [`uv`](https://docs.astral.sh/uv/), Python 3.12+ (uv
installs it), git. Free host ports 8080 (server), 5432, 9000 and 3000. If one is taken, set
`MINION_SERVER_HOST_PORT`, `MINION_PG_PORT`, `MINION_S3_PORT` or `MINION_GRAFANA_PORT` (for example
`export MINION_SERVER_HOST_PORT=18080`) and use that port below. If you change the S3 port, set
`MINION_CLI_S3_ENDPOINT_URL` (step 8) to match.

**1. Clone and install**

```bash
git clone https://github.com/Robbertvh89/open-minions.git && cd open-minions
uv sync
```

**2. Create an API key and the secrets directory.** The key is printed once; the keys file only
holds its hash.

```bash
mkdir -p deploy/secrets && chmod 700 deploy/secrets
out=$(uv run minion-server-admin generate-api-key --name demo --teams demo)
export MINION_API_KEY=$(printf '%s\n' "$out" | sed -n 's/^api key (shown once): //p')
printf '%s\n' "$out" | sed -n '/^keys:/,$p' > deploy/secrets/api-keys.yaml
chmod 600 deploy/secrets/api-keys.yaml
```

On **Linux** the server runs as uid 10001 and refuses a keys file owned by anyone else: it fails to
start (restart loop, `up --wait` fails, and `docker compose logs server` shows the ownership error).
Give it the files:
`sudo chown -R 10001:10001 deploy/secrets`. (Alternative: keep them `root:root`, directory 0755 and
file 0644.) On macOS Docker Desktop this is not needed.

**3. Optional: enable the web UI.** The UI is off (404) unless a session secret is set before the
server starts.

```bash
export "$(uv run minion-server-admin generate-ui-secret)"
```

**4. Start the stack** with the dev override, which adds the models `fake-embed` and
`fake-complete` to the server's model allow-list (never use that file in production):

```bash
docker compose -f deploy/compose.yml -f deploy/compose.dev.yml --profile app up -d --build --wait
curl -sf http://localhost:8080/healthz
```

The first build takes a few minutes. `up --wait` returns when postgres, S3, the server and the
watchdog are healthy.

**5. Issue an executor token** (printed once; the server stores only a hash):

```bash
docker compose -f deploy/compose.yml -f deploy/compose.dev.yml --profile app exec -T server \
  minion-server-admin issue-executor-token --owner demo
```

The command prints `executor_id: ...` and `token (shown once): ...`. Export the token without
putting it in your shell history: run this, paste the token (nothing is echoed), press Enter:

```bash
read -rs MINION_EXECUTOR_TOKEN && export MINION_EXECUTOR_TOKEN
```

**6. Run a dev executor with the fake engine** (in a second terminal, in the repo, with
`MINION_EXECUTOR_TOKEN` exported there too). `MINION_EXECUTOR_DEV=1` is what allows the `fake`
engine. On a Mac the fake engine runs in the normal Seatbelt sandbox (checked). The executor
pauses on battery power and under memory pressure by default, so on a laptop plug in the charger or
submit will wait forever (see Troubleshooting below).

```bash
MINION_EXECUTOR_DEV=1 uv run minion-executor run --server http://localhost:8080 --engines fake
```

On Linux there is no Seatbelt sandbox; the worker skips it for the fake engine only (any other engine
fails closed), so no extra flag is needed. The Linux path is untested.

`MINION_EXECUTOR_DEV=1` and `MINION_EXECUTOR_SANDBOX=0` are for local development only. Never set
them on a machine that runs real tasks.

**7. Create sample input.** One JSON object per line. Embedding input needs `id` and `text`:

```bash
cat > embed.jsonl <<'EOF'
{"id": "a1", "text": "The quick brown fox jumps over the lazy dog."}
{"id": "a2", "text": "Idle laptops make a cheap GPU cloud."}
{"id": "a3", "text": "Embeddings turn text into vectors."}
EOF
```

Completion input needs `id` and either `prompt` or `messages` (chat turns):

```bash
cat > complete.jsonl <<'EOF'
{"id": "c1", "prompt": "Write a haiku about idle laptops."}
{"id": "c2", "messages": [{"role": "user", "content": "Name three prime numbers."}]}
{"id": "c3", "prompt": "Translate 'good morning' to French."}
EOF
```

**8. Submit.** The CLI is configured through `MINION_CLI_*` variables. The S3 values below are the
dev stack's loopback defaults; a real deployment uses `https` and scoped credentials
([Submitters](#submitters)).

```bash
export MINION_CLI_SERVER=http://localhost:8080 MINION_CLI_TEAM=demo
export MINION_CLI_S3_ENDPOINT_URL=http://localhost:9000 MINION_CLI_S3_BUCKET=minion
export MINION_CLI_S3_ACCESS_KEY=minion MINION_CLI_S3_SECRET_KEY=minion-secret
uv run minion submit --type embedding --model fake-embed embed.jsonl -o out-embed --name demo-embed
uv run minion submit --type completion --model fake-complete complete.jsonl -o out-complete --name demo-complete
```

`submit` uploads the file to S3, creates the task, waits for it and downloads the results. Look at
what you got (`head` shows record content, so only do this with data you can see):

```bash
ls -a out-embed               # results.jsonl  manifest.json  parts/  .minion.lock
head -n 3 out-embed/results.jsonl
```

Each line is `{"id", "embedding" | "completion", "model", "model_revision", ...}`. The fake engine
returns an 8-dimensional hash vector and echoes the prompt.

**9. Follow up** (these work for any task by name, also after Ctrl-C of `submit`, which never
cancels):

```bash
uv run minion status demo-embed       # status and subtask counts; exit code follows the status
uv run minion fetch demo-embed -o out-embed    # re-download; finished parts are skipped
uv run minion submit --type embedding --model fake-embed embed.jsonl --no-wait --name demo-two
uv run minion cancel demo-two         # the only way to cancel
```

The instant fake engine may finish `demo-two` before you cancel; `cancel` then says `TASK_FINISHED`
(`409`, exit 2). To see a cancel, stop the executor first (Ctrl-C), submit, cancel, then restart it.

**10. Look around.** Web UI: <http://localhost:8080/ui> (log in with the API key from step 2; you
see the task list, details, the create form and cancel). Grafana: <http://localhost:3000>, login
`admin` / `admin` (dev default), dashboards `open-minions: tasks` and `open-minions: fleet`.

**11. Tear down.** Stop the executor with Ctrl-C in its terminal (it finishes or releases the
current subtask first).

```bash
docker compose -f deploy/compose.yml -f deploy/compose.dev.yml --profile app down
rm -rf embed.jsonl complete.jsonl out-embed out-complete
rm -rf deploy/secrets      # on Linux after the chown: sudo rm -rf deploy/secrets
```

`down` keeps the data volumes, so old tasks survive and an executor (FIFO) would process these
leftovers first next time. For a clean slate use `down -v` instead (this **deletes** all dev
data: database, S3 objects, Grafana).

**Troubleshooting.** `submit` waits and nothing happens: run `uv run minion-executor status`; it
prints `state: paused (<reason>)`. On battery the executor is paused by default (`on_battery`; with
`allow_on_battery = true` it pauses as `low_battery` once the charge is under the minimum): plug in,
or set `allow_on_battery = true` in `~/.config/minion/executor.toml` ([Mac owners](#mac-owners)) and
restart the executor, which reads that file only at start. It also pauses at a memory-pressure
warning.

## Using real models on a Mac

On an Apple Silicon Mac, reuse Quick start steps 2, 5 and 8, but start the stack **without**
`compose.dev.yml` (the stock model allow-list, no fake models), and in step 8 submit with
`--model bge-small-en` or `--model qwen2.5-0.5b-instruct`. Start the stack and issue the token with:

```bash
docker compose -f deploy/compose.yml --profile app up -d --build --wait
docker compose -f deploy/compose.yml --profile app exec -T server \
  minion-server-admin issue-executor-token --owner <name>
```

Then install the engines and run without the dev flags (if you changed a host port, use it in
`--server` and the `MINION_CLI_*` values):

```bash
uv sync --package minion-executor --extra mlx --extra hf     # mlx: default engine; hf: fallback
read -rs MINION_EXECUTOR_TOKEN && export MINION_EXECUTOR_TOKEN   # paste the token, Enter (no echo)
# or use the Keychain (recommended, see Mac owners)
uv run minion-executor run --server http://localhost:8080 --profile light
```

The first subtask loads the model: a few seconds with the weights cached (see
[`docs/bench/m3-apple-m3-pro.md`](docs/bench/m3-apple-m3-pro.md)), longer when it must download first.

- `--engines auto` (the default) probes `mlx` and `hf` in a sandboxed worker subprocess and
  advertises what works (exit 2 if none). Ollama is opt-in (`--engines ollama`, see
  [ADR-0009](docs/adr/0009-offline-sandboxed-inference-worker.md)). `fake` needs `MINION_EXECUTOR_DEV=1`.
- The server only accepts models from its allow-list, [`deploy/models.yaml`](deploy/models.yaml):
  `qwen2.5-0.5b-instruct` (completion, mlx), `bge-small-en` (embedding, mlx) and their `-hf` variants.
  Each entry pins a revision. Models are downloaded on first use into the model cache by a pinned,
  sha256-verified subprocess, so the first subtask on a Mac is slow and needs network access to the
  Hugging Face hub.
- Profiles `light` (25 % of RAM, 30 % GPU duty, batch 4), `balanced` (40 %, 50 %, 8), `full`
  (70 %, 100 %, 32) and `custom`. Omit `--profile` for adaptive mode ([Mac owners](#mac-owners)).
- Measure your own Mac: `uv run minion-executor bench --models deploy/bench-models.yaml --model <id>`
  (`uv run minion-executor bench --help` lists all modes). Results from an M3 Pro: [`docs/bench/`](docs/bench/m3-apple-m3-pro.md).
  Measurement procedures: [`docs/bench/governor-measurements.md`](docs/bench/governor-measurements.md).

## Guides

### Operators

You run the server, database, S3, watchdog, Grafana and hand out keys and tokens.

**Deploy.** `deploy/compose.yml` is a dev stack (default passwords, no TLS, loopback ports). For
production use managed PostgreSQL 16, an S3 bucket, at least two server replicas and one or two
watchdogs, one deployment per team or organisation for isolation. The container image
(`deploy/Dockerfile`) runs `migrate`, `minion-server` and `minion-watchdog`. Run the server behind a
reverse proxy that terminates **TLS** (required outside local dev, S-6), forwards `Host` with the port
and applies per-key/per-IP rate limits on `/api/v1/tasks`, `/api/v1/executors` and `POST /ui/login`
(S-12, S-13). Run `migrate` as the schema owner and the server as a non-superuser DML-only role. All
settings are `MINION_*` environment variables; the documented list is [`.env.example`](.env.example).
Details: [ARCHITECTURE §6](docs/ARCHITECTURE.md).

**API keys** (user credentials) live in a YAML file named by `MINION_API_KEYS_FILE`, no database
table ([ADR-0007](docs/adr/0007-api-keys-config-file.md)):

```bash
uv run minion-server-admin generate-api-key --name <who> --teams a,b [--operator] [--expires-days N]
```

It prints the key once and the entry (a SHA-256 hash) to add under `keys:`. `--operator` grants the
fleet view (`GET /executors`, `/ui/fleet`); `--expires-days` (1..3650) sets an expiry. The file is
re-read when it changes (`MINION_API_KEYS_RELOAD_S`, 5 s), so deleting an entry revokes the key
within that interval. Replace the file atomically (temp file plus rename); it must be on local disk,
owned by root or the server user and not group/world-writable, else the server refuses it and fails
closed. Per-team pool policy (`default_pool`, `allowed_pools`) is described in SPEC S-1.

**Executor tokens** (per-executor, hashed, revocable, 90 days by default):

```bash
uv run minion-server-admin issue-executor-token --owner <name> [--pool default] [--ttl-days 1..365]
uv run minion-server-admin list-executors            # never prints token hashes
uv run minion-server-admin rotate-executor-token --executor-id <uuid> [--ttl-days N]   # same id, new token once
uv run minion-server-admin revoke-executor-token --executor-id <uuid>
```

In the compose stack run these with `docker compose ... exec -T server minion-server-admin ...` as in
the quick start. A revoked or expired token gets `401` on its next request. Pools restrict which
executors may run a task (`allowed_pool`), the control for confidential data.

**Web UI.** Set `MINION_UI_SESSION_SECRET` (at least 32 bytes; make it with
`uv run minion-server-admin generate-ui-secret`) and, in production, `MINION_UI_ALLOWED_HOSTS`
(DNS-rebinding guard). Users log in with their API key; the session lasts `MINION_UI_SESSION_TTL_S`
(8 h). See [ADR-0010](docs/adr/0010-web-ui-session-auth.md).

**Grafana and observability.** Dashboards are provisioned from `deploy/grafana/dashboards/`. Grafana
connects as `minion_grafana`, a member of `minion_readonly`, which can only `SELECT` from the
`minion_ro` views (no token hashes, params or object keys). Set `MINION_GRAFANA_DB_PASSWORD` and
`GF_SECURITY_ADMIN_PASSWORD` outside dev. The fleet dashboard shows per-owner activity: keep Grafana
operator-only, and point it at a read replica in production (the role cannot stop a hostile Grafana
user from exhausting CPU or memory). `/healthz` is always on; Prometheus `/metrics` is off unless
`MINION_METRICS_ENABLED=true`, and with `MINION_METRICS_TOKEN` (32+ chars) it requires
`Authorization: Bearer <token>`. Logs never contain payload content.

**Rate limits.** Executor endpoints are rate-limited in memory per replica (`429` with
`Retry-After`); the intervals and defaults are in [`.env.example`](.env.example) and
[ADR-0012](docs/adr/0012-in-memory-executor-rate-limits.md). Put per-IP limits at the proxy.

**Storage and S3 policy.** Executors only get presigned URLs (default TTL 900 s); give each team's
submitters their **own** scoped credentials, never the server's, and add lifecycle rules and a
bucket quota. The exact policy is in [SPEC S-14](docs/SPEC.md) and S-2.

**Watchdog.** `uv run minion-watchdog` (DB access only, no S3) reclaims subtasks whose heartbeat is
older than `MINION_HEARTBEAT_TIMEOUT_S` (900 s) or whose lease exceeds `MINION_MAX_LEASE_S` (6 h),
and sweeps stragglers of cancelled tasks. Keep the timeout at 2 to 3 times the executor heartbeat
interval (300 s default). The compose healthcheck uses a liveness file. See
[ADR-0008](docs/adr/0008-fault-tolerance-reclaim-and-cancel.md).

**Not covered here:** database backups and restore, S3 replication, Postgres high availability.
Plan for them yourself.

### Mac owners

You donate idle capacity and keep working. Full install notes:
[`docs/ops/executor-install.md`](docs/ops/executor-install.md).

**Menu-bar app:** `uv run minion-executor menubar` shows a status item to pause, resume and pick
the profile without a terminal. Launch it at login with `uv run minion-executor menubar install`
(`--dry-run` previews the plist; `menubar uninstall` removes it; independent of the executor service).

**Install as a background service** (per-user LaunchAgent, [ADR-0011](docs/adr/0011-launchd-user-agent.md)).
Install a non-editable copy (launchd cannot read `~/Documents`, `~/Desktop`, `~/Downloads` or iCloud
Drive, and `service install` refuses an install that references them), put the token in the Keychain
and install:

```bash
uv tool install './packages/minion-executor[mlx]'
security add-generic-password -s open-minions-executor -a "$USER" -w     # paste the token when asked
minion-executor service install --server https://minions.example.com --dry-run   # print the plist
minion-executor service install --server https://minions.example.com
minion-executor service status
```

To remove it again: `minion-executor service uninstall`.

Options: `--profile`, `--engines`, `--label`, `--no-load`. For real use keep the token in the Keychain
(launchd cannot see `$MINION_EXECUTOR_TOKEN`, so the service needs the Keychain item anyway). The log is
`~/Library/Logs/open-minions/executor.log`.

**Profiles.** `--profile light|balanced|full|custom` pins a profile. Without `--profile` the
executor is **adaptive**: it starts on `active_profile` from `executor.toml` (default `light`) and
switches to `idle_profile` (default `full`) when you have been away for `idle_after_min` (10) on AC
power. Memory use is capped at 75 % of RAM for every profile.

```bash
uv run minion-executor pause [--for 1h]            # no new subtasks; the current one finishes or is released
uv run minion-executor resume
uv run minion-executor profile <light|balanced|full|custom|auto>   # auto = back to the run's pin, or adaptive
uv run minion-executor status                      # profile, budget, paused reason, current subtask, signals
```

The commands talk to the running executor through `control.json` and `status.json` in
`$MINION_EXECUTOR_STATE_DIR` (default `~/Library/Application Support/open-minions`); use the same
variable as at install. `status` exits 1 if no executor is running.

**Automatic pauses.** No new work, and the running subtask is released after 60 s (at once under
critical memory pressure), on: memory pressure, serious/critical thermal state, battery power (unless
`allow_on_battery = true`, then below 30 %), Low Power Mode, an unusable `control.json`, or signals
that cannot be read (`signals_unavailable`). Stop it
any time with SIGTERM or `launchctl bootout`; the in-flight subtask finishes within
`MINION_EXECUTOR_STOP_GRACE_S` (30) or is released.

**`executor.toml`** at `~/.config/minion/executor.toml` (or `$MINION_EXECUTOR_CONFIG`): owner-only
permissions, unknown keys are an error (exit 2). Keys: `active_profile`, `idle_profile`,
`idle_after_min`, `allow_on_battery`, `min_battery_percent`, `on_call`, `[profiles.<name>]`
overrides, `[profiles.custom]`, `[[schedule]]` (profile, `HH:MM` start/end, days).

**`on_call` and privacy.** The governor can notice that a microphone or camera is in use and cap or
pause the executor (`on_call = auto|light|pause|ignore`); no device is opened. The server learns
your profile, memory budget and pause reason, which can reveal when you are idle or on a call;
keyboard idle time and mic/camera state are never sent. Details and how to opt out: [SPEC
S-11](docs/SPEC.md). Prefer a task or team with
`minion-executor prefer --server URL (--team T [--project P] [--task-name N] | --clear)`; it is a
hint, not a filter.

**What stays on your Mac.** The executor downloads one input slice per subtask, runs it, uploads the
output and deletes the local files (S-4). It never logs payload content and never writes it into
`status.json`. Submitters' data is processed on your machine, so only join a pool that the operator
and your organisation have agreed on, and expect the operator to restrict confidential tasks to
trusted pools (S-1).

**Sandbox and model cache.** Inference runs in a worker subprocess in a macOS Seatbelt sandbox with no
network, no process exec and no access to your home directory (S-10, ADR-0009). Models are fetched by
a separate sandboxed, pinned and checksum-verified process into
`MINION_EXECUTOR_MODEL_CACHE_DIR` (default `~/Library/Caches/open-minions/models`), LRU-capped at
`MINION_EXECUTOR_MODEL_CACHE_GB` (50). Only one executor may run per state directory. The idle worker
unloads its model after `MINION_EXECUTOR_IDLE_UNLOAD_S` (600).

### Submitters

You send JSONL in and get JSONL out. Your operator gives you a server URL, a team, an API key and
S3 credentials scoped to your team.

**CLI.** `uv run minion` (or the `minion` script):

```bash
minion submit --type embedding|completion --model <id> FILE... [-o DIR] [--name N] [--server URL] \
  [--team T] [--project P] [--no-wait] [--timeout S] [--records-per-subtask N] [--priority N] \
  [--pool P] [--params JSON]
minion status NAME [--server URL] [--team T] [--project P]
minion fetch NAME [-o DIR] [--wait] [--timeout S] [--server URL] [--team T] [--project P]
minion cancel NAME [--server URL] [--team T] [--project P]
```

`FILE...` are input JSONL files, in output order. `--params` takes a JSON object (completion:
`temperature` 0..2, `top_p`, `max_tokens` 1..4096, `seed`, `system`; embedding: `normalize`; unknown
keys are rejected with 422). Results land in `-o DIR` (default `./NAME`): `results.jsonl`,
`manifest.json`, `parts/` and a `.minion.lock`. Re-running `fetch` resumes; a directory that belongs
to another task is refused.

Configuration is the environment (there is no config file):

| Variable | Meaning |
|---|---|
| `MINION_API_KEY` | your API key; or the Keychain item `open-minions-api-key` (`security add-generic-password -s open-minions-api-key -a "$USER" -w`) |
| `MINION_CLI_SERVER` | server URL (`https`, or `http` to loopback) |
| `MINION_CLI_TEAM`, `MINION_CLI_PROJECT` | team (required, must be in your key's teams) and project (default `default`) |
| `MINION_CLI_S3_ENDPOINT_URL` | S3 endpoint (`https`, or `http` to loopback); unset = AWS default |
| `MINION_CLI_S3_BUCKET`, `MINION_CLI_S3_REGION` | bucket (`minion`), region (`us-east-1`) |
| `MINION_CLI_S3_ACCESS_KEY`, `MINION_CLI_S3_SECRET_KEY` | your team-scoped credentials; unset = boto3 default chain |
| `MINION_CLI_INPUT_PREFIX`, `MINION_CLI_OUTPUT_PREFIX` | `inputs/`, `outputs/` |

**Exit codes**

| Code | Meaning |
|---|---|
| 0 | task `SUCCEEDED` |
| 1 | task `FAILED` or `CANCELLED` |
| 2 | usage, config, auth or other 4xx error |
| 3 | task `PARTIAL` (some subtasks failed; fetch what succeeded) |
| 4 | timeout, or server (also 5xx) or S3 unreachable |
| 130 | Ctrl-C (the task keeps running; `minion cancel` is the only way to cancel) |

**Input and output.** Embedding input: `{"id", "text"}`. Completion input: `{"id", "prompt"}` or
`{"id", "messages": [{"role", "content"}]}`. Output lines carry `id`, `embedding` (plus `tokens`) or
`completion` (plus `finish_reason`, `prompt_tokens`, `completion_tokens`), and always `model` and
`model_revision`. A record that cannot be processed yields `{"id", "error": "PROMPT_TOO_LONG", ...}`
instead of failing the subtask. Input is split by record count (`--records-per-subtask`, default 500);
`results.jsonl` joins the parts in order. Spec: [SPEC §4.1](docs/SPEC.md).

**REST API** under `/api/v1`, JSON, `Authorization: Bearer <api key>`. Task create takes S3
*prefixes* (the CLI uploads for you; with the raw API you upload first). This example reuses the
input that `minion submit` uploaded for `demo-embed` in the quick start; `params` is optional:

```bash
curl -s -H "Authorization: Bearer $MINION_API_KEY" -H 'Content-Type: application/json' \
  -X POST http://localhost:8080/api/v1/tasks -d '{
    "team": "demo", "project": "default", "task_name": "api-demo", "type": "embedding",
    "model": "fake-embed", "input_uri": "s3://minion/inputs/demo/default/demo-embed/",
    "output_uri": "s3://minion/outputs/demo/default/api-demo/"}'
curl -s -H "Authorization: Bearer $MINION_API_KEY" \
  http://localhost:8080/api/v1/tasks/demo/default/api-demo
```

The response is the task (`202`, status `SPLITTING`, later `PENDING`, `RUNNING`, `SUCCEEDED`).

Other endpoints: `GET /tasks`, `GET /tasks/{team}/{project}/{task_name}/subtasks`,
`POST /tasks/{team}/{project}/{task_name}/cancel`, and (operator keys) `GET /executors`. Errors are
`{"detail": {"code", "message"}}`. Full reference: [SPEC §6](docs/SPEC.md).

**Web UI.** `/ui` (when the operator enabled it): log in with your API key, list and inspect tasks,
create and cancel them. It is team-scoped like the API.

## Development

```
packages/
  minion-common/     shared Pydantic schemas, enums, typed API client
  minion-server/     FastAPI app: task API, executor API, splitter, repositories, web UI, admin CLI
  minion-executor/   macOS agent: governor, worker subprocess, runners (mlx, hf, ollama, fake), CLI
  minion-watchdog/   heartbeat monitor; requeues orphaned subtasks
  minion-cli/        `minion` submitter CLI
migrations/          Alembic
deploy/              docker-compose, Dockerfile, models, Grafana, launchd
docs/                SPEC, ARCHITECTURE, ADRs (docs/adr), ops, bench, usability
```

```bash
uv sync
cp .env.example .env
docker compose -f deploy/compose.yml up -d         # postgres + s3 + grafana only
uv run alembic upgrade head
uv run minion-server                               # native server on :8080
uv run pytest -m "not integration"                 # fast unit tests
uv run pytest                                      # all tests; integration needs Docker (testcontainers)
uv run ruff check . && uv run ruff format --check . && uv run mypy packages migrations conftest.py
```

Test markers: `integration` (Docker: Postgres and S3 via testcontainers, including the acquisition
race), `macos` (Seatbelt sandbox tests, skipped elsewhere), `gpu` (real MLX models; `pytest -m "gpu or model"`),
`model` (downloads a tiny real model; opt in), `ollama` (needs a live daemon; opt in).

Invariants you must not break (details in [CLAUDE.md](CLAUDE.md)): executors never talk to
PostgreSQL; ownership is decided only by the `INSERT ... ON CONFLICT DO NOTHING` into
`minion_subtask_acquirement`; only the current owner may mutate a subtask (stale `acquirement_id` is
`409`); large payloads bypass the server; execution is idempotent per `subtask_id` + `attempt`; the
executor yields to the user; inference runs in a worker subprocess.

Workflow: every change starts with a [Backlog.md](backlog/) task, one branch and PR per change,
Conventional Commits, done by a pipeline of project subagents (`.claude/agents/`) as described in
CLAUDE.md and `.claude/skills/feature-workflow`. Decisions that deviate from the paper get an ADR in
`docs/adr/`. Performance claims about engines need a `minion-executor bench` result
([`docs/bench/`](docs/bench/)).

## Known limitations

- **NF-7 (no visible harm during video calls) is not met everywhere.** The synthetic foreground
  proxy ([`docs/bench/m5-foreground-apple-m3-pro.md`](docs/bench/m5-foreground-apple-m3-pro.md)) fails for the
  `minilm` embedding model at `light` (+1.11 percentage points of missed frames) and `balanced`
  (+1.56 pp); the completion model passes. `minilm` is a bench-only model; the served embedding
  model `bge-small-en` was not measured. The proxy is confounded by GPU clock scaling (DVFS: a
  probe frame takes about 22 ms on an idle GPU and 9.5 ms under full load). Tracked in TASK-24.
- **The human NF-7 study has not been run.** [`docs/usability/nf7-video-call-protocol.md`](docs/usability/nf7-video-call-protocol.md)
  needs at least 8 people on their own Macs. Until then, do not claim calls are unaffected.
- **Real microphone/camera call detection is not verified on hardware** (`on_call`); it is covered by
  tests with injected signals only. Screen sharing is not detected.
- **GPU duty cycle is enforced by the worker pacing itself**, not by the control process (TASK-15):
a misbehaving or compromised worker can exceed it.
  Measured on one Mac (Apple M3 Pro, [`docs/bench/`](docs/bench/)); other chips are untested.
- **Output size is capped by the S3 policy, not the quota.** The presigned POST refuses a body above
  `MINION_MAX_OUTPUT_BYTES`, but attempts accumulate under `_attempts/`: use a bucket quota and
  lifecycle rules (S-2, [ADR-0017](docs/adr/0017-presigned-post-output-uploads.md)). Executors older
  than this release are refused with `426 EXECUTOR_OUTDATED`.
- **Signed `Range` reads are not verified on SeaweedFS** (the dev S3); AWS S3 and MinIO enforce them.
  A `check-storage` admin command is planned (TASK-20).
- **Server logs are plain, not JSON** (the watchdog's are structured; TASK-17). Task list/detail
  aggregate subtask counts per request (TASK-16); rate-limit the read endpoints at the proxy.
- **Ollama is opt-in and not benchmarked** (TASK-13): its daemon runs outside the sandbox and the
  capacity budget ([ADR-0009](docs/adr/0009-offline-sandboxed-inference-worker.md)).
- **Not built yet:** the scale test (TASK-7), timer-based split-lease renewal for very
large single-object inputs (TASK-11).
- **Hardening gaps (TASK-9):** no absolute per-batch wall-clock cap, no server cap on
  `records_per_subtask` x `max_tokens`, and macOS ACLs are not checked on the state dir or
  `executor.toml` (mode bits only).
- **Ollama completions** silently truncate long prompts instead of returning `PROMPT_TOO_LONG` (TASK-12).
- **Operations gaps:** no backup guidance, no code-signed executor build, the compose stack is
  dev only. The web UI has no SSO (put an OIDC proxy in front).

Open items live in `backlog/tasks/`.

## License and citation

The repository does not contain a LICENSE file yet, so no license is granted by default; ask the
maintainer before reusing the code. This is an independent implementation of the paper:

> Li, Xing, De Silva, Flint, Yuan, Suchter. *Minions: A Dynamic and Distributed Virtual GPU Cloud
> Built on Idle Apple Silicon Devices.* arXiv:2610.12970, 2026.
