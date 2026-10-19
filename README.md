# open-minions

Open-source implementation of *Minions: A Dynamic and Distributed Virtual GPU Cloud Built on Idle
Apple Silicon Devices* (Li et al., [arXiv:2610.12970](https://arxiv.org/abs/2610.12970)).

- Idle Apple Silicon Macs run an **executor** that *pulls* batch inference jobs (embeddings, LLM
  completions) from a central **server**, runs them on the local GPU with
  [MLX](https://github.com/ml-explore/mlx) and writes the results to S3.
- The Mac's owner sets a **capacity budget** and the executor yields to them: it pauses under memory
  pressure, thermal stress or low battery, and never keeps the Mac awake.
- There is no central scheduler: coordination is PostgreSQL behind a REST API, and a **watchdog**
  takes work back from executors that go silent.

```
 you: minion CLI / web UI --> server (FastAPI) <--> PostgreSQL        S3 (inputs, results)
                                  ^                                       ^
                                  | REST + executor token (pull)          | presigned URLs
                                  +------------ executor on a Mac -------+
                                  watchdog: requeues work from silent executors
```

**Status.** Milestones M0 to M6 (scale harness and menu-bar app included) are implemented and tested; the GPU duty guard (the executor measures the worker's GPU use itself) is in; read [Known limitations](#known-limitations)
before running it on people's work laptops.

## Requirements

| What | Why |
|---|---|
| An Apple Silicon Mac (M1 or later) | the executor and MLX need it (the server side also runs on Linux) |
| [`uv`](https://docs.astral.sh/uv/) | installs Python 3.12 and all packages |
| Docker with Compose v2 | PostgreSQL, S3 (SeaweedFS), Grafana, server, watchdog |
| A few GB of free disk and network | images, plus a small model download on first use |

## Quick start: everything on one Mac (about 15 minutes)

You will start the server stack in Docker, run an executor natively on your Mac with a real MLX
model, submit two small jobs and look at the results. Run the commands from the repository root, in
**one terminal** (the `export`s matter), unless a step says otherwise.

**Commands.** `minion` and `minion-executor` below mean `uv run minion ...` / `uv run minion-executor ...` from the checkout, or the bare commands after `uv tool install` (see the service section).

**Ports.** The stack listens on `127.0.0.1` ports `8080` (server), `5432` (PostgreSQL), `9000` (S3)
and `3000` (Grafana). If one is taken, set `MINION_SERVER_HOST_PORT`, `MINION_PG_PORT`,
`MINION_S3_PORT` or `MINION_GRAFANA_PORT` before step 3 and use that port wherever this guide says
`8080`, `9000` or `3000`. To keep this stack apart from other open-minions containers and volumes,
add `-p some-name` right after `docker compose` in every command.

### 1. Install

```bash
git clone https://github.com/Robbertvh89/open-minions.git && cd open-minions
uv sync --all-packages --extra mlx --extra hf
```

`--all-packages` installs the server, CLI and admin tools next to the executor; `--extra mlx` is the
default inference engine and `--extra hf` the fallback engine. Plain `uv sync` leaves both engines
out, and `uv sync --package minion-executor ...` removes the other packages, so use the line above.

### 2. Create an API key (and the web UI secret)

The API key is what *you* use to submit work. It is printed once; the keys file keeps only its hash.

```bash
mkdir -p deploy/secrets && chmod 700 deploy/secrets
out=$(uv run minion-server-admin generate-api-key --name me --teams demo)
printf '%s\n' "$out" | sed -n 's/^api key (shown once): //p'          # show the key once
printf '%s\n' "$out" | sed -n '/^keys:/,$p' > deploy/secrets/api-keys.yaml
chmod 600 deploy/secrets/api-keys.yaml
export "$(uv run minion-server-admin generate-ui-secret)"    # optional: turns on the web UI at /ui
security add-generic-password -s open-minions-api-key -a "$USER" -w   # paste the API key when asked
```

The last command stores the key in the Keychain item `open-minions-api-key`, which `minion` reads by
itself, also after you close the terminal. To read it back (for the web UI login in step 8):
`security find-generic-password -s open-minions-api-key -w`.

### 3. Start the stack

```bash
docker compose -f deploy/compose.yml --profile app up -d --build --wait
curl -sf http://localhost:8080/healthz        # {"status":"ok"}
```

The first run builds the server image (a few minutes). This starts PostgreSQL, S3, Grafana, a
one-shot database migration, the server and the watchdog. The server only accepts the models in
[`deploy/models.yaml`](deploy/models.yaml): `bge-small-en` (embeddings) and
`qwen2.5-0.5b-instruct` (completions), both on MLX, plus `-hf` variants for the fallback engine.

### 4. Issue an executor token and put it in the Keychain

The executor has its own credential, separate from your API key. It is printed once.

```bash
docker compose -f deploy/compose.yml --profile app exec -T server \
  minion-server-admin issue-executor-token --owner me
security add-generic-password -s open-minions-executor -a "$USER" -w
```

The second command asks for the password: paste only the value after `token (shown once): ` from the
first command. The executor reads the Keychain item `open-minions-executor` by itself.

### 5. Run the executor

Open a **second terminal** in the repository and start it:

```bash
uv run minion-executor run --server http://localhost:8080 --profile light
```

It probes its engines in the sandbox (`engines: mlx,hf`) and then waits for work. Leave it running.
Models download on first use, so the first job is slow.

> **Nothing happens? Run `uv run minion-executor status` first.** On battery power, at a macOS
> memory-pressure warning, at a serious thermal state or in Low Power Mode the executor pauses by
> design, and `status` shows `paused (<reason>)`. For the quick start, plug in the charger and quit
> heavy apps. See [Troubleshooting](#troubleshooting).

### 6. Submit a job

Back in the first terminal. The CLI needs the server, your team and the S3 location (these are the
local stack's defaults; a real deployment gives you scoped credentials):

```bash
export MINION_CLI_SERVER=http://localhost:8080 MINION_CLI_TEAM=demo
export MINION_CLI_S3_ENDPOINT_URL=http://localhost:9000 MINION_CLI_S3_BUCKET=minion
export MINION_CLI_S3_ACCESS_KEY=minion MINION_CLI_S3_SECRET_KEY=minion-secret

uv run minion submit --type embedding --model bge-small-en examples/embeddings.jsonl -o out-embed --name my-embeddings
uv run minion submit --type completion --model qwen2.5-0.5b-instruct examples/completions.jsonl -o out-complete --name my-completions
```

`submit` uploads the file to S3, creates the task, waits for it and downloads the results into
`-o DIR`. The samples are [`examples/embeddings.jsonl`](examples/embeddings.jsonl) (5 sentences) and
[`examples/completions.jsonl`](examples/completions.jsonl) (3 prompts).

### 7. Look at the results

```bash
ls -a out-embed                       # results.jsonl  manifest.json  parts/  .minion.lock
head -c 200 out-embed/results.jsonl   # {"id":"e1","embedding":[-0.10,...
cat out-complete/results.jsonl
uv run minion status my-embeddings    # SUCCEEDED, subtasks 1/1 done
```

Every result line has the record `id`, the `embedding` or `completion`, and the `model` and
`model_revision` that produced it.

### 8. Web UI and Grafana

- **Web UI:** <http://localhost:8080/ui>. Log in with your API key (read it back with the `security find-generic-password` command from step 2). You see your tasks and
  subtasks and can create and cancel tasks. A 404 means the UI secret was not set when the server
  started: export it as in step 2 and run step 3 again.
- **Grafana:** <http://localhost:3000>, user `admin`, password `admin` (dev default). The dashboards
  `open-minions: tasks` and `open-minions: fleet` show task progress and executor activity.

### 9. Stop

Press Ctrl-C in the executor terminal; it finishes or releases the current subtask first. Then:

```bash
docker compose -f deploy/compose.yml --profile app down        # stop; keeps database, S3 data, Grafana
docker compose -f deploy/compose.yml --profile app down -v     # stop and DELETE that data
rm -rf out-embed out-complete deploy/secrets
rm -rf ~/Library/Caches/open-minions/models "$HOME/Library/Application Support/open-minions"   # optional: model cache and executor state
security delete-generic-password -s open-minions-api-key       # remove the API key too
security delete-generic-password -s open-minions-executor      # remove the token from the Keychain
```

## Day to day as a Mac owner

You run the executor on your own Mac and keep working. This is how you stay in control.

### Run it as a background service

The service is a per-user launchd agent. launchd cannot read `~/Documents`, `~/Desktop`,
`~/Downloads` or iCloud Drive, so install a copy of the executor with `uv tool` instead of using the
repository's `.venv`. The token must be in the Keychain (quick start step 4). Stop a foreground
executor (Ctrl-C) first: only one executor may use the state directory.

```bash
uv tool install './packages/minion-executor[mlx]'
minion-executor service install --server http://localhost:8080 --dry-run   # print the plist, change nothing
minion-executor service install --server http://localhost:8080             # write the plist and load it
minion-executor service status
```

`--server` must be `https://...` for anything but localhost. Other options: `--profile`, `--engines`,
`--label`, `--no-load`. After you update the checkout, re-run `uv tool install --reinstall ...` and then `launchctl kickstart -k gui/$(id -u)/com.open-minions.executor`. The
log is `~/Library/Logs/open-minions/executor.log`. Remove the service with `minion-executor service
uninstall`. If `minion-executor` is not found after `uv tool install`, add `~/.local/bin` to your `PATH`.
Details: [`docs/ops/executor-install.md`](docs/ops/executor-install.md).

### Menu-bar app

```bash
minion-executor menubar                 # run it now: status, pause 30m/1h/2h/until resume, profile
minion-executor menubar install         # start it at every login (--dry-run previews the plist)
minion-executor menubar status
minion-executor menubar uninstall
```

### Pause, resume, profile, status

These talk to the running executor (foreground or service) through files in
`~/Library/Application Support/open-minions` (`$MINION_EXECUTOR_STATE_DIR`). Use the same state
directory as the executor you want to control.

```bash
minion-executor status                  # profile, budget, paused reason, current subtask, signals
minion-executor pause --for 1h          # no new work; the current subtask finishes or is released
minion-executor resume
minion-executor profile balanced        # light | balanced | full | custom | auto
```

| Profile | Memory | GPU duty cycle | Batch |
|---|---|---|---|
| `light` | 25 % of RAM | 30 % | 4 |
| `balanced` | 40 % | 50 % | 8 |
| `full` | 70 % | 100 % | 32 |

Memory use is capped at 75 % of RAM for every profile. Without `--profile` the executor is
**adaptive**: it starts on `light` and switches to `full` after 10 minutes of you being away on AC
power. `profile auto` returns to the run's `--profile` pin, or to adaptive mode if there is none. `status` exits 1 if no executor is running.

**GPU guard.** The profile's duty cycle is not left to the worker's good behaviour: the executor
itself measures how much GPU the worker process uses and stops a worker that stays above the
profile's duty plus 10 percentage points (for example 40 % at `light`). The subtask is released
for another attempt and the worker restarts after a back-off. `status.json` (and `status`) then
shows `gpu_violation` for 10 minutes, and `gpu_quarantined` lists an engine that was stopped 3
times within an hour and is not used until that hour has passed. The reason also reaches the server as
presence `paused_reason=gpu_violation`. If the GPU meter cannot be read at startup, `run` exits 2.
Honest workers stay well under the limit (worst 10 s window measured: 23 % at `light`, 46 % at
`balanced`, see [ADR-0006](docs/adr/0006-cooperative-capacity-governor.md) D17). The guard stops a
worker; it does not yet slow one down (SIGSTOP gating: TASK-15.1).

### What pauses it automatically

No new work starts, and the running subtask is released after 60 s (at once under critical memory
pressure), on: memory pressure, serious or critical thermal state, battery power (unless you allow it),
Low Power Mode, an unusable `control.json`, or unreadable signals. SIGTERM, Ctrl-C and `service
uninstall` stop it cleanly: the running subtask finishes within 30 s or is released.

### `executor.toml`

Optional settings file at `~/.config/minion/executor.toml` (or `$MINION_EXECUTOR_CONFIG`), owner-only
permissions (`chmod 600`). It is read at start, so restart the executor after editing. Unknown keys
are an error.

```toml
active_profile = "light"        # profile while you are around
idle_profile = "full"           # profile after idle_after_min minutes away on AC power
idle_after_min = 10
allow_on_battery = false        # true: run on battery too, pausing below min_battery_percent
min_battery_percent = 30
on_call = "auto"                # auto | light | pause | ignore: back off when mic/camera is in use
```

`[profiles.<name>]` overrides and `[[schedule]]` time windows are described in
[SPEC §4.6](docs/SPEC.md). To favour a team's work: `minion-executor prefer --server URL --team T`
(a hint, not a filter; `--clear` removes it).

### Privacy and safety

- Inference runs in a worker subprocess inside a macOS Seatbelt sandbox: no network, no process exec,
  and no access to your home directory except the model cache and the Python install. Models are fetched by a separate pinned, checksum-verified
  process into `~/Library/Caches/open-minions/models` (capped at 50 GB, least recently used first).
- The executor downloads one input slice per subtask, runs it, uploads the result and deletes the
  local files. It never logs payload contents.
- The server learns your profile, memory budget and pause reason (so, roughly, when you are idle or
  on a call). It never learns keyboard activity or mic/camera state. With `on_call = "pause"` it sees
  `paused_reason=in_call`, and adaptive profile switching reveals when you are away. To reveal
  less, pin a profile (`--profile`) and set `on_call = "ignore"`. See [SPEC S-11](docs/SPEC.md).
- Only join a pool that your operator and organisation agreed on: submitters' data is processed on
  your Mac.

## Submitting work

You send JSONL in and get JSONL out. You need a server URL, a team, an API key and S3 access scoped
to your team (the quick start shows the local values).

### Input format

One JSON object per line. Embeddings: `{"id": "a1", "text": "..."}`. Completions:
`{"id": "c1", "prompt": "..."}` or `{"id": "c2", "messages": [{"role": "user", "content": "..."}]}`.
A record that cannot be processed (for example a prompt that is too long) comes back as
`{"id": ..., "error": "PROMPT_TOO_LONG", ...}` and does not fail the rest.

### Command line

```bash
uv run minion submit --type embedding|completion --model <id> FILE... -o DIR --name NAME
uv run minion status NAME          # status and subtask counts
uv run minion fetch NAME -o DIR    # (re-)download results; finished parts are skipped
uv run minion cancel NAME          # the only way to cancel; Ctrl-C of submit never cancels
```

Useful `submit` options: `--no-wait` (return after submitting, then `status` and `fetch --wait`),
`--timeout S`, `--project P`, `--params '{"max_tokens": 64, "temperature": 0.2}'` (completion:
`temperature`, `top_p`, `max_tokens`, `seed`, `system`; embedding: `normalize`),
`--records-per-subtask N` (default 500), `--priority N`, `--pool P`. Several `FILE`s make one task,
joined in order. `uv run minion submit --help` lists everything.

Configuration is environment only:

| Variable | Meaning |
|---|---|
| `MINION_API_KEY` | your API key; or the Keychain item `open-minions-api-key` (`security add-generic-password -s open-minions-api-key -a "$USER" -w`) |
| `MINION_CLI_SERVER` | server URL (`https`, or `http` to localhost) |
| `MINION_CLI_TEAM`, `MINION_CLI_PROJECT` | team (must be one of your key's teams) and project (default `default`) |
| `MINION_CLI_S3_ENDPOINT_URL`, `_BUCKET`, `_REGION` | S3 location; endpoint unset = AWS |
| `MINION_CLI_S3_ACCESS_KEY`, `_SECRET_KEY` | your team-scoped S3 credentials; unset = the boto3 default chain |

Exit codes of `minion`:

| Code | Meaning |
|---|---|
| 0 | `SUCCEEDED` |
| 1 | `FAILED` or `CANCELLED` |
| 2 | usage, config, auth or other 4xx error |
| 3 | `PARTIAL`: some subtasks failed; `fetch` gets what succeeded |
| 4 | timeout, or server or S3 unreachable |
| 130 | Ctrl-C (the task keeps running) |

### Web UI

`http://<server>/ui` (when the operator enabled it): log in with your API key, list and inspect your
team's tasks, create a task, cancel one. For big files use the CLI, which uploads straight to S3.

### REST API

JSON under `/api/v1` with `Authorization: Bearer <api key>`: `POST /tasks`, `GET /tasks`,
`GET /tasks/{team}/{project}/{name}`, `.../subtasks`, `POST .../cancel`. Payloads never go through
the API: you upload to S3 first and pass prefixes. See [SPEC §6](docs/SPEC.md).

## Troubleshooting

**`submit` waits and nothing runs.** Run `minion-executor status`. If it says `paused (...)`:

| Reason | What to do |
|---|---|
| `on_battery`, `low_battery` | plug in, or set `allow_on_battery = true` in `executor.toml` and restart the executor |
| `memory_pressure` | macOS reports a memory warning (`sysctl kern.memorystatus_vm_pressure_level`: 1 normal, 2 warning, 4 critical). Quit heavy apps; it resumes by itself |
| `thermal` | the Mac is hot; it resumes when it cools down |
| `low_power_mode` | turn Low Power Mode off |
| `user_paused` | `minion-executor resume` |
| `in_call` | the Mac thinks a mic or camera is in use (`on_call`); set `on_call = "ignore"` or `"light"` in `executor.toml` |
| `gpu_violation` | the worker used more GPU than the profile allows and was stopped (`GPU_DUTY_VIOLATION` release). It retries after a back-off; if it recurs, check the executor log and report the model/engine |
| `control_invalid`, `signals_unavailable` | `control.json` or the macOS signals cannot be read; see the log |

**GPU guard messages.** `GPU_DUTY_VIOLATION` (subtask released): the worker exceeded the profile's
duty plus 10 pp; the next acquire waits 10 s, doubling per repeat. `status` showing
`gpu_quarantined: [engine]`: that engine was stopped 3 times within an hour and is skipped for the
rest of the hour (with no other engine the executor idles; restarting does not clear it, switching
`--engines` may help). `GPU_METER_UNAVAILABLE` (subtask released): the GPU meter failed three reads in
a row, so the worker was stopped rather than trusted. `gpu_meter_probe_failed` and exit 2 at start:
the meter exists but its first read failed; the executor does not start without it. See the log
line for the error type.

If `status` exits 1 (nothing running), the executor is not started; check its terminal or
`~/Library/Logs/open-minions/executor.log`.

**Executor exit codes.** `minion-executor run` exits **2** on a configuration error and does not
retry: a missing token (put it in the Keychain, or export `MINION_EXECUTOR_TOKEN`), an `http://`
server URL that is not localhost (use `https://`), another executor already using the state
directory (`executor.lock`), no usable engine (`--engines auto` found neither mlx nor hf: redo
step 1: `uv sync --all-packages --extra mlx`), or a failed sandbox self-test. It exits **1** if its capacity governor
died. `MINION_EXECUTOR_DEV=1` and `MINION_EXECUTOR_SANDBOX=0` are for development only: never set
them on a machine that runs real tasks.

**`error: server answered 401 UNAUTHORIZED` (executor exits 1).** The token is wrong, expired or
revoked (as a service it is logged as `permanent_failure` and not retried). Issue a new one (step 4),
replace the Keychain item with `security add-generic-password -U -s open-minions-executor -a
"$USER" -w`, and start the executor (service: `launchctl kickstart gui/$(id -u)/<label>`).

**`error: worker timed out` at start.** The sandboxed engine probe has 90 s. On a Mac that is
swapping heavily (check `sysctl vm.swapusage`), importing MLX can take longer; quit memory-hungry
apps and start again, or probe only one engine with `--engines mlx`.

**`401` or `403` from `minion`.** Wrong or expired API key, or `MINION_CLI_TEAM` is not one of your
key's teams. A key you add to the keys file works within 5 s.

**`429` and `503` (`SERVER_BUSY`, `TOO_MANY_ACQUIREMENTS`, `ACQUIRE_DEADLINE`).** The server is
protecting itself. The executor waits for the `Retry-After` and retries by itself, so this only needs
action if it never ends. `minion` reports an unreachable server as exit 4.

**The server container keeps restarting (Linux).** The server runs as uid 10001 and refuses a keys
file owned by someone else: `sudo chown -R 10001:10001 deploy/secrets` (not tested here). macOS
does not need this.

**The first job is very slow.** The model is downloading from the Hugging Face hub; it is cached
afterwards.

**A subtask failed or the worker crashed.** The control loop survives a worker crash (out of memory,
Metal error) and releases the subtask so another attempt can run. After repeated failures the task
ends as `FAILED` or `PARTIAL`. See `minion status NAME` and the executor log.

### Day-2 operations

- **Logs:** `docker compose -f deploy/compose.yml --profile app logs -f server watchdog`; executor:
  `~/Library/Logs/open-minions/executor.log` (service) or its terminal.
- **API keys:** `uv run minion-server-admin generate-api-key --name <who> --teams a,b
  [--operator] [--expires-days N]`; add the entry to the keys file (live-reloaded within 5 s); delete
  the entry to revoke.
- **Executor tokens** (through `docker compose ... exec -T server` when the server runs in Docker):
  `minion-server-admin issue-executor-token --owner NAME [--pool P] [--ttl-days N]`,
  `list-executors`, `rotate-executor-token --executor-id UUID`,
  `revoke-executor-token --executor-id UUID`. A revoked token is refused at once for acquire, register, presence and heartbeat, and within 10 s
  (`MINION_EXECUTOR_AUTH_CACHE_TTL_S`) for complete, fail and release.
- **Health:** `curl http://localhost:8080/healthz`. The watchdog requeues subtasks whose executor
  stopped sending heartbeats (after 900 s by default).
- **Update:** `git pull`, `uv sync --all-packages --extra mlx --extra hf`, then
  `docker compose -f deploy/compose.yml --profile app up -d --build --wait` (migrations run
  automatically), and `uv tool install --reinstall './packages/minion-executor[mlx]'` for the service.
- **Backups:** not covered. Back up the PostgreSQL volume and the S3 bucket yourself.
- **Benchmark your Mac:** `uv run minion-executor bench --models deploy/bench-models.yaml --model
  qwen2.5-0.5b-mlx` (see `bench --help`; M3 Pro results in [`docs/bench/`](docs/bench/)).

## Multi-Mac and team setup (preview)

Running one server for several Macs and people (TLS reverse proxy, managed PostgreSQL and S3,
per-team S3 credentials, pools for confidential data, an install guide for colleagues) is **not
documented as a guide yet**: it is tracked as TASK-37 (colleague pilot, on hold). The compose stack in
`deploy/compose.yml` is a development stack (default passwords, no TLS) and must not be exposed as
is. Until the guide exists, start from [ARCHITECTURE §6-7](docs/ARCHITECTURE.md), the threat model in
[SPEC §8](docs/SPEC.md) and [`docs/ops/executor-install.md`](docs/ops/executor-install.md).

## Documentation

- [`docs/SPEC.md`](docs/SPEC.md): what the system must do (requirements, API, data model, threat model)
- [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md): components, flows, decisions
- [`docs/adr/`](docs/adr/): architecture decision records (MLX default engine, capacity governor,
  sandboxed worker, launchd agent, ...)
- [`docs/ops/executor-install.md`](docs/ops/executor-install.md): executor service install notes
- [`docs/bench/`](docs/bench/): measurements on real hardware
- [`.env.example`](.env.example): every `MINION_*` setting

## Development

```
packages/
  minion-common/     shared Pydantic schemas, enums, typed API client
  minion-server/     FastAPI app: task API, executor API, splitter, repositories, web UI, admin CLI
  minion-executor/   macOS agent: governor, worker subprocess, runners (mlx, hf, ollama, fake), CLI
  minion-watchdog/   heartbeat monitor; requeues orphaned subtasks
  minion-cli/        `minion` submitter CLI
  minion-scale/      dev-only scale harness
migrations/          Alembic
deploy/              docker-compose, Dockerfile, models, Grafana, launchd
docs/                SPEC, ARCHITECTURE, ADRs, ops, bench, usability
```

```bash
uv sync --all-packages
cp .env.example .env
docker compose -f deploy/compose.yml up -d         # postgres + s3 + grafana only
uv run alembic upgrade head
uv run minion-server                               # native server on :8080
uv run pytest -m "not integration"                 # fast unit tests
uv run pytest                                      # all tests; integration needs Docker (testcontainers)
uv run ruff check . && uv run ruff format --check . && uv run mypy packages migrations conftest.py
```

To try the system without a model or GPU, use the dev override (adds the models `fake-embed` and
`fake-complete`; never in production) and the `fake` engine:

```bash
docker compose -f deploy/compose.yml -f deploy/compose.dev.yml --profile app up -d --build --wait
MINION_EXECUTOR_DEV=1 uv run minion-executor run --server http://localhost:8080 --engines fake
uv run minion submit --type embedding --model fake-embed examples/embeddings.jsonl -o out-fake --name fake-run
```

Test markers: `integration` (Docker: Postgres and S3 via testcontainers, including the acquisition
race), `macos` (Seatbelt sandbox tests), `gpu` (real MLX models), `model` (downloads a tiny real
model), `ollama` (needs a live daemon).

Invariants you must not break (details in [CLAUDE.md](CLAUDE.md)): executors never talk to
PostgreSQL; ownership is decided only by the `INSERT ... ON CONFLICT DO NOTHING` into
`minion_subtask_acquirement`; only the current owner may mutate a subtask (stale `acquirement_id` is
`409`); large payloads bypass the server; execution is idempotent per `subtask_id` + `attempt`; the
executor yields to the user; inference runs in a worker subprocess.

Workflow: every change starts with a [Backlog.md](backlog/) task, one branch and PR per change,
Conventional Commits, as described in CLAUDE.md and `.claude/skills/feature-workflow`. Decisions that
deviate from the paper get an ADR in `docs/adr/`.

## Known limitations

- **NF-7 (no visible harm during video calls) is not met everywhere.** The synthetic foreground
  proxy ([`docs/bench/m5-foreground-apple-m3-pro.md`](docs/bench/m5-foreground-apple-m3-pro.md)) fails
  for the `minilm` embedding model at `light` (+1.11 percentage points of missed frames) and
  `balanced` (+1.56 pp); the completion model passes. `minilm` is a bench-only model; the served
  embedding model `bge-small-en` was not measured. Tracked in TASK-24.
- **The human NF-7 study has not been run.** [`docs/usability/nf7-video-call-protocol.md`](docs/usability/nf7-video-call-protocol.md)
  needs at least 8 people on their own Macs. Until then, do not claim calls are unaffected.
- **Real microphone/camera call detection is not verified on hardware** (`on_call`); it is covered by
  tests with injected signals only. Screen sharing is not detected.
- **GPU duty cycle:** the control process measures the worker's GPU use and kills a worker above
  duty + 10 pp, but it cannot slow one down first; SIGSTOP gating is open (TASK-15.1). A rogue worker
  can use up to about 49 s of excess before it is stopped. Measured on one Mac (Apple M3 Pro); other
  chips are untested, and engines that run outside the worker process (Ollama, the dev fake) are not
  metered.
- **Output size is capped by the S3 policy, not the quota.** Use a bucket quota and lifecycle rules
  (SPEC S-2, [ADR-0017](docs/adr/0017-presigned-post-output-uploads.md)).
- **Signed `Range` reads are not verified on SeaweedFS** (the dev S3); AWS S3 and MinIO enforce them.
- **Server logs are plain, not JSON** (the watchdog's are structured; TASK-17).
- **Ollama is opt-in and not benchmarked** (TASK-13): its daemon runs outside the sandbox and the
  capacity budget ([ADR-0009](docs/adr/0009-offline-sandboxed-inference-worker.md)). Ollama
  completions silently truncate long prompts (TASK-12).
- **Operations gaps:** no backup guidance, no code-signed executor build, the compose stack is dev
  only, the web UI has no SSO (put an OIDC proxy in front).

Open items live in `backlog/tasks/`.

## License and citation

The repository does not contain a LICENSE file yet, so no license is granted by default; ask the
maintainer before reusing the code. This is an independent implementation of the paper:

> Li, Xing, De Silva, Flint, Yuan, Suchter. *Minions: A Dynamic and Distributed Virtual GPU Cloud
> Built on Idle Apple Silicon Devices.* arXiv:2610.12970, 2026.
