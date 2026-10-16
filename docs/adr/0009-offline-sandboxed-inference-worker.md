# ADR-0009: Offline, sandboxed inference worker with a pinned, verified model fetch

- Status: **Accepted** (implemented in TASK-4.2; profile hardened after the security audit)
- Date: 2026-10-16
- Deciders: project owner
- Deviates from paper: **yes**. The paper has no sandbox and lets the inference process load models
  however its engine does. It says nothing about model provenance.

## Context

Executors run on personal work laptops (SPEC S-1). The inference worker loads third-party model files
and runs engine code (MLX, transformers, tokenizers) over untrusted-ish input. A malicious or
compromised model repo, a deserialisation bug in a loader, or a prompt that reaches a parser bug could
give an attacker code execution inside the worker. In an unconfined worker that means:

1. HOME is readable: `~/.aws`, `~/.ssh`, `~/.config/minion`, browser profiles.
2. The Keychain is reachable (`security` CLI, securityd), including the executor token.
3. Any outbound socket works, so data and credentials can leave the Mac.
4. The shared model cache can be modified, which poisons later runs of other models.
5. The Hugging Face token and any credential-bearing proxy URL in the environment are readable.

## Decision

**D5. Fetch in a separate, killable subprocess; verify; pin.**
The control side downloads models in a short-lived `spawn` *fetch subprocess* (`model_cache.py`), not
in the worker. It calls `snapshot_download(repo, revision=<40-hex sha>, allow_patterns=SAFE)` where
`SAFE` is safetensors, JSON configs, tokenizer files and chat templates. It never includes `*.py`,
`*.bin`, `*.pt`, `*.pkl` or `*.gguf`, and a `config.json` with `auto_map` is refused
(`MODEL_UNSUPPORTED`). Every LFS file's sha256 and size are checked against hub metadata; a manifest
is written; the staging directory is renamed atomically into `<root>/hf/<org>--<name>/<sha>/`.
The cache is LRU-bounded by `MINION_EXECUTOR_MODEL_CACHE_GB`. A stop or cancel kills the fetch.
The fetch subprocess is itself confined with the same Seatbelt helpers (network allowed; writes only
to its staging dir, where `HF_HOME`, `HF_XET_CACHE` and `TMPDIR` also live; HOME reads, exec,
LaunchServices and AppleEvents denied; Mach lookups limited to the worker list plus
`com.apple.trustd.agent`, which `truststore` needs for TLS; a CA bundle under HOME is re-allowed).
It fails closed; `MINION_EXECUTOR_FETCH_SANDBOX=0` exists for development only. Non-LFS files are
checked against the hub's git blob sha1, and a download must leave `max(MIN_FREE_DISK_GB, 10%)` free.

**D7. Credentials reach only the fetch process; the worker is offline.**
`HF_TOKEN`, proxy URLs, CA settings and `HF_ENDPOINT` are captured before `restrict_environ()` and
handed only to the fetch subprocess as an explicit env dict. The worker keeps its env allowlist and
additionally gets `HF_HUB_OFFLINE=1`, `TRANSFORMERS_OFFLINE=1`, `HF_DATASETS_OFFLINE=1`,
`HF_HUB_DISABLE_TELEMETRY=1`. Runners load from the local snapshot path with `trust_remote_code=False`.

**D8. Seatbelt sandbox on the worker.**
Before any engine import or model load, the worker calls `sandbox_init` (libsandbox, via ctypes) with
a profile built by the pure function `worker/sandbox.build_profile`. The base is `(allow default)`
(Metal, IOKit and dyld cannot be enumerated for every Mac), then whole classes are denied and the
minimum is re-allowed (last match wins, so order matters):

- **Network.** `(deny network*)`; `(allow network-outbound (remote tcp "localhost:<port>"))` only in
  the spec of an **Ollama load**, with the port taken from the same `EngineConfig` the handle passes to
  the runner. Probe, MLX, HF and fake workers get no network at all.
- **Exec.** `(deny process-exec*)`.
- **Escape hatches.** `(deny lsopen)` (LaunchServices: `open`, URL handlers),
  `(deny appleevent-send)`, `(deny distributed-notification-post)`,
  `(deny user-preference-read user-preference-write)` (so `cfprefsd` cannot be used to read
  `~/Library/Preferences` although the path is denied), and `(deny signal)` +
  `(allow signal (target self))`. `(deny signal (target others))` is accepted by the parser but does
  not deny anything on macOS 27 (verified: `SIGCONT` to the parent succeeded), so deny-all plus
  allow-self is used.
- **Mach.** `(deny mach-lookup)`, then allow only `com.apple.MTLCompilerService`,
  `com.apple.cvmsServ`, `com.apple.gpumemd.source`, `com.apple.logd`,
  `com.apple.system.notification_center`, `com.apple.system.opendirectoryd.libinfo`. Ablation on
  macOS 27 with mlx 0.32 (matmul, `mx.compile`, a custom Metal kernel) showed MLX runs with an empty
  list; these are kept because they are Metal/logging/libinfo daemons that expose no user data and
  other macOS releases or mlx-lm/transformers may use them. Denied (and tested): launchservicesd,
  appleevents, pasteboard, nsurlsessiond, windowserver, mds, tccd, SecurityServer, secd, trustd.
- **Writes.** `(deny file-write* (subpath "/"))`, then allow the per-spawn tmp dir (created by the
  control side as `minion-exec-<control pid>-worker-*` so `cleanup_stale_tmp` sweeps it; `TMPDIR`
  points to it), `/dev/null`, `/dev/dtracehelper`, `/dev/tty`, `/dev/zero`, and Metal's shader cache
  `<DARWIN_USER_CACHE_DIR>/(org.python.python|python*)/com.apple.metal/**` (regex, not other apps').
- **Reads.** HOME (passwd and `$HOME`), `/Users`, `/Volumes`, `/private/tmp`, `/private/var/tmp`,
  `/private/var/folders` and `/Library` are denied, then re-allowed read-only: the **one model dir**
  of the current load (not the cache root), `sys.prefix`, `sys.base_prefix`, site-packages, the source
  roots named in `.pth` files (editable installs), and the tmp dir and Metal cache above. No cwd,
  `sys.path[0]` or `PYTHONPATH` entry is granted; the worker moves to its tmp dir and prunes `sys.path`
  to granted roots before applying. `file-read-metadata` (only) is allowed on ancestors of the grants
  so path walks work. A model or cache dir that is HOME, an ancestor of HOME, or an ancestor of
  `~/Library/Keychains` is refused.
- **Self-test.** After `apply`, the worker asserts that listing HOME and creating a file in
  `/private/tmp` both fail; otherwise it exits immediately (exit code 78, seen by the control side as a
  crashed worker, so `run` exits 2 from the probe).

Paths are symlink-resolved and escaped (`\` and `"`); control characters are rejected. `apply()` raises
`SANDBOX_FAILED` on any error and never continues unsandboxed (fail closed). Off macOS only the fake
engine may run unsandboxed; every other engine fails closed with `SANDBOX_FAILED`. The startup probe
runs in a sandboxed worker, so a broken sandbox makes `minion-executor run` exit 2. A sandboxed worker's
grants are fixed, so each load gets a fresh worker. `MINION_EXECUTOR_SANDBOX=0` is accepted only
together with `MINION_EXECUTOR_DEV=1`.

## Consequences

- Network: the worker has none (`(deny network*)` covers TCP, UDP and unix sockets; verified with
  a real AF_UNIX listener). The fetch child, which needs the hub, gets `(deny network-outbound)`
  plus DNS via the mDNSResponder socket, TCP to `*:443` (and the `HF_ENDPOINT` port), and the
  configured proxy's port; loopback is denied unless the proxy itself is local. Seatbelt cannot
  filter hostnames, so any host on :443 is reachable from the fetch child.
- Other processes' launch environment is closed with all three rules together
  (`(deny process-info*)`, `(allow process-info* (target self))`,
  `(deny sysctl-read (sysctl-name-prefix "kern.procargs"))`); any one or two alone leave
  `KERN_PROCARGS2` readable. Self-test checks this and outbound TCP (worker).
- The fetch child's staging dir is polled and the child killed if it exceeds the hub's announced
  total plus a small margin. CA bundles are granted as file literals; `SSL_CERT_DIR` under HOME is
  refused.
- A compromised worker cannot read HOME secrets or other users' data under `/Users`, reach the
  Keychain, launch or script apps, read preferences or the pasteboard, signal other processes, exec
  tools, open outside sockets, or write anywhere except its tmp dir, a few `/dev` nodes and the Python
  Metal cache. It can still use CPU/GPU within the budget.
- **Residual risks (honest list).**
  - The base is still allow-default. Reads of the rest of the filesystem outside the denied roots
    (`/etc`, `/opt`, `/usr`, `/var` except `tmp` and `folders`, `/private/var/db`, `/Applications`) and
    most other operations (sysctl, IOKit, other Mach-free syscalls) remain possible. Nothing under
    those roots is a per-user secret by default, but a Mac with secrets in e.g. `/opt` or `/etc` is
    exposed.
  - Homebrew data dirs (`/opt/homebrew/{var,etc}`, `/usr/local/{var,etc}`) are denied, but other
    paths under `/opt` and `/usr/local` (e.g. `/opt/homebrew/Cellar` configs, `/usr/local/share`)
    remain readable.
  - The per-process Metal shader cache (`com.apple.metal`) is writable and shared by all Python
    workers of the user: a compromised worker could plant a poisoned shader cache entry for later
    workers. Accepted and documented only; not mitigated.
  - The fetch child reaches any host on port 443 (and the proxy), so a compromised fetch child can
    exfiltrate what it can read (its staging dir, the fetch env including `HF_TOKEN`).
  - With a proxy configured the fetch child may reach `*:<proxyport>` (any host on that port, as
    Seatbelt does not filter by hostname) and no longer `*:443`; `HTTP_PROXY` is not honoured for
    https fetches and opens nothing. Without a proxy it is `*:443` (plus the `HF_ENDPOINT` port). If `NO_PROXY`/`no_proxy` is set
    next to a proxy, direct access is legitimate, so `*:443` and the `HF_ENDPOINT` port are allowed
    in addition to the proxy port.
  - Both profiles deny `process-fork`, `network-bind` and `network-inbound`, so a compromised child
    cannot leave a grandchild serving a fake local service (e.g. Ollama on 11434) after
    `proc.kill()`. Fork denial is the control; there are no process groups or sessions (`killpg`
    on a reaped pid could hit an unrelated group after pid reuse). In the dev-only sandbox-off
    mode grandchildren are not reaped (accepted, dev only).
  - `needed` and the plan file sizes are child-reported, so a compromised fetch child could force
    eviction of the cache (DoS only, bounded by the free-disk reserve).
  - The localhost deny covers `localhost` only: the rest of 127/8 and the Mac's own LAN IP are not
    covered.
  - Process listing (`KERN_PROC_ALL`) stays possible, as do `AF_ROUTE` and `AF_SYSTEM` sockets.
  - Staging-size accounting walks the tree and may double count hard links/renamed files while a
    download is in flight (accepted, low risk; a free-disk-reserve check backs it up).
  - Allowed Mach services (above) and the `com.apple.metal` cache are shared system surface; a bug in
    Metal's compiler service is reachable from the worker.
  - A malicious model can still burn CPU/GPU up to the budget and can fill its tmp dir (disk quota is
    not enforced).
  - Seatbelt does not filter by hostname; the Ollama grant is a loopback port, so a hostile local
    service on that port is trusted.
  - **Ollama daemon outside the sandbox (confused deputy).** The Ollama runner only talks HTTP to a
    loopback daemon. `/api/pull` makes that unsandboxed daemon download weights from its own
    registry (into `~/.ollama`, outside the model-cache cap), and it then runs inference outside our
    Seatbelt profile and capacity budget. Therefore **Ollama is opt-in**: `--engines auto` probes
    mlx and hf only, and Ollama needs `--engines ollama` (or a list containing it). Mitigations: a
    `ModelEntry` ref must carry an explicit tag and must not name a registry host (a first path
    segment with `.` or `:`, `localhost`, or an IP is rejected), so pulls come from Ollama's default
    library; the model is pinned by manifest digest, which the runner checks against `/api/tags`
    after every load **and after every run** (TOCTOU: a swapped model must not get its outputs
    stamped). A mismatch is executor-local trouble (this Mac's store), so the subtask is released
    (`MODEL_INTEGRITY`, retryable, no output stamped) rather than failed. The pull timeout stays
    below the worker's load timeout, so a large pull is a release, never a killed worker. The
    runner refuses non-loopback URLs and never logs prompts or responses.
  - **Ollama deviations.** The daemon cannot return unnormalised embeddings: the server rejects
    `normalize=false` for ollama models with 422 `INVALID_PARAMS`, and the runner fails it
    non-retryably (`PARAMS_UNSUPPORTED`; no other engine is tried because the server already
    refused such tasks). Embedding `tokens` is `prompt_eval_count` per text (one `/api/embed` call
    each) and can under-report when the prompt cache holds a shared prefix. Over-long prompts are
    truncated by the daemon and not detectable (no `PROMPT_TOO_LONG` lines); `num_ctx` is the constant
    `max_prompt_tokens + 4096` (constant so the daemon never reloads for a changed context; the cost
    is KV cache for that whole window per request). Requests carry a bounded `keep_alive`. The
    worker unloads the model (`keep_alive: 0`) on idle unload and on a model switch, under a total
    wall-clock deadline that always ends in a kill (a hostile reply or tick flood cannot keep it
    alive). After a hard kill (stop, cancel, 409) the control side sends a best-effort
    `keep_alive: 0` (short timeout, `trust_env=False`); if that fails the daemon drops the weights
    when `keep_alive` expires (at most 300 s). A budget downgrade (M4) will reuse the same path.
    Load-phase 5xx and transport errors are releases (the daemon is this Mac's problem); a run-phase
    404 (model removed locally) is a release too.
  - MLX workers are spawned with `USE_TORCH=0`, so `transformers` (used only for tokenisation) never
    imports `torch`; MLX loads pass `trust_remote_code=False` and `local_files_only` explicitly.
  - The HF runner (`torch` 2.14 / `transformers` 5.20 on `mps`) runs under the same profile with
    **no extra grants**: weights load from the read-only model dir, and every cache variable
    torch/transformers might write (`TORCH_HOME`, `TORCHINDUCTOR_CACHE_DIR`, `XDG_CACHE_HOME`,
    `HF_MODULES_CACHE`, ...) is pointed into the worker tmp dir by `runners/hf.confine_caches`
    before `import torch`, with `torch.compile`/dynamo disabled. MPSGraph logs a harmless
    "Error creating directory" for its debug dumps; it also does so unsandboxed.
  - The profile is validated on macOS 27 / Apple Silicon with mlx 0.32 only; other macOS releases may
    need extra allows (the failure mode is `SANDBOX_FAILED`/engine error, never silent widening).
- **`sandbox_init` is deprecated** by Apple. It is still shipped and is what Chromium, Firefox and
  Bazel use for self-sandboxing. The use is isolated in `worker/sandbox.py`; if it disappears, the
  probe fails closed and the executor refuses to start rather than run unconfined.
- Model download needs the network on the control side; the worker never needs it.
- The `macos`-marked tests exercise the real kernel sandbox with the fake engine, and, when the
  optional `mlx` extra is installed (`uv sync --extra mlx`), a real MLX GPU matmul and `mx.compile`
  through the same profile. CI is Linux and skips them.

## Alternatives considered

- **Separate macOS user for the worker.** Strongest isolation, but needs admin/root at install time and
  a privileged helper; belongs to M5 packaging.
- **`sandbox-exec` wrapper.** Deprecated CLI as well; needs a custom `multiprocessing` executable and an
  on-disk profile script, and gives the child no way to report profile errors over the pipe.
- **App Sandbox entitlements.** Requires a signed app bundle; not applicable to a `uv tool` CLI.
- **Containers / VMs.** No GPU access to Metal from containers on macOS; Virtualization.framework adds
  large overhead and no Metal passthrough.
- **Download inside the worker with a network allowlist.** Seatbelt cannot filter by hostname, and the
  token would have to enter the worker. Rejected in favour of D5/D7.
- **Do nothing (paper behaviour).** Rejected: unacceptable on personal laptops.
