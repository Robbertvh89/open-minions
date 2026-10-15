# ADR-0009: Offline, sandboxed inference worker with a pinned, verified model fetch

- Status: **Proposed** (implemented in TASK-4.2; Accepted at review)
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

**D7. Credentials reach only the fetch process; the worker is offline.**
`HF_TOKEN`, proxy URLs, CA settings and `HF_ENDPOINT` are captured before `restrict_environ()` and
handed only to the fetch subprocess as an explicit env dict. The worker keeps its env allowlist and
additionally gets `HF_HUB_OFFLINE=1`, `TRANSFORMERS_OFFLINE=1`, `HF_DATASETS_OFFLINE=1`,
`HF_HUB_DISABLE_TELEMETRY=1`. Runners load from the local snapshot path with `trust_remote_code=False`.

**D8. Seatbelt sandbox on the worker.**
Before any engine import or model load, the worker calls `sandbox_init` (libsandbox, via ctypes) with
a profile built by the pure function `worker/sandbox.build_profile`:

- `(allow default)` as the base, then in this order (last match wins):
- `(deny network*)`, plus `(allow network-outbound (remote tcp "localhost:<port>"))` only when the
  Ollama engine is requested;
- `(deny process-exec*)`: the worker cannot run `security`, `curl`, `osascript` or anything else;
- `(deny mach-lookup ...)` for `com.apple.SecurityServer`, `com.apple.securityd.xpc`, `com.apple.secd`
  and the other Keychain services;
- `(deny file-read* file-write* (subpath HOME))` for the passwd home and `$HOME`;
- `file-read*` re-allowed for the model dir, the cache root, Python prefixes, site-packages and the
  `sys.path` entries, and `file-read-metadata` (only) on their ancestors under HOME so path walks work;
- `(deny file-write* ...)` on every read-only path, so the cache is read-only for the worker;
- read-write on one per-spawn tmp dir, created by the control side (`TMPDIR` points to it) and removed
  when the worker is reset.

Paths are symlink-resolved and escaped (`\` and `"`); control characters are rejected. `apply()` raises
`SANDBOX_FAILED` on any error and never continues unsandboxed (fail closed). The startup probe runs in
a sandboxed worker, so a broken sandbox makes `minion-executor run` exit 2. A sandboxed worker's grants
are fixed, so each load gets a fresh worker. `MINION_EXECUTOR_SANDBOX=0` is accepted only together with
`MINION_EXECUTOR_DEV=1`. Off macOS the sandbox is a no-op (only the fake engine runs there).

## Consequences

- A compromised worker cannot read HOME secrets, reach the Keychain, exec tools, open outside
  sockets or modify the cache. It can still use CPU/GPU within the budget and read non-HOME files
  that `allow default` leaves readable (for example `/etc`, other users' world-readable files).
- **Residual risk.** The base is allow-default, not deny-default, because a deny-default profile for
  Metal/IOKit/dyld cannot be validated for every Mac model and engine. Writes outside HOME and the
  tmp dir are not blocked. Tightening to deny-default is a follow-up once `gpu` e2e tests run through
  the sandbox on several chips.
- **`sandbox_init` is deprecated** by Apple. It is still shipped and is what Chromium, Firefox and
  Bazel use for self-sandboxing. The use is isolated in `worker/sandbox.py`; if it disappears, the
  probe fails closed and the executor refuses to start rather than run unconfined.
- Model download needs the network on the control side; the worker never needs it.
- The `macos`-marked tests exercise the real kernel sandbox with the fake engine. They do not import
  MLX/Metal (mlx is not a CI dependency); the MLX e2e through the sandbox is TASK-4.3's `gpu` test.

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
