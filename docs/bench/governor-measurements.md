# Governor measurements on a real Mac (NF-4, NF-8, NF-9)

How the numbers in [ADR-0006](../adr/0006-cooperative-capacity-governor.md) "Measurements" are
produced. CI covers the logic with the fake engine; the GPU, `ioreg` and real signals need a Mac.
Prepare: on AC power, close heavy apps, `uv sync --package minion-executor --extra mlx --extra hf`,
models cached once (`bench` downloads them on first use, network). Nothing here needs a server.

Result files for the M3 Pro run: `docs/bench/m4-governor-apple-m3-pro.json` (budget check, from
`--out`) and `docs/bench/m4-governor-apple-m3-pro.md` (the tee'd table plus the NF-4/NF-9 notes).
Commit only derived figures (shares, `reaction_s` values, CPU %), never raw timestamped output
lines: they reveal when the owner was at the keyboard.

## NF-8: budget adherence (about 14 min for 3 profiles x 1 model)

```bash
uv run minion-executor bench --budget-check --models deploy/bench-models.yaml \
  --model qwen2.5-0.5b-mlx --profiles light,balanced,full --duration 60 --baseline-s 20 \
  --repeats 3 --out docs/bench/m4-governor-apple-m3-pro.json | tee docs/bench/m4-governor-apple-m3-pro.md
# embeddings too:  --model minilm-mlx
```

**Metric.** The worker's own GPU time. `ioreg -r -l -w 0 -c AGXDeviceUserClient` lists every
process's Metal user clients with `"IOUserClientCreator" = "pid N, name"` and
`"AppUsage" = ({"API"="Metal", ..., "accumulatedGPUTime"=<ns>}, ...)`. The delta of the worker pid's
summed `accumulatedGPUTime` over a phase window divided by the window's wall time is the share of the
GPU our process kept busy. No baseline is subtracted: other apps, the display and DVFS do not enter.
(The device-wide `Device Utilization %` is not a per-process share on a Mac in use: a first run saw
an idle device at 30-43 % and failed `light` at 32 % against 20 % expected.) Sampling is about 2 Hz
from the control process (one filtered `ioreg` call takes about 15 ms; the unfiltered `ioreg -l`
takes about 240 ms and 7 MB). A client that appears inside a window starts at 0; a worker with
several clients is summed.

Per (model, profile, repeat): the worker loads and runs one excluded warm-up batch, then `--baseline-s`
idle (device column only), `--duration` s unpaced (duty 1.0), a 2 s gap, `--duration` s paced at the
profile duty. The first 1.5 s of each phase are dropped from its window. `full` skips the unpaced
phase. Per profile about `warm-up + baseline + 2 x duration + 2 s` = 145 s plus model load (full:
about 85 s), times `--repeats`. The footprint sampler (10 Hz) and the GPU sampler start before the
model loads, so the footprint peak includes the load.

Columns: `unpaced` / `measured` (worker GPU time share), `expected` = duty x unpaced, `pacer-reported`
(worker `busy/wall`, advisory), `device idle/unpaced/paced` (device-wide utilization, informational
only, never judged), `footprint GB` (peak `phys_footprint`) vs `budget GB`, and pass/fail:

- hard bound: `measured <= duty + 10 pp`;
- accuracy: `|measured - duty x unpaced| <= 10 pp`, unpaced measured the same way; `-` for `full`
  (no unpaced phase) and it does not count against the row;
- memory: footprint peak <= 1.1 x budget.

Rows with status `unavailable` (engine missing, model larger than the profile's memory budget) are
excluded from pass/fail; an `error` row fails the check. Exit 1 if any row fails, 2 if `ioreg` is
unavailable. A CPU-bound workload (short embedding batches, tokenization) has `unpaced` well below
100 %; judge it by `expected`, not by the profile duty (ADR-0006 Gap 1). The batch size is the
profile's cap.

## NF-9: reaction time (< 5 s)

CI (`tests/test_governor_reaction.py`, about 15 s): 20 trials over thermal serious, memory warning,
memory critical (release), power (battery 5 %), user return, with `FileSignals`, a real
`GovernorRuntime`, the real `ExecutorLoop._watch` / `_push_control` and a fake-engine worker with
100 ms steps and a 0.25 s poll. Pause and duty are timed to the worker's ack, memory critical to the
worker process exiting. It reports p50 and max per event
(`uv run pytest packages/minion-executor/tests/test_governor_reaction.py -s`) and asserts max < 5 s
and max < poll + step + 1.0 s. `MINION_EXECUTOR_SIGNAL_POLL_S` is capped at 4 s so poll plus one
engine step stays under the bound.

On the Mac with real signals and the production poll (2 s), `--watch-reactions` runs a real
`GovernorRuntime` (your `executor.toml`, this Mac's memory) and prints one timestamped line (epoch
seconds) per decision change. It prints to stdout only (`--out` is rejected, exit 2):

```bash
uv run minion-executor bench --watch-reactions --duration 120 2>/dev/null
```

- **User return.** Let the Mac sit idle, or run it with `MINION_EXECUTOR_CONFIG` setting a short
  `idle_after_min`, then touch the keyboard. The line `user-return reaction_s=...` is back-dated from
  the HID idle counter (`taken_at - hid_idle_s`), so it needs no stopwatch. It is the time to the
  *observation*; the decision change line follows in the same poll.
- **Memory pressure.** Needs the operator's approval (it makes the Mac swap). In another terminal:
  `python3 -c 'import time; print(time.time())'; memory_pressure -l warn` (stop it with Ctrl-C when
  done), then subtract that epoch from the `decision paused=memory_pressure` line. Critical:
  `-l critical` shows `release_now=True`.
- **Thermal and power.** Not inducible safely. The real readers are unit-tested and live-read
  (`test_signals_macos.py`); the decision path is covered by the CI trials through `FileSignals`.
  To time the loop end to end, run `bench --watch-reactions` with `MINION_EXECUTOR_DEV=1
  MINION_EXECUTOR_FAKE_SIGNALS=/path/signals.json` and edit the file (see `FileSignals` for fields).

The worker leg (control message to ack) is measured by the CI test; a pause also waits for at most
one engine step, so a long prefill step adds to the figure (ADR-0006 risks).

## NF-4: idle and paused CPU (< 1 %)

```bash
uv run minion-executor bench --cpu-check --duration 60 --out cpu.json   # about 2 min
```

`--cpu-check` covers the **control process only**: a real `GovernorRuntime` (your `executor.toml`,
this Mac's memory) polling the real signals at the production poll, first idle, then owner-paused
(`control.json`), reporting this process's CPU time (all threads) over the window. It leaves out the
server round trips and the worker. For a whole `minion-executor run` against a server, sum the CPU of
the process tree (control process plus its worker child), idle and then after `minion-executor pause`:

```bash
CTRL=$(pgrep -n -f '\.venv/bin/minion-executor run')      # newest match: the control process
PIDS="$CTRL $(pgrep -P "$CTRL" | tr '\n' ' ')"            # plus the worker (child)
cpu() { ps -o cputime= -p "$(echo $PIDS | tr ' ' ',')" | awk -F'[:.]' \
  '{ if (NF == 4) s += $1*3600 + $2*60 + $3 + $4/100; else s += $1*60 + $2 + $3/100 }
   END { print s }'; }                                    # CPU seconds, summed over the tree
A=$(cpu); sleep 60; B=$(cpu); echo "scale=3; ($B - $A) / 60 * 100" | bc   # % of one core
```

`ps %cpu` is a lifetime average on macOS, so the start and end `cputime` of the 60 s window are used.
If the worker is not running (idle executors unload it), the tree is the control process alone. Adjust
the `pgrep` pattern if you start the executor another way (`pgrep -fl minion-executor` shows what
runs).
