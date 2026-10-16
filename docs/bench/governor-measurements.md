# Governor measurements on a real Mac (NF-4, NF-8, NF-9)

How the numbers in [ADR-0006](../adr/0006-cooperative-capacity-governor.md) "Measurements" are
produced. CI covers the logic with the fake engine; the GPU, `ioreg` and real signals need a Mac.
Prepare: on AC power, close heavy apps, `uv sync --package minion-executor --extra mlx --extra hf`,
models cached once (`bench` downloads them on first use, network). Nothing here needs a server.

## NF-8: budget adherence (about 12 min for 3 profiles x 1 model)

```bash
uv run minion-executor bench --budget-check --models deploy/bench-models.yaml \
  --model qwen2.5-0.5b-mlx --profiles light,balanced,full --duration 60 --baseline-s 20 \
  --repeats 3 --out docs/bench/m4-governor-apple-m3-pro.json | tee docs/bench/m4-governor-apple-m3-pro.table.md
# embeddings too:  --model minilm-mlx
```

Per (model, profile, repeat): 20 s idle GPU baseline, `--duration` s unpaced (duty 1.0), `--duration` s
paced at the profile duty. Per profile about `baseline + 2 x duration` = 140 s (full skips the
unpaced phase: 80 s), times `--repeats`. Columns: `baseline` (median ioreg %), `unpaced` / `measured`
(baseline-subtracted busy share), `expected` = duty x unpaced, `pacer-reported` (worker `busy/wall`,
advisory), `footprint GB` (peak `phys_footprint`, 10 Hz) vs `budget GB`, and pass/fail for the hard
bound (share <= duty + 10 pp), accuracy (+-10 pp of expected) and memory (<= 1.1 x). Exit 1 if any
row fails, 2 if `ioreg` is unavailable.

Reading it: a CPU-bound workload (short embedding batches, tokenization) has `unpaced` well below
100 %; judge it by `expected`, not by the profile duty (ADR-0006 Gap 1). A high `baseline` means
something else is using the GPU: repeat with the Mac quiet. The batch size is the profile's cap.

## NF-9: reaction time (< 5 s)

CI (`tests/test_governor_reaction.py`, about 15 s): 20 trials over thermal serious, memory warning,
memory critical (release), power (battery 5 %), user return, with `FileSignals`, a real fake-engine
worker with 100 ms steps and a 0.25 s poll. It reports p50 and max per event
(`uv run pytest packages/minion-executor/tests/test_governor_reaction.py -s`).

On the Mac with real signals and the production poll (2 s), `--watch-reactions` runs a real
`GovernorRuntime` and prints one timestamped line (epoch seconds) per decision change:

```bash
uv run minion-executor bench --watch-reactions --duration 120 2>/dev/null
```

- **User return.** Let the Mac sit idle, or run it with `MINION_EXECUTOR_CONFIG` setting a short
  `idle_after_min`, then touch the keyboard. The line `user-return reaction_s=...` is back-dated from
  the HID idle counter (`taken_at - hid_idle_s`), so it needs no stopwatch. It is the time to the
  *observation*; the decision change line follows in the same poll.
- **Memory pressure.** Needs the operator's approval (it makes the Mac swap). In another terminal:
  `python3 -c 'import time;print(time.time())'; timeout 60 memory_pressure -l warn`, then
  subtract that epoch from the `decision paused=memory_pressure` line. Critical: `-l critical`
  shows `release_now=True`.
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

A real `GovernorRuntime` polling the real signals at the production poll, first idle, then
owner-paused (`control.json`), reporting this process's CPU time (all threads) over the window.
For a whole `minion-executor run` against a server, sample the control process instead:

```bash
PID=$(pgrep -f 'minion-executor run' | head -1)
for i in $(seq 12); do ps -o %cpu= -p "$PID"; sleep 5; done   # idle; repeat after `minion-executor pause`
```

`ps %cpu` is a lifetime average on macOS, so prefer `ps -o cputime= -p "$PID"` at the start and end of
the 60 s window: `(end - start) / 60 s`.
