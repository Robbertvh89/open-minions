"""Quantum pacing (ADR-0006): sleep only once enough busy time accumulated; sync only then."""

from __future__ import annotations

import multiprocessing
import sys
import time
import types
from typing import Any

import pytest
from minion_executor.worker.control import WorkerCtl
from minion_executor.worker.process import _Ctl, _send
from minion_executor.worker.throttle import QUANTUM_S, DutyCyclePacer


class _Clock:
    def __init__(self) -> None:
        self.now = 0.0
        self.slept: list[float] = []

    def __call__(self) -> float:
        return self.now

    def sleep(self, s: float) -> None:
        self.slept.append(s)
        self.now += s


def _pacer(duty: float, c: _Clock) -> DutyCyclePacer:
    return DutyCyclePacer(duty, c, c.sleep, quantum_s=0.05)


def test_pacer_accumulates_and_sleeps_only_at_the_quantum() -> None:
    c = _Clock()
    p = _pacer(0.5, c)
    for _ in range(4):  # 4 x 10 ms < quantum
        c.now += 0.01
        assert not p.quantum_due()
        assert p.step_done() == 0.0
    assert c.slept == []
    c.now += 0.02  # accumulated 0.06 >= 0.05
    assert p.quantum_due()
    assert p.step_done() == pytest.approx(0.06)  # busy * (1 - d) / d
    assert p.busy_s == pytest.approx(0.06)
    c.now += 0.01  # the accumulator restarted
    assert not p.quantum_due()


def test_pacer_sleep_length_follows_duty_and_credit_is_capped() -> None:
    c = _Clock()
    p = _pacer(0.25, c)
    c.now += 0.1
    assert p.step_done() == pytest.approx(0.3)

    over = _Clock()

    def late(s: float) -> bool:
        over.now += s + 5.0  # wakes 5 s late
        return False

    q = DutyCyclePacer(0.5, over, late, quantum_s=0.05)
    over.now += 0.1
    q.step_done()
    over.now += 0.6  # owes 0.6 s; the credit is capped at 0.5 s, so ~0.1 s is slept
    slept = q.step_done()
    assert slept == pytest.approx(5.0 + 0.1) and over.now > 0  # late wake-up of 5 s on top


def test_pacer_full_duty_never_due_or_sleeping() -> None:
    c = _Clock()
    p = _pacer(1.0, c)
    c.now += 5.0
    assert not p.quantum_due()
    assert p.step_done() == 0.0 and c.slept == []


def test_pacer_duty_drop_forces_the_next_step_to_drain_and_sleep() -> None:
    c = _Clock()
    p = _pacer(1.0, c)
    c.now += 0.001
    p.set_duty(0.5)
    assert p.quantum_due()  # tiny step, but the duty just dropped
    assert p.step_done() == pytest.approx(0.001)
    c.now += 0.001
    assert not p.quantum_due()
    p.set_duty(0.9)
    p.set_duty(0.95)  # raising never forces
    assert not p.quantum_due()


def test_pacer_flush_pays_the_partial_quantum() -> None:
    c = _Clock()
    p = _pacer(0.5, c)
    c.now += 0.01
    assert p.step_done() == 0.0
    c.now += 0.01
    assert p.flush() == pytest.approx(0.02)
    assert p.flush() == 0.0  # nothing left owed
    assert _pacer(1.0, c).flush() == 0.0


def test_latched_answer_busy_crossing_quantum_after_will_sleep_does_not_sleep() -> None:
    c = _Clock()
    p = _pacer(0.5, c)
    c.now += 0.01
    assert not p.quantum_due()  # asked: no
    c.now += 0.06  # busy crosses the quantum before the checkpoint
    assert p.step_done() == 0.0 and c.slept == []
    c.now += 0.01
    assert p.quantum_due()
    assert p.step_done() == pytest.approx(0.08)


def test_latched_answer_duty_drop_at_checkpoint_syncs_next_step() -> None:
    c = _Clock()
    p = _pacer(1.0, c)
    c.now += 0.001
    assert not p.quantum_due()
    p.set_duty(0.5)  # applied at the checkpoint, after will_sleep said no
    assert p.step_done() == 0.0 and c.slept == []
    c.now += 0.001
    assert p.quantum_due()  # the force carried over
    assert p.step_done() == pytest.approx(0.002)


def test_unasked_step_done_falls_back_to_the_quantum() -> None:
    c = _Clock()
    p = _pacer(0.5, c)
    c.now += 0.06
    assert p.step_done() == pytest.approx(0.06)


def test_finish_returns_from_the_flush_when_paused() -> None:
    ctl, parent = _ctl(0.5)
    ctl._pacer._acc = 0.0
    ctl._pacer._last = time.monotonic() - 2.0  # owes ~2 s
    _send(parent, WorkerCtl(gen=1, duty=0.5, paused=True).to_msg())
    t0 = time.monotonic()
    ctl.finish()
    assert time.monotonic() - t0 < 1.0


def test_quantum_default_constant() -> None:
    assert 0 < QUANTUM_S <= 0.1


def _ctl(duty: float, cap: float = 1.0) -> tuple[_Ctl, Any]:
    parent, child = multiprocessing.Pipe()
    ctl = _Ctl(child, WorkerCtl(gen=0, duty=duty, paused=False), 3600.0, 1 << 20, lambda: None, cap)
    return ctl, parent


def test_ctl_will_sleep_follows_live_duty_and_the_budget_cap() -> None:
    ctl, parent = _ctl(1.0)
    assert not ctl.will_sleep()
    _send(parent, WorkerCtl(gen=1, duty=0.5, paused=False).to_msg())
    assert ctl.will_sleep()  # message applied by will_sleep itself; duty just dropped
    assert ctl._pacer.duty == 0.5
    ctl.checkpoint(1)  # drains the quantum
    assert not ctl.will_sleep()
    _send(parent, WorkerCtl(gen=2, duty=1.0, paused=False).to_msg())
    assert not ctl.will_sleep()

    capped, _ = _ctl(1.0, cap=0.4)  # the budget's duty bounds a looser live duty
    assert capped._pacer.duty == 0.4


def test_completion_runner_syncs_only_when_the_quantum_is_due(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fake mx / mlx_lm: the runner calls sync_gpu exactly on steps where will_sleep() is True."""
    from minion_common.schemas import CompletionParams
    from minion_executor.runners import mlx_common
    from minion_executor.runners.mlx_completion import MlxCompletionRunner

    class Resp:
        def __init__(self, uid: int, token: int, reason: str | None) -> None:
            self.uid, self.token, self.finish_reason = uid, token, reason

    class FakeGen:
        def __init__(self, *a: Any, **k: Any) -> None:
            self.n = 0

        def insert(self, prompts: list[list[int]]) -> list[int]:
            return [0]

        def next(self) -> tuple[list[Any], list[Any]]:
            self.n += 1
            return [], [Resp(0, 7, "length" if self.n == 6 else None)]

        def close(self) -> None:
            pass

    mx = types.ModuleType("mlx.core")
    mx.random = types.SimpleNamespace(seed=lambda s: None)  # type: ignore[attr-defined]
    gen_mod = types.ModuleType("mlx_lm.generate")
    gen_mod.BatchGenerator = FakeGen  # type: ignore[attr-defined]
    gen_mod.generation_stream = object()  # type: ignore[attr-defined]
    samp = types.ModuleType("mlx_lm.sample_utils")
    samp.make_sampler = lambda **k: None  # type: ignore[attr-defined]
    for name, mod in {
        "mlx": types.ModuleType("mlx"),
        "mlx.core": mx,
        "mlx_lm": types.ModuleType("mlx_lm"),
        "mlx_lm.generate": gen_mod,
        "mlx_lm.sample_utils": samp,
    }.items():
        monkeypatch.setitem(sys.modules, name, mod)

    syncs: list[int] = []
    monkeypatch.setattr(mlx_common, "sync_gpu", lambda stream=None: syncs.append(1))
    monkeypatch.setattr(mlx_common, "clamp_wired_limit", lambda b: 0)

    class Ctl:
        def __init__(self) -> None:
            self.calls = 0
            self.synced_at: list[int] = []

        def will_sleep(self) -> bool:
            self.calls += 1
            return self.calls % 3 == 0  # every third step ends a quantum

        def checkpoint(self, records_done: int | None = None) -> None:
            if len(syncs) > len(self.synced_at):
                self.synced_at.append(self.calls)

    from types import SimpleNamespace

    runner: Any = MlxCompletionRunner.__new__(MlxCompletionRunner)
    runner._budget = SimpleNamespace(max_batch_size=2)
    runner._cfg = SimpleNamespace(max_prompt_tokens=100)
    runner._context = 0
    runner._model = object()
    runner._tok = SimpleNamespace(eos_token_ids=[0], decode=lambda ids: "x")
    runner._encode = lambda rec, params: [1, 2, 3]
    ctl = Ctl()
    out = runner._run([{"id": "a"}], CompletionParams(max_tokens=8), ctl)
    assert out[0]["finish_reason"] == "length"
    assert ctl.calls == 6
    assert len(syncs) == 2  # steps 3 and 6 only
    assert ctl.synced_at == [3, 6]
