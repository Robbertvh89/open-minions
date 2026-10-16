"""Governor runtime (SPEC FR-52..56, ADR-0006 D1): polls the signals and the owner's control file,
runs the pure `decide` step and publishes the resulting `Decision` to the executor loop.

One asyncio task (`GovernorRuntime.run`) beside the loop, every `poll_s` (2 s). Signal reads go
through `read_with_timeout` (a wedged framework call is a failed read, never a stall) on one source
object for the whole run; the control file is read in a worker thread under a timeout. The loop
reads `decision` (always the latest) and waits for changes with `wait_change(version, timeout)`.

Fail closed (invariant 6): until the first poll finished the decision is paused with
`signals_unavailable`; an unexpected error while polling pauses the same way. Nothing here logs
signal values beyond coarse state names, and no payload ever passes through.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol

import structlog
from minion_common.enums import CapacityProfile, PausedReason
from minion_common.schemas import Budget

from minion_executor.capacity.config import ExecutorConfig
from minion_executor.capacity.control import ControlUnusable, OwnerControl, read_control
from minion_executor.capacity.governor import (
    Decision,
    GovernorTiming,
    GovState,
    ProfileSource,
    decide,
    initial_state,
)
from minion_executor.capacity.profiles import ProfileSpec, budget_for
from minion_executor.capacity.signals import (
    DEFAULT_READ_TIMEOUT_S,
    SignalError,
    SignalSnapshot,
    SignalSource,
    read_with_timeout,
)

log = structlog.get_logger("minion_executor.governor")

DEFAULT_POLL_S = 2.0
CONTROL_READ_TIMEOUT_S = 1.5


class Governor(Protocol):
    """What the executor loop needs from a governor."""

    @property
    def decision(self) -> Decision: ...

    @property
    def version(self) -> int: ...

    @property
    def snapshot(self) -> SignalSnapshot | None: ...

    @property
    def nominal(self) -> Budget:
        """Full budget of the profile in force (above `decision.budget` during a hold)."""
        ...

    async def prime(self) -> None:
        """Do the first poll so `decision` is real before the loop acquires anything."""
        ...

    async def run(self) -> None:
        """Poll until cancelled."""
        ...

    async def wait_change(self, version: int, timeout_s: float) -> int:
        """Return a version different from `version` as soon as one exists, else after
        `timeout_s` the unchanged `version`."""
        ...


def decision_key(d: Decision) -> tuple[object, ...]:
    """What counts as a change worth waking the loop for (timer starts are not)."""
    return (
        d.profile,
        d.budget,
        d.paused_reason,
        d.release_now,
        d.pause_expired,
        d.trim,
        d.profile_source,
        d.candidate,
    )


class _Waiters:
    def __init__(self) -> None:
        self.version = 0
        self._events: set[asyncio.Event] = set()

    def bump(self) -> None:
        self.version += 1
        for ev in self._events:
            ev.set()

    async def wait(self, version: int, timeout_s: float) -> int:
        if self.version != version:
            return self.version
        ev = asyncio.Event()
        self._events.add(ev)
        try:
            await asyncio.wait_for(ev.wait(), max(0.0, timeout_s))
        except TimeoutError:
            pass
        finally:
            self._events.discard(ev)
        return self.version


class FixedGovernor:
    """A governor that never polls: a constant budget, never paused (bench, tests, and the
    default when the loop is given only a `Budget`). `update` swaps the decision and wakes
    waiters; it is the seam tests use to script a governor."""

    def __init__(self, budget: Budget) -> None:
        self._waiters = _Waiters()
        self._decision = self._make(budget)

    @staticmethod
    def _make(budget: Budget) -> Decision:
        return Decision(
            profile=budget.profile,
            budget=budget,
            paused_reason=None,
            release_now=False,
            pause_expired=False,
            trim=False,
            profile_source=ProfileSource.PINNED,
            candidate=None,
            candidate_since=None,
        )

    @property
    def decision(self) -> Decision:
        return self._decision

    @property
    def version(self) -> int:
        return self._waiters.version

    @property
    def snapshot(self) -> SignalSnapshot | None:
        return None

    @property
    def nominal(self) -> Budget:
        return self._decision.budget

    def update(self, decision: Decision) -> None:
        changed = decision_key(decision) != decision_key(self._decision)
        self._decision = decision
        if changed:
            self._waiters.bump()

    async def prime(self) -> None:
        return None

    async def run(self) -> None:
        await asyncio.Event().wait()

    async def wait_change(self, version: int, timeout_s: float) -> int:
        return await self._waiters.wait(version, timeout_s)


class GovernorRuntime:
    def __init__(
        self,
        source: SignalSource,
        cfg: ExecutorConfig,
        specs: dict[CapacityProfile, ProfileSpec],
        *,
        state_dir: Path,
        total_gb: float,
        pinned: CapacityProfile | None = None,
        timing: GovernorTiming | None = None,
        poll_s: float = DEFAULT_POLL_S,
        read_timeout_s: float = DEFAULT_READ_TIMEOUT_S,
        control_timeout_s: float = CONTROL_READ_TIMEOUT_S,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        local_clock: Callable[[], datetime] = lambda: datetime.now().astimezone(),
    ) -> None:
        self._source = source  # one object for the whole run: the read bound is per source
        self._cfg, self._specs = cfg, specs
        self._state_dir, self._total_gb, self._pinned = state_dir, total_gb, pinned
        self._timing = timing or GovernorTiming()
        self._poll_s, self._read_timeout_s = poll_s, read_timeout_s
        self._control_timeout_s = control_timeout_s
        self._clock, self._local_clock = clock, local_clock
        # always the seeded state: a cold GovState() has no applied budget and skips the hold
        self._state: GovState = initial_state(cfg, pinned, specs=specs, total_gb=total_gb)
        self._waiters = _Waiters()
        self._snapshot: SignalSnapshot | None = None
        self._signal_note: str | None = None  # last logged read problem (log once per change)
        self._control_note: str | None = None
        self._decision = self._closed_decision()

    # --- state ---------------------------------------------------------------------------------

    def _closed_decision(self) -> Decision:
        """Fail-closed decision used before the first poll and after an unexpected error."""
        profile = self._state.profile or self._cfg.active_profile
        budget = self._state.applied or budget_for(profile, self._total_gb, self._specs)
        return Decision(
            profile=profile,
            budget=budget,
            paused_reason=PausedReason.SIGNALS_UNAVAILABLE,
            release_now=False,
            pause_expired=False,
            trim=False,
            profile_source=ProfileSource.PINNED if self._pinned else ProfileSource.ACTIVE,
            candidate=None,
            candidate_since=None,
        )

    @property
    def decision(self) -> Decision:
        return self._decision

    @property
    def version(self) -> int:
        return self._waiters.version

    @property
    def snapshot(self) -> SignalSnapshot | None:
        return self._snapshot

    @property
    def nominal(self) -> Budget:
        return budget_for(self._decision.profile, self._total_gb, self._specs)

    async def wait_change(self, version: int, timeout_s: float) -> int:
        return await self._waiters.wait(version, timeout_s)

    def _publish(self, decision: Decision) -> None:
        old = self._decision
        self._decision = decision
        if decision_key(decision) == decision_key(old):
            return
        self._waiters.bump()
        log.info(
            "governor_decision",
            profile=decision.profile.value,
            source=decision.profile_source.value,
            paused_reason=decision.paused_reason.value if decision.paused_reason else None,
            memory_gb=decision.budget.memory_gb,
            gpu_duty_cycle=decision.budget.gpu_duty_cycle,
            max_batch_size=decision.budget.max_batch_size,
            release_now=decision.release_now,
            pause_expired=decision.pause_expired,
        )

    # --- polling -------------------------------------------------------------------------------

    async def _read_signals(self) -> SignalSnapshot | None:
        try:
            snap = await read_with_timeout(self._source, self._read_timeout_s)
        except Exception as exc:  # SignalError, or whatever a source raised: a failed read
            note = str(exc) if isinstance(exc, SignalError) else type(exc).__name__
            if note != self._signal_note:
                self._signal_note = note
                log.warning("signal_read_failed", reason=note)
            return None
        if self._signal_note is not None:
            self._signal_note = None
            log.info("signal_read_recovered")
        return snap

    async def _read_owner(self) -> OwnerControl | ControlUnusable | None:
        try:
            owner = await asyncio.wait_for(
                asyncio.to_thread(read_control, self._state_dir), self._control_timeout_s
            )
        except TimeoutError:
            owner = ControlUnusable("control read timed out")
        note = owner.reason if isinstance(owner, ControlUnusable) else None
        if note != self._control_note:  # once per change, never per poll
            if note is not None:
                log.warning("owner_control_unusable", reason=note)
            else:
                log.info("owner_control_usable")
            self._control_note = note
        return owner

    async def poll_once(self) -> Decision:
        """One governor step: read, decide, publish. Never raises on a bad read or file."""
        snap = await self._read_signals()
        owner = await self._read_owner()
        decision, self._state = decide(
            snap,
            self._cfg,
            self._specs,
            owner,
            self._clock(),
            self._local_clock(),
            self._state,
            total_gb=self._total_gb,
            pinned=self._pinned,
            timing=self._timing,
        )
        self._snapshot = self._state.last_snapshot
        self._publish(decision)
        return decision

    async def prime(self) -> None:
        await self.poll_once()

    async def run(self) -> None:
        while True:
            try:
                await self.poll_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # a bug must not leave the last (open) decision in force
                log.error("governor_poll_error", error=type(exc).__name__)
                self._publish(self._closed_decision())
            await asyncio.sleep(self._poll_s)
