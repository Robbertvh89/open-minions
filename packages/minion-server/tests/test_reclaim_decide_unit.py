"""Pure `decide()` table (TASK-35): the same rules as the old per-row reclaim_one."""

import uuid
from datetime import UTC, datetime

import pytest
from minion_common.enums import AttemptOutcome, SubtaskStatus, TaskStatus
from minion_server.repositories.completion import Owned
from minion_server.repositories.reclaim import Finish, ReclaimRow
from minion_server.services.completion import next_status
from minion_server.services.reclaim import ReclaimResult, decide

P, F, C = SubtaskStatus.PENDING, SubtaskStatus.FAILED, SubtaskStatus.CANCELLED


def _row(
    *,
    stale: bool = True,
    never_acked: bool = False,
    over_lease: bool = False,
    budget_used: int = 0,
    unacked_used: int = 0,
    max_attempts: int = 3,
    cancelled: bool = False,
) -> ReclaimRow:
    o = Owned(
        acquirement_id=uuid.uuid4(),
        team="t",
        project="p",
        task_name="n",
        subtask_id=1,
        executor_id=uuid.uuid4(),
        attempt=1,
        acquired_at=datetime.now(UTC),
        record_count=1,
        max_attempts=max_attempts,
        task_output_uri="s3://b/o",
        task_status=TaskStatus.CANCELLED if cancelled else TaskStatus.RUNNING,
    )
    return ReclaimRow(o, stale, never_acked, over_lease, budget_used, unacked_used)


@pytest.mark.parametrize(
    ("row", "cap", "status", "code"),
    [
        # stale, acked: HEARTBEAT_TIMEOUT consumes budget
        (_row(budget_used=0), 0, P, "HEARTBEAT_TIMEOUT"),
        (_row(budget_used=1), 0, P, "HEARTBEAT_TIMEOUT"),
        (_row(budget_used=2), 0, F, "HEARTBEAT_TIMEOUT"),
        (_row(budget_used=2, cancelled=True), 0, C, "HEARTBEAT_TIMEOUT"),
        # never acked, cap 0 = paper semantics
        (_row(never_acked=True, budget_used=2), 0, F, "HEARTBEAT_TIMEOUT"),
        # never acked below the cap: free, budget untouched even when exhausted
        (_row(never_acked=True, unacked_used=0, budget_used=2), 2, P, "NOT_ACKNOWLEDGED"),
        (_row(never_acked=True, unacked_used=1), 2, P, "NOT_ACKNOWLEDGED"),
        (_row(never_acked=True, unacked_used=1, cancelled=True), 2, C, "NOT_ACKNOWLEDGED"),
        # at the cap: falls through to a budget-consuming timeout
        (_row(never_acked=True, unacked_used=2, budget_used=0), 2, P, "HEARTBEAT_TIMEOUT"),
        (_row(never_acked=True, unacked_used=2, budget_used=2), 2, F, "HEARTBEAT_TIMEOUT"),
        # fresh heartbeat but over the lease
        (_row(stale=False, over_lease=True, budget_used=0), 0, P, "MAX_LEASE_EXCEEDED"),
        (_row(stale=False, over_lease=True, budget_used=2), 0, F, "MAX_LEASE_EXCEEDED"),
        # stale beats over lease
        (_row(stale=True, over_lease=True), 0, P, "HEARTBEAT_TIMEOUT"),
    ],
)
def test_decide_finish(row: ReclaimRow, cap: int, status: SubtaskStatus, code: str) -> None:
    d = decide(row, unacked_free_cap=cap)
    assert isinstance(d, Finish)
    assert d.outcome == AttemptOutcome.EXPIRED
    assert d.subtask_status == status
    written = d.error if d.error is not None else d.attempt_error
    assert written is not None
    assert written["code"] == code
    assert (d.attempt_error is not None) == (code == "NOT_ACKNOWLEDGED")  # history only
    if code != "NOT_ACKNOWLEDGED":
        assert status == next_status(
            row.budget_used + 1,
            row.owned.max_attempts,
            retryable=True,
            task_cancelled=row.owned.task_status == TaskStatus.CANCELLED,
        )


def test_decide_not_stale() -> None:
    assert decide(_row(stale=False), unacked_free_cap=2) == ReclaimResult.NOT_STALE
    assert decide(_row(stale=False, never_acked=True), unacked_free_cap=2) == (
        ReclaimResult.NOT_STALE
    )


def test_decide_flags_cap_reached_only_at_the_cap() -> None:
    at_cap = decide(_row(never_acked=True, unacked_used=2), unacked_free_cap=2)
    below = decide(_row(never_acked=True, unacked_used=1), unacked_free_cap=2)
    assert isinstance(at_cap, Finish) and at_cap.cap_reached
    assert isinstance(below, Finish) and not below.cap_reached
