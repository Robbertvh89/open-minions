"""Invariant verifier for a drained scale run (SPEC NF-2, NF-3). Read-only.

NF-2: every SUCCEEDED subtask has exactly one SUCCEEDED attempt, no other subtask has one, and
the promoted final objects match the SUCCEEDED subtasks one to one.
NF-3: after the drain every subtask is terminal and no acquirement is orphaned (an acquirement
whose subtask is not RUNNING on that attempt, or one without its heartbeat row, which the
watchdog could never reclaim). Task counters must equal the actual subtask counts.

Output carries counts and a few `task/subtask_id` samples only: no tokens, URLs or payloads.
"""

import asyncio
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from sqlalchemy import bindparam, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

SAMPLE_LIMIT = 5
TERMINAL_TASK = ("SUCCEEDED", "FAILED", "PARTIAL", "CANCELLED")

Sm = async_sessionmaker[AsyncSession]


@dataclass
class Check:
    name: str
    ok: bool
    detail: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {"name": self.name, "ok": self.ok, "detail": self.detail}


@dataclass
class VerifyReport:
    checks: list[Check]
    counts: dict[str, int]

    @property
    def ok(self) -> bool:
        return all(c.ok for c in self.checks)

    @property
    def failed(self) -> list[str]:
        return [c.name for c in self.checks if not c.ok]

    def to_dict(self) -> dict[str, Any]:
        return {"ok": self.ok, "counts": self.counts, "checks": [c.to_dict() for c in self.checks]}


def _sample(rows: list[Any]) -> list[str]:
    return [f"{r[0]}/{r[1]}/{r[2]}#{r[3]}" for r in rows[:SAMPLE_LIMIT]]


_SUCCESS_ATTEMPTS = text(
    """
    SELECT s.team, s.project, s.task_name, s.subtask_id
    FROM minion_subtask s
    LEFT JOIN (
        SELECT team, project, task_name, subtask_id, count(*) AS n
        FROM minion_subtask_attempt WHERE outcome = 'SUCCEEDED' AND team = :team
        GROUP BY team, project, task_name, subtask_id
    ) a USING (team, project, task_name, subtask_id)
    WHERE s.team = :team
      AND ((s.status = 'SUCCEEDED' AND coalesce(a.n, 0) <> 1)
        OR (s.status <> 'SUCCEEDED' AND coalesce(a.n, 0) > 0)
        OR coalesce(a.n, 0) > 1)
    ORDER BY 1, 2, 3, 4
    """
)

_NON_TERMINAL = text(
    "SELECT team, project, task_name, subtask_id FROM minion_subtask"
    " WHERE team = :team AND status IN ('PENDING', 'RUNNING') ORDER BY 1, 2, 3, 4"
)

_ORPHANS = text(
    """
    SELECT a.team, a.project, a.task_name, a.subtask_id
    FROM minion_subtask_acquirement a
    LEFT JOIN minion_subtask s USING (team, project, task_name, subtask_id)
    LEFT JOIN minion_executor_heartbeat h USING (acquirement_id)
    WHERE a.team = :team
      AND (s.status IS DISTINCT FROM 'RUNNING' OR s.attempt <> a.attempt
           OR h.acquirement_id IS NULL)
    ORDER BY 1, 2, 3, 4
    """
)

_COUNTERS = text(
    """
    SELECT t.team, t.project, t.task_name, 0
    FROM minion_task t
    LEFT JOIN (
        SELECT team, project, task_name, sum(succeeded) AS ok, sum(failed) AS failed,
               sum(cancelled) AS cancelled
        FROM minion_task_counter WHERE team = :team GROUP BY team, project, task_name
    ) k USING (team, project, task_name)
    LEFT JOIN (
        SELECT team, project, task_name, count(*) AS total,
               count(*) FILTER (WHERE status = 'SUCCEEDED') AS sok,
               count(*) FILTER (WHERE status = 'FAILED') AS sfailed,
               count(*) FILTER (WHERE status = 'CANCELLED') AS scancelled
        FROM minion_subtask WHERE team = :team GROUP BY team, project, task_name
    ) c USING (team, project, task_name)
    WHERE t.team = :team
      AND (t.total_subtasks <> coalesce(c.total, 0)
        OR coalesce(k.ok, 0) <> coalesce(c.sok, 0)
        OR coalesce(k.failed, 0) <> coalesce(c.sfailed, 0)
        OR coalesce(k.cancelled, 0) <> coalesce(c.scancelled, 0))
    ORDER BY 1, 2, 3
    """
)

_TASK_STATUS = text(
    """
    SELECT t.team, t.project, t.task_name, 0
    FROM minion_task t
    WHERE t.team = :team AND t.status NOT IN :terminal
    ORDER BY 1, 2, 3
    """
).bindparams(bindparam("terminal", expanding=True))

_SUCCEEDED_OUTPUTS = text(
    "SELECT output_uri FROM minion_subtask WHERE team = :team AND status = 'SUCCEEDED'"
)

_COUNTS = text(
    """
    SELECT
      (SELECT count(*) FROM minion_task WHERE team = :team),
      (SELECT count(*) FROM minion_subtask WHERE team = :team),
      (SELECT count(*) FROM minion_subtask WHERE team = :team AND status = 'SUCCEEDED'),
      (SELECT count(*) FROM minion_subtask WHERE team = :team AND status = 'FAILED'),
      (SELECT count(*) FROM minion_subtask WHERE team = :team AND status = 'CANCELLED'),
      (SELECT count(*) FROM minion_subtask_attempt WHERE team = :team),
      (SELECT count(*) FROM minion_subtask_acquirement WHERE team = :team)
    """
)


def list_final_objects(client: "S3Client", bucket: str, prefix: str) -> set[str]:
    """Promoted outputs under `prefix` (attempt objects live under `_attempts/` and are skipped)."""
    keys: set[str] = set()
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        keys.update(o["Key"] for o in page.get("Contents", []) if "/_attempts/" not in o["Key"])
    return keys


def _key_of(uri: str, bucket: str) -> str:
    head = f"s3://{bucket}/"
    return uri.removeprefix(head)


async def verify(
    sm: Sm,
    *,
    team: str,
    s3: "S3Client | None" = None,
    bucket: str = "minion",
    output_prefix: str | None = None,
) -> VerifyReport:
    """Run every check for `team`. Without `s3` the object-count check is skipped (and said so)."""
    p = {"team": team}
    # One snapshot for every query: a run that is still finishing cannot make the checks disagree.
    async with sm() as session, session.begin():
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
        counts_row = (await session.execute(_COUNTS, p)).one()
        succ = list((await session.execute(_SUCCESS_ATTEMPTS, p)).all())
        pending = list((await session.execute(_NON_TERMINAL, p)).all())
        orphans = list((await session.execute(_ORPHANS, p)).all())
        counters = list((await session.execute(_COUNTERS, p)).all())
        bad_status = list(
            (await session.execute(_TASK_STATUS, {**p, "terminal": list(TERMINAL_TASK)})).all()
        )
        outputs = [
            _key_of(r[0], bucket)
            for r in (await session.execute(_SUCCEEDED_OUTPUTS, p)).all()
            if r[0]
        ]
    counts = dict(
        zip(
            ("tasks", "subtasks", "succeeded", "failed", "cancelled", "attempts", "acquirements"),
            (int(x) for x in counts_row),
            strict=True,
        )
    )
    checks = [
        Check(
            "nf2_one_succeeded_attempt_per_succeeded_subtask",
            not succ,
            {"violations": len(succ), "samples": _sample(succ)},
        ),
        await _objects_check(s3, bucket, output_prefix or f"outputs/{team}/", outputs, counts),
        Check(
            "nf3_all_subtasks_terminal",
            not pending,
            {"non_terminal": len(pending), "samples": _sample(pending)},
        ),
        Check(
            "nf3_no_orphan_acquirements",
            not orphans,
            {"orphans": len(orphans), "samples": _sample(orphans)},
        ),
        Check(
            "task_counters_match_subtasks",
            not counters,
            {
                "violations": len(counters),
                "samples": [f"{r[0]}/{r[1]}/{r[2]}" for r in counters[:SAMPLE_LIMIT]],
            },
        ),
        Check(
            "tasks_terminal",
            not bad_status,
            {"non_terminal_tasks": len(bad_status)},
        ),
    ]
    return VerifyReport(checks=checks, counts=counts)


async def _objects_check(
    s3: "S3Client | None",
    bucket: str,
    prefix: str,
    expected_keys: list[str],
    counts: dict[str, int],
) -> Check:
    name = "nf2_final_objects_match_succeeded"
    if s3 is None:
        return Check(name, True, {"skipped": True})
    listed = await asyncio.to_thread(list_final_objects, s3, bucket, prefix)
    expected = set(expected_keys)
    missing, extra = expected - listed, listed - expected
    return Check(
        name,
        not missing and not extra and len(listed) == counts["succeeded"],
        {
            "final_objects": len(listed),
            "succeeded": counts["succeeded"],
            "missing": len(missing),
            "unexpected": len(extra),
        },
    )
