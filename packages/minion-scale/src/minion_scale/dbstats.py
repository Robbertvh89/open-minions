"""Database-side and container statistics for a scale run (operator tooling, read-only).

`pg_stat_statements` (needs `shared_preload_libraries`, see deploy/compose.scale.yml) is reported
as a before/after delta per statement; `pg_stat_user_tables` for the minion tables (dead tuples,
autovacuum); `pg_stat_activity` connection counts. Query texts from pg_stat_statements are the
normalized statements (parameters already replaced by `$n`), truncated; never row values.
Only this role's SELECT/INSERT/UPDATE/DELETE/WITH statements are read (not other roles' or utility
statements such as SET/CREATE/ALTER, which can carry literal values).
Docker stats only look at containers of one named compose project.
"""

import asyncio
import json
import shutil
import subprocess
from typing import Any

from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

Sm = async_sessionmaker[AsyncSession]

QUERY_CHARS = 160
TOP_STATEMENTS = 15

_STATEMENTS = text(
    """
    SELECT queryid::text, calls, total_exec_time, rows, shared_blks_hit, shared_blks_read,
           left(regexp_replace(query, '\\s+', ' ', 'g'), :chars)
    FROM pg_stat_statements
    WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
      AND userid = (SELECT oid FROM pg_roles WHERE rolname = current_user)
      AND query ~* '^\\s*(select|insert|update|delete|with)\\y'
      AND query NOT LIKE '%pg_stat_%'
    """
)

_TABLES = text(
    """
    SELECT relname, n_live_tup, n_dead_tup, seq_scan, idx_scan, n_tup_ins, n_tup_upd, n_tup_del,
           n_tup_hot_upd, autovacuum_count, autoanalyze_count,
           last_autovacuum
    FROM pg_stat_user_tables WHERE relname LIKE 'minion\\_%' ORDER BY relname
    """
)

_CONNECTIONS = text(
    """
    SELECT coalesce(state, 'background'), count(*)
    FROM pg_stat_activity WHERE datname = current_database() GROUP BY 1 ORDER BY 1
    """
)

_MAX_CONNECTIONS = text("SELECT current_setting('max_connections')::int")


async def ensure_extension(sm: Sm) -> bool:
    """Best-effort `CREATE EXTENSION pg_stat_statements` (needs the preload and a superuser)."""
    async with sm() as s:
        try:
            await s.execute(text("CREATE EXTENSION IF NOT EXISTS pg_stat_statements"))
            await s.commit()
        except DBAPIError:
            await s.rollback()
            return False
        try:
            await s.execute(text("SELECT 1 FROM pg_stat_statements LIMIT 1"))
        except DBAPIError:
            await s.rollback()
            return False  # extension exists but the library is not preloaded
    return True


async def snapshot(sm: Sm) -> dict[str, Any]:
    """One point-in-time reading. `statements` is None when pg_stat_statements is unavailable."""
    async with sm() as s:
        statements: list[dict[str, Any]] | None
        try:
            rows = (await s.execute(_STATEMENTS, {"chars": QUERY_CHARS})).all()
            statements = [
                {
                    "queryid": r[0],
                    "calls": int(r[1]),
                    "total_exec_ms": float(r[2]),
                    "rows": int(r[3]),
                    "shared_blks_hit": int(r[4]),
                    "shared_blks_read": int(r[5]),
                    "query": r[6],
                }
                for r in rows
            ]
        except DBAPIError:
            statements = None
            await s.rollback()
        tables = [
            {
                "table": r[0],
                "live_tuples": int(r[1]),
                "dead_tuples": int(r[2]),
                "seq_scans": int(r[3]),
                "idx_scans": int(r[4] or 0),
                "inserts": int(r[5]),
                "updates": int(r[6]),
                "deletes": int(r[7]),
                "hot_updates": int(r[8]),
                "autovacuum_count": int(r[9]),
                "autoanalyze_count": int(r[10]),
                "last_autovacuum": r[11].isoformat() if r[11] else None,
            }
            for r in (await s.execute(_TABLES)).all()
        ]
        conns = {r[0]: int(r[1]) for r in (await s.execute(_CONNECTIONS)).all()}
        max_conns = int((await s.execute(_MAX_CONNECTIONS)).scalar_one())
    return {
        "statements": statements,
        "tables": tables,
        "connections": {"by_state": conns, "total": sum(conns.values()), "max": max_conns},
    }


def statement_deltas(
    before: list[dict[str, Any]] | None, after: list[dict[str, Any]] | None
) -> list[dict[str, Any]] | None:
    """Top statements by execution time spent between two snapshots."""
    if after is None:
        return None
    prev = {s["queryid"]: s for s in before or []}
    out: list[dict[str, Any]] = []
    for s in after:
        b = prev.get(s["queryid"])
        calls = s["calls"] - (b["calls"] if b else 0)
        if calls <= 0:
            continue
        ms = s["total_exec_ms"] - (b["total_exec_ms"] if b else 0.0)
        out.append(
            {
                "query": s["query"],
                "calls": calls,
                "total_exec_ms": round(ms, 3),
                "mean_exec_ms": round(ms / calls, 4),
                "rows": s["rows"] - (b["rows"] if b else 0),
                "shared_blks_hit": s["shared_blks_hit"] - (b["shared_blks_hit"] if b else 0),
                "shared_blks_read": s["shared_blks_read"] - (b["shared_blks_read"] if b else 0),
            }
        )
    out.sort(key=lambda d: d["total_exec_ms"], reverse=True)
    return out[:TOP_STATEMENTS]


def db_report(before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    """Delta report from two snapshots (table counters are cumulative: both ends are kept)."""
    return {
        "pg_stat_statements": (
            None
            if after["statements"] is None
            else {"top_by_total_time": statement_deltas(before["statements"], after["statements"])}
        ),
        "tables_before": before["tables"],
        "tables_after": after["tables"],
        "connections_before": before["connections"],
        "connections_after": after["connections"],
    }


def docker_stats(project: str) -> list[dict[str, Any]] | None:
    """`docker stats --no-stream` for the containers of one compose project (None: no docker)."""
    docker = shutil.which("docker")
    if docker is None or not project:
        return None
    ps = subprocess.run(  # noqa: S603
        [docker, "ps", "-q", "--filter", f"label=com.docker.compose.project={project}"],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    ids = ps.stdout.split()
    if ps.returncode != 0 or not ids:
        return None
    stats = subprocess.run(  # noqa: S603
        [docker, "stats", "--no-stream", "--format", "{{json .}}", *ids],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    keep = ("Name", "CPUPerc", "MemUsage", "MemPerc", "NetIO", "BlockIO", "PIDs")
    rows = []
    for line in stats.stdout.splitlines():
        try:
            d = json.loads(line)
        except ValueError:
            continue
        rows.append({k: d.get(k) for k in keep})
    return rows or None


async def docker_stats_async(project: str) -> list[dict[str, Any]] | None:
    return await asyncio.to_thread(docker_stats, project)
