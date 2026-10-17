"""JSON report assembly (schema: docs/bench/scale-report-schema.md).

Every CLI command writes a partial document with the same top-level layout; `merge` combines
them. Nothing here ever receives a token, a URL with credentials or an object payload: the
server is reduced to `host:port`, the sims to counts and timings.
"""

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from minion_scale.metrics import Recorder

SCHEMA = "minion-scale-report/1"
SECTIONS = ("config", "run", "db", "docker", "explain", "verify")


def server_label(url: str) -> str:
    """`host:port` only: drops scheme details, userinfo, path and query."""
    parts = urlsplit(url)
    return f"{parts.hostname or ''}:{parts.port}" if parts.port else (parts.hostname or "")


def run_section(rec: Recorder, *, duration_s: float, sims: int, procs: int) -> dict[str, Any]:
    d = rec.to_dict()
    completed = int(d["events"].get("completed", 0))
    return {
        "duration_s": round(duration_s, 3),
        "sims": sims,
        "procs": procs,
        "completed": completed,
        "throughput_per_s": round(completed / duration_s, 3) if duration_s > 0 else 0.0,
        "assigned": int(d["events"].get("assigned", 0)),
        "conflicts_409": d["conflicts_409"],
        "rate_limited_429": d["rate_limited_429"],
        "server_errors_5xx": d["server_errors_5xx"],
        "transport_errors": d["transport_errors"],
        "faults": {
            k.removeprefix("fault_"): v for k, v in d["events"].items() if k.startswith("fault_")
        },
        "events": d["events"],
        "calls": d["calls"],
        "event_loop_lag": d["event_loop_lag"],
    }


def new_report(**sections: Any) -> dict[str, Any]:
    unknown = set(sections) - set(SECTIONS)
    if unknown:
        raise ValueError(f"unknown report sections: {sorted(unknown)}")
    return {
        "schema": SCHEMA,
        "generated_at": datetime.now(UTC).isoformat(),
        **{k: v for k, v in sections.items() if v is not None},
    }


def merge(parts: list[dict[str, Any]]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for p in parts:
        if p.get("schema") != SCHEMA:
            raise ValueError(f"not a {SCHEMA} document")
        out.update({k: v for k, v in p.items() if k in SECTIONS})
    return new_report(**out)


def write_json(path: Path, doc: dict[str, Any]) -> None:
    path.write_text(json.dumps(doc, indent=2, sort_keys=True) + "\n")


def read_json(path: Path) -> dict[str, Any]:
    doc = json.loads(path.read_text())
    if not isinstance(doc, dict):
        raise ValueError("report must be a JSON object")
    return doc


def summary_lines(doc: dict[str, Any]) -> list[str]:
    lines: list[str] = []
    if (run := doc.get("run")) is not None:
        lines.append(
            f"run: {run['completed']} completed in {run['duration_s']} s "
            f"({run['throughput_per_s']}/s) with {run['sims']} sims over {run['procs']} proc(s)"
        )
        lines.append(
            f"     409={run['conflicts_409']} 429={run['rate_limited_429']} "
            f"5xx={run['server_errors_5xx']} transport={run['transport_errors']} "
            f"faults={run['faults']}"
        )
        for op, c in run["calls"].items():
            lines.append(
                f"     {op:<17} n={c['count']:<7} p50={c['p50_s'] * 1000:.0f} ms "
                f"p99={c['p99_s'] * 1000:.0f} ms max={c['max_s'] * 1000:.0f} ms"
            )
        lag = run["event_loop_lag"]
        lines.append(
            f"     loop lag p99={lag['p99_s'] * 1000:.0f} ms max={lag['max_s'] * 1000:.0f} ms"
        )
    for e in doc.get("explain", []):
        lines.append(
            f"explain {e['name']}: exec={e['execution_ms']} ms plan={e['planning_ms']} ms "
            f"seq_scan={e['seq_scan']}"
        )
    if (v := doc.get("verify")) is not None:
        failed = ", ".join(c["name"] for c in v["checks"] if not c["ok"])
        lines.append("verify: PASS" if v["ok"] else f"verify: FAIL {failed}")
    return lines
