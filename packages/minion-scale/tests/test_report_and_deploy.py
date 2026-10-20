"""Report assembly, EXPLAIN statements and the scale compose overlay (TASK-7.2 c)."""

import json
from pathlib import Path
from typing import Any

import pytest
import yaml
from minion_scale import report
from minion_scale.explain import (
    GRAFANA_STATEMENTS,
    compile_generic,
    compile_literal,
    hot_statements,
    parse_plan,
)
from minion_scale.metrics import Recorder
from minion_server.repositories.completion import settleable_stmt

from conftest import REPO_ROOT

DOC = REPO_ROOT / "docs" / "bench" / "scale-report-schema.md"
COMPOSE = REPO_ROOT / "deploy" / "compose.scale.yml"


def _run_doc() -> dict[str, Any]:
    rec = Recorder()
    rec.record("acquire_assigned", 0.02, 200)
    rec.record("complete", 0.05, 409)
    rec.count("completed", 10)
    rec.count("fault_vanish", 2)
    return report.new_report(
        config={"server": report.server_label("http://user:pw@127.0.0.1:58080/x?k=v")},
        run=report.run_section(rec, duration_s=5.0, sims=3, procs=1, drained=True),
    )


def test_server_label_drops_credentials_and_paths() -> None:
    assert report.server_label("http://user:pw@127.0.0.1:58080/x?k=v") == "127.0.0.1:58080"
    assert report.server_label("https://example.com/api") == "example.com"


def test_run_section_derives_throughput_and_faults() -> None:
    run = _run_doc()["run"]
    assert run["completed"] == 10 and run["throughput_per_s"] == 2.0
    assert run["faults"] == {"vanish": 2} and run["conflicts_409"] == 1


def test_merge_combines_sections_and_rejects_foreign_documents(tmp_path: Path) -> None:
    verify = report.new_report(verify={"ok": True, "counts": {}, "checks": []})
    merged = report.merge([_run_doc(), verify])
    assert {"run", "config", "verify"} <= set(merged)
    path = tmp_path / "r.json"
    report.write_json(path, merged)
    assert json.loads(path.read_text())["schema"] == report.SCHEMA
    with pytest.raises(ValueError):
        report.merge([{"schema": "other"}])
    with pytest.raises(ValueError):
        report.new_report(bogus=1)
    assert any(ln.startswith("verify: PASS") for ln in report.summary_lines(merged))


def test_schema_doc_names_every_section_and_check() -> None:
    text = DOC.read_text()
    for section in report.SECTIONS:
        assert f"`{section}`" in text
    assert report.SCHEMA in text
    for check in (
        "nf2_one_succeeded_attempt_per_succeeded_subtask",
        "nf2_final_objects_match_succeeded",
        "nf3_all_subtasks_terminal",
        "nf3_no_orphan_acquirements",
        "task_counters_match_subtasks",
        "tasks_terminal",
    ):
        assert check in text


def test_hot_statements_compile_to_literal_sql() -> None:
    stmts = hot_statements(
        model_ids=["scale-fake"], team="scale", heartbeat_timeout_s=20.0, max_lease_s=3600.0
    )
    assert set(stmts) == {
        "candidate",
        "candidate_preference",
        "stale_heartbeat",
        "over_lease",
        "settleable_tasks",
        "grafana_task_status",
        "grafana_live_tasks",
        "grafana_subtask_status",
    }
    for name, stmt in stmts.items():
        sql = stmt if isinstance(stmt, str) else compile_literal(stmt)
        assert ":" not in sql.replace("::", "") and "%(" not in sql, name
    settle = compile_literal(settleable_stmt(100))
    assert "JOIN LATERAL" in settle and "LIMIT 100" in settle  # live-driven, TASK-34
    small = hot_statements(
        model_ids=["m"], team="t", heartbeat_timeout_s=1.0, max_lease_s=1.0, settle_batch=7
    )["settleable_tasks"]
    assert not isinstance(small, str) and "LIMIT 7" in compile_literal(small)
    pref, cand = stmts["candidate_preference"], stmts["candidate"]
    assert not isinstance(pref, str) and not isinstance(cand, str)
    assert "minion_task.team = 'scale'" in compile_literal(pref)
    assert "scale-fake" in compile_literal(cand)


def test_compile_generic_keeps_bind_parameters() -> None:
    stmts = hot_statements(
        model_ids=["scale-fake", "o'neil"], team="scale", heartbeat_timeout_s=20.0, max_lease_s=1.0
    )
    stmt = stmts["candidate_preference"]
    assert not isinstance(stmt, str)
    prepare, args = compile_generic(stmt)
    assert prepare.startswith("PREPARE q AS ") and "$1" in prepare
    assert "scale-fake" not in prepare and "'scale'" not in prepare  # values are bound
    assert "'scale-fake'" in args and "'o''neil'" in args and "'scale'" in args
    assert "LIMIT 16" in prepare  # window stays a literal (TASK-54)


def _raw_sqls(node: Any) -> list[str]:
    if isinstance(node, dict):
        found = [node["rawSql"]] if isinstance(node.get("rawSql"), str) else []
        return found + [x for v in node.values() for x in _raw_sqls(v)]
    if isinstance(node, list):
        return [x for v in node for x in _raw_sqls(v)]
    return []


def test_grafana_statements_match_dashboard() -> None:
    dash = json.loads((REPO_ROOT / "deploy/grafana/dashboards/tasks.json").read_text())
    panel_sql = {" ".join(s.split()) for s in _raw_sqls(dash)}
    for name, sql in GRAFANA_STATEMENTS.items():
        assert " ".join(sql.split()) in panel_sql, f"{name} drifted from tasks.json"


def test_parse_plan_extracts_timings() -> None:
    plan = [
        "Limit  (cost=0.1..1 rows=1)",
        "  Buffers: shared hit=12 read=3",
        "  ->  Seq Scan on t",
        "Planning Time: 0.250 ms",
        "Execution Time: 1.500 ms",
    ]
    p = parse_plan(plan)
    assert (p["planning_ms"], p["execution_ms"]) == (0.25, 1.5)
    assert p["top_buffers"] == "hit=12 read=3" and p["seq_scan"] is True


def _load(path: Path) -> dict[str, Any]:
    class Loader(yaml.SafeLoader):
        pass

    Loader.add_multi_constructor("!", lambda loader, _suffix, node: loader.construct_sequence(node))
    doc: dict[str, Any] = yaml.load(path.read_text(), Loader=Loader)  # noqa: S506 - SafeLoader
    return doc


def test_compose_scale_overlay() -> None:
    text = COMPOSE.read_text()
    assert text.startswith("# DEV ONLY")
    doc = _load(COMPOSE)
    assert doc["name"] == "open-minions-scale"  # never the dev stack's project (or its volumes)
    services = doc["services"]
    for name, svc in services.items():  # everything published is on loopback only
        for port in svc.get("ports", []):
            assert str(port).startswith("127.0.0.1:"), (name, port)
    assert {"postgres", "s3", "server", "grafana"} <= {
        n for n, v in services.items() if "ports" in v
    }
    cmd = " ".join(services["postgres"]["command"])
    assert "shared_preload_libraries=pg_stat_statements" in cmd and "shared_buffers=" in cmd
    env = services["watchdog"]["environment"]
    assert "MINION_HEARTBEAT_TIMEOUT_S" in env and "MINION_WATCHDOG_INTERVAL_S" in env
    models_mount = services["server"]["volumes"][0]
    assert models_mount.split(":")[0].endswith("scale/models.scale.yaml")
    # never the regular dev stack's host ports (8080 in particular)
    for svc in ("postgres", "s3", "server", "grafana"):
        for port in services[svc]["ports"]:
            assert not any(f":-{d}}}" in port for d in (8080, 5432, 9000, 3000)), port
