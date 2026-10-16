"""`minion-executor bench` with the fake engine (CI-safe: no models, no Metal)."""

import asyncio
import json
from pathlib import Path

import pytest
from minion_common.enums import CapacityProfile, Engine, TaskType
from minion_common.schemas import Budget, ModelEntry
from minion_executor import bench
from minion_executor.main import app
from minion_executor.model_cache import ModelCache
from minion_executor.worker.process import WorkerHandle
from typer.testing import CliRunner

BUDGET = Budget(profile=CapacityProfile.FULL, memory_gb=8, gpu_duty_cycle=1.0, max_batch_size=32)
CONFIG = bench.BenchConfig(records=10, batch_sizes=(1, 4), max_tokens=8)
RESULT_KEYS = {
    "model_id",
    "engine",
    "type",
    "status",
    "detail",
    "batch_size",
    "records",
    "error_records",
    "load_s",
    "run_s",
    "prompt_tokens",
    "completion_tokens",
    "tok_s",
    "prompt_tok_s",
    "records_per_h",
    "engine_peak_gb",
    "worker_rss_peak_gb",
    "engine_version",
}


def fake_entry(type_: TaskType, model_id: str = "m") -> ModelEntry:
    return ModelEntry(model_id=model_id, type=type_, engine=Engine.FAKE, ref="fake", memory_gb=0.1)


def run(
    tmp_path: Path, entries: list[ModelEntry], pending: list[bench.PendingModel] | None = None
) -> bench.BenchReport:
    cache = ModelCache(tmp_path / "cache", 10**9, sandbox=False)

    def factory(engine: Engine) -> WorkerHandle:
        return WorkerHandle([engine])

    return asyncio.run(
        bench.run_bench(
            cache,
            factory,
            entries,
            CONFIG,
            BUDGET,
            pending=pending or [],
            environment={"chip": "test"},
        )
    )


@pytest.fixture(autouse=True)
def dev(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MINION_EXECUTOR_DEV", "1")


def test_synthetic_records_deterministic() -> None:
    a = bench.synthetic_records(TaskType.COMPLETION, 20, 7)
    assert a == bench.synthetic_records(TaskType.COMPLETION, 20, 7)
    assert a != bench.synthetic_records(TaskType.COMPLETION, 20, 8)
    assert bench.synthetic_records(TaskType.COMPLETION, 5, 7) == a[:5]
    assert all(set(r) == {"id", "prompt"} for r in a)
    emb = bench.synthetic_records(TaskType.EMBEDDING, 3, 0)
    assert all(set(r) == {"id", "text"} for r in emb)


def test_rates_math() -> None:
    tok_s, prompt_tok_s, per_h = bench.rates(50, 20.0, 400, 1000)
    assert tok_s == 50.0
    assert prompt_tok_s == 20.0
    assert per_h == 9000.0
    assert bench.rates(5, 0.0, 1, 1) == (None, None, None)
    assert bench.rates(5, 1.0, 0, 0)[:2] == (None, None)


def test_fake_engine_reports_every_metric(tmp_path: Path) -> None:
    report = run(
        tmp_path, [fake_entry(TaskType.COMPLETION, "c"), fake_entry(TaskType.EMBEDDING, "e")]
    )
    assert [(r.model_id, r.batch_size) for r in report.results] == [
        ("c", 1),
        ("c", 4),
        ("e", 1),
        ("e", 4),
    ]
    for r in report.results:
        assert r.status == "ok"
        assert r.records == CONFIG.records
        assert r.load_s is not None and r.load_s >= 0
        assert r.run_s is not None and r.run_s > 0
        assert r.records_per_h == pytest.approx(r.records / r.run_s * 3600)
        assert r.engine_peak_gb is not None
        assert r.worker_rss_peak_gb is not None and r.worker_rss_peak_gb > 0
        assert r.engine_version == "fake"
    table = bench.render_table(report)
    assert "| c | fake | completion | 4 | ok |" in table
    assert table.count("\n") == 5  # header, rule, 4 rows


def test_unavailable_engine_is_reported_not_raised(tmp_path: Path) -> None:
    ollama = ModelEntry(
        model_id="o",
        type=TaskType.COMPLETION,
        engine=Engine.OLLAMA,
        ref="qwen2.5:0.5b",
        revision="a" * 64,
        memory_gb=1,
    )
    pending = [
        bench.PendingModel("p", TaskType.EMBEDDING, Engine.OLLAMA, "all-minilm:l6-v2", "not pinned")
    ]

    class Unavailable(WorkerHandle):
        def probe(self, timeout_s: float = 90.0) -> dict[str, str | None]:
            return {"ollama": None}

    cache = ModelCache(tmp_path / "cache", 10**9, sandbox=False)
    report = asyncio.run(
        bench.run_bench(
            cache,
            lambda e: Unavailable([e]),
            [ollama, fake_entry(TaskType.EMBEDDING, "f")],
            CONFIG,
            BUDGET,
            pending=pending,
            environment={},
        )
    )
    by_id = {r.model_id: r for r in report.results}
    assert by_id["o"].status == "unavailable" and by_id["o"].batch_size is None
    assert by_id["p"].status == "unavailable" and "not pinned" in by_id["p"].detail
    assert "unavailable" in bench.render_table(report)
    assert report.results[0].records_per_h is None


def test_model_failure_is_reported_not_raised(tmp_path: Path) -> None:
    big = ModelEntry(
        model_id="big", type=TaskType.COMPLETION, engine=Engine.FAKE, ref="f", memory_gb=100
    )
    report = run(tmp_path, [big, fake_entry(TaskType.COMPLETION, "ok")])
    errors = [r for r in report.results if r.model_id == "big"]
    assert errors and all(r.status == "error" for r in errors)
    assert all(r.status == "ok" for r in report.results if r.model_id == "ok")


def test_json_schema_and_no_payload(tmp_path: Path) -> None:
    report = run(tmp_path, [fake_entry(TaskType.COMPLETION)])
    doc = json.loads(json.dumps(report.to_json()))
    assert doc["schema_version"] == bench.SCHEMA_VERSION
    assert set(doc) == {"schema_version", "environment", "config", "results"}
    assert doc["config"] == {"records": 10, "batch_sizes": [1, 4], "max_tokens": 8, "seed": 0}
    assert all(set(r) == RESULT_KEYS for r in doc["results"])
    assert "essay" not in json.dumps(doc)  # synthetic prompt text never leaves the worker path


def test_load_bench_models_default_file() -> None:
    root = Path(__file__).resolve().parents[3]
    entries, pending = bench.load_bench_models(root / "deploy" / "bench-models.yaml")
    assert {e.model_id for e in entries} >= {"qwen2.5-0.5b-mlx", "minilm-hf"}
    assert {p.engine for p in pending} == {Engine.OLLAMA}


def test_config_validation() -> None:
    with pytest.raises(ValueError):
        bench.BenchConfig(records=0)
    with pytest.raises(ValueError):
        bench.parse_batch_sizes("1,x")
    assert bench.parse_batch_sizes("1, 8,32") == (1, 8, 32)


def test_cli_bench_with_fake_engine(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    models = tmp_path / "m.yaml"
    models.write_text(
        "models:\n  - {model_id: f, type: embedding, engine: fake, ref: fake, memory_gb: 0.1}\n"
    )
    monkeypatch.setenv("MINION_EXECUTOR_SANDBOX", "0")
    monkeypatch.setenv("MINION_EXECUTOR_FETCH_SANDBOX", "0")
    monkeypatch.setenv("MINION_EXECUTOR_MODEL_CACHE_DIR", str(tmp_path / "cache"))
    out = tmp_path / "out.json"
    result = CliRunner().invoke(
        app,
        [
            "bench",
            "--models",
            str(models),
            "--records",
            "6",
            "--batch-sizes",
            "2",
            "--out",
            str(out),
        ],
    )
    assert result.exit_code == 0, result.output
    assert "| f | fake | embedding | 2 | ok |" in result.output
    assert json.loads(out.read_text())["results"][0]["records"] == 6


def test_cli_bench_unknown_model(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[3]
    result = CliRunner().invoke(
        app, ["bench", "--models", str(root / "deploy" / "bench-models.yaml"), "--model", "nope"]
    )
    assert result.exit_code == 2
