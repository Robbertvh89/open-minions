"""`minion-executor bench` with the fake engine (CI-safe: no models, no Metal)."""

import asyncio
import json
import os
from pathlib import Path
from typing import Any

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
    assert by_id["f"].status == "unavailable"


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
        bench.BenchConfig(records=bench.MAX_RECORDS + 1)
    with pytest.raises(ValueError):
        bench.BenchConfig(batch_sizes=(5000,))
    with pytest.raises(ValueError):
        bench.parse_batch_sizes("0")
    with pytest.raises(ValueError):
        bench.parse_batch_sizes("4097")
    with pytest.raises(ValueError):
        bench.parse_batch_sizes("1,x")
    assert bench.parse_batch_sizes("1, 8,32") == (1, 8, 32)


def test_cli_bench_with_fake_engine(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    models = tmp_path / "m.yaml"
    models.write_text(
        "models:\n  - {model_id: f, type: embedding, engine: fake, ref: fake, memory_gb: 0.1}\n"
    )
    import minion_executor.main as m

    monkeypatch.setattr(m, "restrict_environ", lambda: None)  # would wipe the pytest env
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


@pytest.mark.parametrize(
    "text",
    [
        "models: [unclosed",
        "- a\n- b\n",
        "models: 3\n",
        "models: {a: b}\n",
        "pending: 3\n",
        "models:\n  - 5\n",
        "pending:\n  - {model_id: x}\n",
        "pending:\n  - 5\n",
        "pending:\n  - {model_id: x, type: nope, engine: ollama, ref: r}\n",
    ],
)
def test_load_bench_models_rejects_bad_files(tmp_path: Path, text: str) -> None:
    f = tmp_path / "b.yaml"
    f.write_text(text)
    with pytest.raises(ValueError):
        bench.load_bench_models(f)


def test_embedding_tokens_counted_as_prompt_tokens(tmp_path: Path) -> None:
    class Tokens(WorkerHandle):
        def run(self, records: list[Any], params: Any) -> list[Any]:  # type: ignore[override]
            return [{"id": r["id"], "embedding": [0.0], "tokens": 7} for r in records]

    cache = ModelCache(tmp_path / "cache", 10**9, sandbox=False)
    report = asyncio.run(
        bench.run_bench(
            cache,
            lambda e: Tokens([e]),
            [fake_entry(TaskType.EMBEDDING)],
            CONFIG,
            BUDGET,
            environment={},
        )
    )
    for r in report.results:
        assert r.prompt_tokens == 7 * r.records and r.prompt_tok_s is not None
        assert r.completion_tokens == 0 and r.tok_s is None


def test_probe_failure_is_reported(tmp_path: Path) -> None:
    from minion_executor.runners.base import RunnerError
    from minion_executor.worker.process import WorkerCrashed

    for exc in (RunnerError("boom", "x"), WorkerCrashed("died")):

        class Failing(WorkerHandle):
            def probe(self, timeout_s: float = 90.0, exc: Exception = exc) -> dict[str, str | None]:
                raise exc

        cache = ModelCache(tmp_path / "cache", 10**9, sandbox=False)
        report = asyncio.run(
            bench.run_bench(
                cache,
                lambda e: Failing([e]),
                [fake_entry(TaskType.COMPLETION)],
                CONFIG,
                BUDGET,
                environment={},
            )
        )
        assert report.results[0].status == "unavailable"
        assert "probe failed" in report.results[0].detail


def test_all_records_failing_is_error_status(tmp_path: Path) -> None:
    class Erroring(WorkerHandle):
        def run(self, records: list[Any], params: Any) -> list[Any]:  # type: ignore[override]
            return [{"id": r["id"], "error": "boom"} for r in records]

    cache = ModelCache(tmp_path / "cache", 10**9, sandbox=False)
    report = asyncio.run(
        bench.run_bench(
            cache,
            lambda e: Erroring([e]),
            [fake_entry(TaskType.COMPLETION)],
            CONFIG,
            BUDGET,
            environment={},
        )
    )
    assert all(r.status == "error" and r.error_records == r.records for r in report.results)
    table = bench.render_table(report)
    assert "| errors |" in table and "| error (every record failed) | 10 |" in table


def test_failed_records_do_not_count_toward_rates(tmp_path: Path) -> None:
    class Mixed(WorkerHandle):
        def run(self, records: list[Any], params: Any) -> list[Any]:  # type: ignore[override]
            return [
                {"id": r["id"], "error": "boom", "prompt_tokens": 7, "completion_tokens": 9}
                if i % 2
                else {"id": r["id"], "prompt_tokens": 3, "completion_tokens": 5}
                for i, r in enumerate(records)
            ]

    class Failing(WorkerHandle):
        def run(self, records: list[Any], params: Any) -> list[Any]:  # type: ignore[override]
            return [
                {"id": r["id"], "error": "boom", "prompt_tokens": 7, "completion_tokens": 9}
                for r in records
            ]

    def go(handle: type[WorkerHandle]) -> bench.BenchResult:
        cache = ModelCache(tmp_path / "cache", 10**9, sandbox=False)
        report = asyncio.run(
            bench.run_bench(
                cache,
                lambda e: handle([e]),
                [fake_entry(TaskType.COMPLETION)],
                bench.BenchConfig(records=10, batch_sizes=(2,), max_tokens=8),
                BUDGET,
                environment={},
            )
        )
        return report.results[0]

    mixed = go(Mixed)
    assert mixed.status == "ok" and mixed.error_records == 5
    assert (mixed.prompt_tokens, mixed.completion_tokens) == (15, 25)
    failing = go(Failing)
    assert failing.status == "error"
    assert (failing.prompt_tokens, failing.completion_tokens) == (0, 0)
    assert failing.tok_s is None and failing.prompt_tok_s is None
    assert failing.records_per_h is None


def test_cache_oserror_is_reported(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    async def boom(self: ModelCache, model: ModelEntry) -> None:
        raise PermissionError("nope")

    monkeypatch.setattr(ModelCache, "ensure", boom)
    report = run(tmp_path, [fake_entry(TaskType.COMPLETION)])
    assert report.results[0].status == "error"
    assert "model fetch failed: PermissionError" in report.results[0].detail


def _cli_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, memory_gb: float = 0.1) -> Path:
    import minion_executor.main as m

    monkeypatch.setattr(m, "restrict_environ", lambda: None)
    models = tmp_path / "m.yaml"
    models.write_text(
        "models:\n  - {model_id: f, type: embedding, engine: fake, ref: fake, "
        f"memory_gb: {memory_gb}}}\n"
    )
    monkeypatch.setenv("MINION_EXECUTOR_SANDBOX", "0")
    monkeypatch.setenv("MINION_EXECUTOR_FETCH_SANDBOX", "0")
    monkeypatch.setenv("MINION_EXECUTOR_MODEL_CACHE_DIR", str(tmp_path / "cache"))
    return models


def test_cli_exit_1_when_no_row_ok(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    models = _cli_env(tmp_path, monkeypatch, memory_gb=1000)
    result = CliRunner().invoke(
        app, ["bench", "--models", str(models), "--records", "2", "--batch-sizes", "1"]
    )
    assert result.exit_code == 1, result.output


def test_cli_out_validated_before_run(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    models = _cli_env(tmp_path, monkeypatch)
    target = tmp_path / "real.json"
    target.write_text("x")
    link = tmp_path / "link.json"
    link.symlink_to(target)
    a_dir = tmp_path / "adir"
    a_dir.mkdir()
    cases = [
        (link, "must not be a symlink"),
        (tmp_path / "nodir" / "o.json", "not a writable directory"),
        (a_dir, "must be a regular file"),
    ]
    for bad, text in cases:
        result = CliRunner().invoke(app, ["bench", "--models", str(models), "--out", str(bad)])
        assert result.exit_code == 2, result.output
        assert text in result.output
    assert target.read_text() == "x"


def test_cli_out_overwrites_existing_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    models = _cli_env(tmp_path, monkeypatch)
    out = tmp_path / "o.json"
    out.write_text("old content that is long" * 1000)
    result = CliRunner().invoke(
        app,
        [
            "bench",
            "--models",
            str(models),
            "--records",
            "2",
            "--batch-sizes",
            "1",
            "--out",
            str(out),
        ],
    )
    assert result.exit_code == 0, result.output
    data = json.loads(out.read_text())
    assert data["results"][0]["model_id"] == "f"


def test_write_out_refuses_fifo(tmp_path: Path) -> None:
    import minion_executor.main as m

    fifo = tmp_path / "fifo"
    os.mkfifo(fifo)
    reader = os.open(fifo, os.O_RDONLY | os.O_NONBLOCK)
    try:
        with pytest.raises(OSError, match="regular file"):
            m._write_out(fifo, "x")
    finally:
        os.close(reader)
