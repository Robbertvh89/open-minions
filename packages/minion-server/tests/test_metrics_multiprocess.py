"""/metrics sums the samples of all worker processes (TASK-7.4.6, prometheus multiprocess mode)."""

import os
from pathlib import Path

from prometheus_client.parser import text_string_to_metric_families

_CHILD = """
import sys
from minion_server.metrics import Metrics
m = Metrics()
for _ in range(int(sys.argv[1])):
    m.observe("/x", "GET", 200)
m.observe_acquire("assigned", 0.01)
"""


def _spawn(directory: Path, n: int) -> None:
    import subprocess
    import sys

    env = {**os.environ, "PROMETHEUS_MULTIPROC_DIR": str(directory)}
    subprocess.run([sys.executable, "-c", _CHILD, str(n)], env=env, check=True)  # noqa: S603


def test_render_sums_counters_and_histograms_across_processes(
    tmp_path: Path, monkeypatch: object
) -> None:
    import pytest

    assert isinstance(monkeypatch, pytest.MonkeyPatch)
    _spawn(tmp_path, 3)
    _spawn(tmp_path, 4)  # two separate processes (pids), both gone by the time we render
    assert len(list(tmp_path.glob("*.db"))) >= 4
    monkeypatch.setenv("PROMETHEUS_MULTIPROC_DIR", str(tmp_path))
    from minion_server.metrics import Metrics

    text = Metrics().render().decode()
    samples = {
        (s.name, tuple(sorted(s.labels.items()))): s.value
        for fam in text_string_to_metric_families(text)
        for s in fam.samples
    }
    key = ("minion_http_requests_total", (("method", "GET"), ("route", "/x"), ("status", "200")))
    assert samples[key] == 7
    assert samples[("minion_acquire_seconds_count", (("outcome", "assigned"),))] == 2
