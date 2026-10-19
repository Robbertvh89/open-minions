"""In-process IOKit GPU client reader (pure parts everywhere, live parity on macOS)."""

from __future__ import annotations

import os
import resource
import subprocess
import sys

import pytest
from minion_executor.capacity import gpu_meter
from minion_executor.capacity.gpu_meter import (
    GpuMeterError,
    IokitGpuReader,
    RawClient,
    clients_by_pid,
    default_gpu_reader,
    parse_gpu_clients,
    pid_from_creator,
)


def test_pid_from_creator() -> None:
    assert pid_from_creator("pid 123, python3") == 123
    assert pid_from_creator("pid 7,x") == 7
    for bad in (None, "", "pid x, a", "pid 123 python", "xpid 1, a", "pid 12345678901, a"):
        assert pid_from_creator(bad) is None


def test_clients_by_pid_shape_and_sums() -> None:
    raw = [
        RawClient(0x1A, "pid 10, a", (5, 7)),
        RawClient(0x1B, "pid 10, a", ()),
        RawClient(0x2, "pid 11, b", (-4,)),
        RawClient(0x3, None, (9,)),
        RawClient(0x4, "garbage", (9,)),
    ]
    assert clients_by_pid(raw) == {10: {"0x1a": 12, "0x1b": 0}, 11: {"0x2": 0}}


def test_same_shape_as_ioreg_parser() -> None:
    text = (
        "+-o AGXAcceleratorG15X  <class AGXAcceleratorG15X>\n"
        "  +-o AGXDeviceUserClient  <class AGXDeviceUserClient, id 0x1a, registered>\n"
        '    {\n      "IOUserClientCreator" = "pid 10, a"\n'
        '      "AppUsage" = ({"API"="Metal","accumulatedGPUTime"=5},'
        '{"API"="Metal","accumulatedGPUTime"=7})\n    }\n'
    )
    raw = [RawClient(0x1A, "pid 10, a", (5, 7))]
    assert clients_by_pid(raw) == parse_gpu_clients(text)


def test_reader_errors_are_loud() -> None:
    with pytest.raises(GpuMeterError):
        IokitGpuReader(lambda: [])()
    with pytest.raises(GpuMeterError, match="creator"):
        IokitGpuReader(lambda: [RawClient(1, None, (3,))])()
    ok = IokitGpuReader(lambda: [RawClient(1, "pid 2, x", (3,))])
    assert ok() == {2: {"0x1": 3}}


def test_reader_propagates_backend_failure() -> None:
    def boom() -> list[RawClient]:
        raise GpuMeterError("IOKit reader: no IOAccelerator service (no Apple GPU?)")

    with pytest.raises(GpuMeterError):
        IokitGpuReader(boom)()


@pytest.mark.skipif(sys.platform == "darwin", reason="off-darwin behaviour")
def test_default_reader_none_off_darwin() -> None:
    assert default_gpu_reader() is None


def test_missing_library_is_meter_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(gpu_meter, "IOKIT_PATH", "/nonexistent/IOKit")
    with pytest.raises(GpuMeterError, match="IOKit"):
        gpu_meter._IokitApi()


def _agx_present() -> bool:
    try:
        gpu_meter.read_gpu_clients()
    except GpuMeterError:
        return False
    return True


@pytest.mark.macos
@pytest.mark.skipif(sys.platform != "darwin", reason="IOKit")
def test_live_parity_with_ioreg_and_no_port_leak() -> None:
    if not _agx_present():
        pytest.skip("no AGX user clients")
    reader = default_gpu_reader()
    assert reader is not None
    child = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(5)"], stdout=subprocess.DEVNULL
    )
    try:
        via_ioreg = gpu_meter.read_gpu_clients()
        via_iokit = reader()
    finally:
        child.kill()
        child.wait()
    shared = set(via_ioreg) & set(via_iokit)
    assert shared, "no pid seen by both readers"
    for pid in shared:
        assert all(c.startswith("0x") for c in via_iokit[pid])
    for _ in range(5):
        reader()
    before = _mach_ports()
    t0 = resource.getrusage(resource.RUSAGE_SELF)
    for _ in range(200):
        reader()
    t1 = resource.getrusage(resource.RUSAGE_SELF)
    assert _mach_ports() <= before + 2
    cpu_ms = ((t1.ru_utime + t1.ru_stime) - (t0.ru_utime + t0.ru_stime)) * 1000 / 200
    assert cpu_ms < 20  # measured ~1 ms; ioreg costs ~15 ms in a child


def _mach_ports() -> int:
    """Number of mach port entries of this process (`lsmp`)."""
    out = subprocess.run(  # noqa: S603 - fixed argv
        ["/usr/bin/lsmp", "-p", str(os.getpid())], capture_output=True, text=True, timeout=30
    ).stdout
    return sum(1 for line in out.splitlines() if line.strip().startswith("0x"))
