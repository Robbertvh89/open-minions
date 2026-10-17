"""executor.toml loading and profile resolution (TASK-5.1, SPEC FR-51/52/53, ADR-0006 D9)."""

import os
from pathlib import Path

import pytest
from minion_common.enums import CapacityProfile, CpuPriority
from minion_executor.capacity.config import (
    ConfigError,
    ExecutorConfig,
    load_config,
    resolve_profiles,
)
from minion_executor.capacity.profiles import (
    PROFILES,
    budget_for,
    nice_for,
    parse_profile,
)

FULL_TOML = """
active_profile = "light"
idle_profile = "custom"
idle_after_min = 5
allow_on_battery = true
min_battery_percent = 40
on_call = "pause"

[profiles.light]
gpu_duty_cycle = 0.2

[profiles.balanced]
memory_gb = 12
cpu_priority = "background"

[profiles.custom]
memory_fraction = 0.6
gpu_duty_cycle = 0.8
max_batch_size = 16
cpu_priority = "normal"

[[schedule]]
profile = "full"
start = "19:00"
end = "07:00"
days = ["mon", "tue"]

[[schedule]]
profile = "custom"
start = "12:00"
end = "13:30"
"""


def _write(tmp_path: Path, text: str, mode: int = 0o600) -> Path:
    f = tmp_path / "executor.toml"
    f.write_text(text)
    f.chmod(mode)
    return f


def test_defaults_when_default_file_absent(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "minion_executor.capacity.config.default_config_path", lambda: tmp_path / "absent.toml"
    )
    cfg = load_config(None)
    assert cfg == ExecutorConfig()
    assert (cfg.active_profile, cfg.idle_profile, cfg.idle_after_min) == (
        CapacityProfile.LIGHT,
        CapacityProfile.FULL,
        10,
    )
    assert (cfg.allow_on_battery, cfg.min_battery_percent) == (False, 30)
    assert cfg.on_call == "light"


def test_explicit_missing_file_is_an_error(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="cannot read"):
        load_config(tmp_path / "nope.toml")


def test_full_config_parses_and_resolves(tmp_path: Path) -> None:
    cfg = load_config(_write(tmp_path, FULL_TOML))
    assert cfg.idle_profile is CapacityProfile.CUSTOM
    assert cfg.idle_after_min == 5 and cfg.allow_on_battery and cfg.min_battery_percent == 40
    assert cfg.on_call == "pause"
    assert [s.profile for s in cfg.schedule] == [CapacityProfile.FULL, CapacityProfile.CUSTOM]
    assert cfg.schedule[0].start_time.hour == 19 and cfg.schedule[0].end_time.hour == 7
    assert cfg.schedule[0].days == ["mon", "tue"] and cfg.schedule[1].days is None

    specs = resolve_profiles(cfg)
    assert specs[CapacityProfile.LIGHT].gpu_duty_cycle == 0.2
    assert (
        specs[CapacityProfile.LIGHT].max_batch_size
        == PROFILES[CapacityProfile.LIGHT].max_batch_size
    )
    bal = specs[CapacityProfile.BALANCED]
    assert (bal.memory_gb, bal.memory_fraction, bal.cpu_priority) == (
        12,
        None,
        CpuPriority.BACKGROUND,
    )
    assert specs[CapacityProfile.FULL] == PROFILES[CapacityProfile.FULL]
    b = budget_for(CapacityProfile.CUSTOM, total_gb=32, specs=specs)
    assert (b.memory_gb, b.gpu_duty_cycle, b.max_batch_size, b.cpu_priority) == (
        19.2,
        0.8,
        16,
        CpuPriority.NORMAL,
    )


def test_ram_cap_always_applies(tmp_path: Path) -> None:
    cfg = load_config(
        _write(
            tmp_path,
            "[profiles.full]\nmemory_fraction = 1.0\n[profiles.balanced]\nmemory_gb = 500\n",
        )
    )
    specs = resolve_profiles(cfg)
    assert budget_for(CapacityProfile.FULL, total_gb=64, specs=specs).memory_gb == 48
    assert budget_for(CapacityProfile.BALANCED, total_gb=64, specs=specs).memory_gb == 48
    # built-ins are capped too
    assert budget_for(CapacityProfile.FULL, total_gb=64).memory_gb == pytest.approx(44.8)


@pytest.mark.parametrize(
    "text",
    [
        "surprise = 1",
        "[profiles.light]\nsurprise = 1",
        "[profiles.turbo]\ngpu_duty_cycle = 0.5",
        "[[schedule]]\nprofile = 'full'\nstart = '1:00'\nend = '07:00'",
        "[[schedule]]\nprofile = 'full'\nstart = '25:00'\nend = '07:00'",
        "[[schedule]]\nprofile = 'full'\nstart = '07:00'\nend = '07:00'",
        "[[schedule]]\nprofile = 'full'\nstart = '01:00'\nend = '07:00'\ndays = ['funday']",
        "[[schedule]]\nprofile = 'full'\nstart = '01:00'\nend = '07:00'\ndays = []",
        "[[schedule]]\nprofile = 'full'\nstart = '01:00'\nend = '07:00'\nextra = 1",
        "[[schedule]]\nprofile = 'custom'\nstart = '01:00'\nend = '07:00'",  # custom undefined
        "idle_profile = 'custom'",
        "active_profile = 'turbo'",
        "idle_after_min = 0",
        "idle_after_min = inf",
        "min_battery_percent = 101",
        "allow_on_battery = 'yes'",
        "[profiles.light]\ngpu_duty_cycle = 0",
        "[profiles.light]\ngpu_duty_cycle = 1.5",
        "[profiles.light]\nmax_batch_size = 0",
        "[profiles.light]\nmemory_fraction = 0.3\nmemory_gb = 4",
        "[profiles.light]\ncpu_priority = 'turbo'",
        "[profiles.custom]\nmemory_gb = 4\ngpu_duty_cycle = 0.5\nmax_batch_size = 2",
        "[profiles.custom]\ngpu_duty_cycle = 0.5\nmax_batch_size = 2\ncpu_priority = 'low'",
        "this is not toml [",
    ],
)
def test_invalid_values_rejected(tmp_path: Path, text: str) -> None:
    with pytest.raises(ConfigError):
        load_config(_write(tmp_path, text))


def test_error_does_not_echo_values(tmp_path: Path) -> None:
    with pytest.raises(ConfigError) as ei:
        load_config(_write(tmp_path, "idle_after_min = 'hunter2-secret'"))
    assert "hunter2" not in str(ei.value)


@pytest.mark.parametrize("mode", [0o666, 0o662, 0o620, 0o602, 0o660])
def test_group_or_world_writable_refused(tmp_path: Path, mode: int) -> None:
    with pytest.raises(ConfigError, match="writable"):
        load_config(_write(tmp_path, "", mode))


@pytest.mark.parametrize("mode", [0o600, 0o644, 0o400])
def test_safe_modes_accepted(tmp_path: Path, mode: int) -> None:
    load_config(_write(tmp_path, "", mode))


def test_group_writable_directory_refused(tmp_path: Path) -> None:
    d = tmp_path / "d"
    d.mkdir()
    f = d / "executor.toml"
    f.write_text("")
    f.chmod(0o600)
    d.chmod(0o775)
    try:
        with pytest.raises(ConfigError, match="directory"):
            load_config(f)
    finally:
        d.chmod(0o700)


def test_not_owned_by_user_refused(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    f = _write(tmp_path, "")
    monkeypatch.setattr(os, "geteuid", lambda: f.stat().st_uid + 1)
    with pytest.raises(ConfigError, match="not owned"):
        load_config(f)


def test_non_regular_and_oversized_refused(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="regular file"):
        load_config(tmp_path)  # a directory
    big = _write(tmp_path, "# " + "x" * 70_000)
    with pytest.raises(ConfigError, match="larger"):
        load_config(big)


def test_symlink_to_safe_file_is_followed_but_checked(tmp_path: Path) -> None:
    real = _write(tmp_path, "idle_after_min = 7")
    link = tmp_path / "link.toml"
    link.symlink_to(real)
    assert load_config(link).idle_after_min == 7
    real.chmod(0o666)
    with pytest.raises(ConfigError, match="writable"):
        load_config(link)


def test_profiles_without_config_unchanged() -> None:
    assert resolve_profiles(ExecutorConfig()) == PROFILES
    b = budget_for(CapacityProfile.BALANCED, total_gb=64)
    assert (b.memory_gb, b.gpu_duty_cycle, b.max_batch_size) == (25.6, 0.5, 8)
    assert b.cpu_priority is CpuPriority.LOW


def test_parse_profile_custom_only_when_defined(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="not defined"):
        parse_profile("custom")
    specs = resolve_profiles(load_config(_write(tmp_path, FULL_TOML)))
    assert parse_profile("custom", specs) is CapacityProfile.CUSTOM
    with pytest.raises(ValueError, match="unknown"):
        parse_profile("bogus", specs)


def test_nice_derived_from_cpu_priority() -> None:
    assert nice_for(CpuPriority.BACKGROUND) == 15
    assert nice_for(CpuPriority.LOW) == 10
    assert nice_for(CpuPriority.NORMAL) == 0
    assert nice_for(None) == 10  # unknown counts as LOW (conservative)
    # built-ins keep the M1 values
    assert [nice_for(PROFILES[p].cpu_priority) for p in PROFILES] == [15, 10, 0]


def test_deeply_nested_toml_is_a_config_error(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="nesting too deep"):
        load_config(_write(tmp_path, "a = " + "[" * 30_000 + "]" * 30_000))


def test_unknown_day_error_does_not_echo_value(tmp_path: Path) -> None:
    text = "[[schedule]]\nprofile = 'full'\nstart = '01:00'\nend = '07:00'\ndays = ['hunter2']"
    with pytest.raises(ConfigError, match="unknown day; use mon") as ei:
        load_config(_write(tmp_path, text))
    assert "hunter2" not in str(ei.value)


def test_symlink_target_directory_is_checked(tmp_path: Path) -> None:
    safe = tmp_path / "safe"
    safe.mkdir()
    loose = tmp_path / "loose"
    loose.mkdir()
    real = loose / "executor.toml"
    real.write_text("")
    real.chmod(0o600)
    link = safe / "executor.toml"
    link.symlink_to(real)
    loose.chmod(0o775)
    try:
        with pytest.raises(ConfigError, match="directory"):
            load_config(link)
    finally:
        loose.chmod(0o700)
