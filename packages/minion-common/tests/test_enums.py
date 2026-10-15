from minion_common.enums import CapacityProfile, Engine, SubtaskStatus


def test_default_engine_values_match_models_yaml_contract() -> None:
    assert {e.value for e in Engine} == {"mlx", "ollama", "hf", "fake"}


def test_subtask_status_is_str() -> None:
    assert SubtaskStatus.PENDING == "PENDING"


def test_capacity_profiles() -> None:
    assert [p.value for p in CapacityProfile] == ["light", "balanced", "full", "custom"]
