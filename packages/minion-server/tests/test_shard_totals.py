"""Pure grouping of cancelled subtasks into counter shards (TASK-16)."""

from minion_server.repositories.completion import TASK_COUNTER_SHARDS, shard_totals


def test_shard_totals_groups_and_sorts() -> None:
    keys = [
        ("t", "p", "b", 17),
        ("t", "p", "a", 1),
        ("t", "p", "a", 17),  # same shard as 1
        ("t", "p", "a", 2),
        ("t", "p", "b", 1),
    ]
    assert shard_totals(keys) == [
        (("t", "p", "a", 1), 2),
        (("t", "p", "a", 2), 1),
        (("t", "p", "b", 1), 2),
    ]


def test_shard_totals_empty_and_modulus() -> None:
    assert shard_totals([]) == []
    ((key, n),) = shard_totals([("t", "p", "a", TASK_COUNTER_SHARDS * 5 + 3)])
    assert key[3] == 3 and n == 1
