import random

from pulsestream.common.backoff import base_delay_for, delay_for


def test_delay_for_varies() -> None:
    values = [delay_for(3) for _ in range(20)]
    assert len(set(values)) > 1


def test_delay_within_bounds() -> None:
    for attempt in range(20):
        assert base_delay_for(attempt) / 2 <= delay_for(attempt) <= base_delay_for(attempt)


def test_base_delay_for_doubles_and_caps() -> None:
    assert base_delay_for(0) == 1.0
    assert base_delay_for(3) == 8.0
    assert base_delay_for(4) == 16.0
    assert base_delay_for(5) == 30.0
    assert base_delay_for(10) == 30.0
    assert base_delay_for(20) == 30.0
    assert base_delay_for(100) == 30.0


def test_delay_for_is_reproducible_with_a_seeded_rng() -> None:
    assert delay_for(2, rng=random.Random(7)) == delay_for(2, rng=random.Random(7))
