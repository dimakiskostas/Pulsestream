import pulsestream.sources.wikimedia as wikimedia


def test_delay_for_varies():
    values = [wikimedia.delay_for(3) for _ in range(20)]
    assert len(set(values)) > 1


def test_delay_within_bounds():
    for attempt in range(20):
        assert (
            wikimedia.base_delay_for(attempt) / 2
            <= wikimedia.delay_for(attempt)
            <= wikimedia.base_delay_for(attempt)
        )


def test_base_delay_for_doubles_and_caps():
    assert wikimedia.base_delay_for(0) == 1.0
    assert wikimedia.base_delay_for(3) == 8.0
    assert wikimedia.base_delay_for(4) == 16.0
    assert wikimedia.base_delay_for(5) == 30.0
    assert wikimedia.base_delay_for(10) == 30.0
    assert wikimedia.base_delay_for(20) == 30.0
    assert wikimedia.base_delay_for(100) == 30.0
