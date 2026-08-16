import pulsestream.sources.wikimedia as wikimedia


def test_delay_for_varies():
    values = [wikimedia.delay_for(3) for _ in range(20)]
    assert len(values) > 1


def test_base_delay_for_doubles_and_caps():
    assert wikimedia.base_delay_for(0) == 1.0
    assert wikimedia.base_delay_for(3) == 8.0
    assert wikimedia.base_delay_for(10) == 30.0
    assert wikimedia.base_delay_for(100) == 30.0
