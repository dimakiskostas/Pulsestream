import pulsestream.sources.wikimedia


def test_delay_for():
    assert pulsestream.sources.wikimedia.delay_for(0) >= 0.5
    assert pulsestream.sources.wikimedia.delay_for(1) >= 1.0
    assert pulsestream.sources.wikimedia.delay_for(2) >= 2.0
    assert pulsestream.sources.wikimedia.delay_for(3) >= 4.0
    assert pulsestream.sources.wikimedia.delay_for(4) >= 8.0
    assert pulsestream.sources.wikimedia.delay_for(5) >= 16.0
    assert pulsestream.sources.wikimedia.delay_for(6) >= 20.0
    assert pulsestream.sources.wikimedia.delay_for(7) >= 20.0
