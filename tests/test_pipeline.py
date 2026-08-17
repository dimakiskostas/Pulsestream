from pulsestream.pipeline import should_flush


def test_should_flush():
    assert should_flush(5, 2.0, 1.0)
    assert not should_flush(0, 2.0, 1.0)
    assert should_flush(5, 1.0, 1.0)
    assert not should_flush(5, 1.0, 2.0)
    assert not should_flush(0, 2.0, 2.0)
    assert not should_flush(0, 1.0, 2.0)
