import math
import statistics

import pytest

from pulsestream.detection.features import (
    MAD_SCALE,
    RollingStats,
    clip,
    ewma_update,
    median_abs_deviation,
    modified_zscore,
    zscore,
)


def test_ewma_seeds_with_the_first_value_then_blends() -> None:
    assert ewma_update(None, 10.0, 0.3) == 10.0
    assert ewma_update(10.0, 20.0, 0.3) == pytest.approx(13.0)  # 0.3*20 + 0.7*10
    assert ewma_update(10.0, 20.0, 1.0) == 20.0  # alpha=1 tracks the latest value


def test_ewma_rejects_bad_alpha() -> None:
    for alpha in (0.0, -0.1, 1.5):
        with pytest.raises(ValueError):
            ewma_update(1.0, 1.0, alpha)


def test_rolling_stats_window_and_moments() -> None:
    stats = RollingStats(window=3)
    assert (stats.mean, stats.std, stats.full) == (None, None, False)
    stats.push(2.0)
    assert stats.mean == 2.0 and stats.std is None  # one value: no spread yet
    stats.push(4.0)
    stats.push(9.0)
    assert stats.full and stats.mean == pytest.approx(5.0)
    assert stats.std == pytest.approx(statistics.stdev([2.0, 4.0, 9.0]))
    stats.push(1.0)  # evicts 2.0
    assert stats.values() == [4.0, 9.0, 1.0] and len(stats) == 3


def test_rolling_stats_needs_a_window_of_at_least_two() -> None:
    with pytest.raises(ValueError):
        RollingStats(window=1)


def test_median_abs_deviation_and_modified_zscore() -> None:
    values = [10.0, 12.0, 9.0, 11.0, 50.0, 10.0, 13.0]
    med, mad = median_abs_deviation(values)
    assert (med, mad) == (11.0, 1.0)
    assert modified_zscore(50.0, med, mad) == pytest.approx(MAD_SCALE * 39.0)
    assert modified_zscore(11.0, med, mad) == 0.0


def test_a_zero_mad_gives_no_score_instead_of_infinity() -> None:
    med, mad = median_abs_deviation([2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 100.0])
    assert (med, mad) == (2.0, 0.0)
    assert modified_zscore(100.0, med, mad) is None


def test_zscore_and_clip() -> None:
    assert zscore(13.0, 10.0, 1.5) == pytest.approx(2.0)
    assert zscore(13.0, 10.0, 0.0) is None
    assert zscore(13.0, 10.0, math.nan) is None
    assert clip(5.0, 0.0, 3.0) == 3.0
    assert clip(-1.0, 0.0, 3.0) == 0.0
    assert clip(2.0, 0.0, 3.0) == 2.0


def test_mad_needs_values() -> None:
    with pytest.raises(ValueError):
        median_abs_deviation([])
