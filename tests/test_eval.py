"""The detector against synthetic ground truth: recall, precision and the masked-spike case."""

import statistics

from pulsestream.cli import main
from pulsestream.detection.eval import (
    Evaluation,
    evaluate,
    format_table,
    inject,
    inject_level,
    spike_positions,
    standard_scenarios,
    synthetic_series,
)
from pulsestream.detection.rate import EwmaBandDetector


def test_synthetic_series_is_deterministic_and_realistic() -> None:
    a = synthetic_series(500, baseline=20.0, seed=3)
    assert a == synthetic_series(500, baseline=20.0, seed=3) and len(a) == 500
    assert all(isinstance(x, int) and x >= 0 for x in a)
    flat = synthetic_series(2000, baseline=20.0, swing=0.0, seed=3)
    assert 19 < statistics.fmean(flat) < 21, "Poisson around the baseline without the swing"
    assert statistics.fmean(a) > statistics.fmean(flat[:500]), "the first hours of the day swing up"
    assert synthetic_series(500, seed=4) != a


def test_inject_and_positions() -> None:
    series = [10] * 50
    assert inject(series, [5, 7], 3.0) == [10] * 5 + [30, 10, 30] + [10] * 42
    assert series == [10] * 50, "the input is not modified"
    assert inject_level(series, [1, 2], 70)[:4] == [10, 70, 70, 10]
    positions = spike_positions(600, 12)
    assert len(positions) == 12 and positions == sorted(positions) and positions[0] >= 30
    assert all(b - a >= 20 for a, b in zip(positions, positions[1:], strict=False))


def test_evaluation_metrics() -> None:
    e = Evaluation(injected=(10, 20, 30), flagged=(10, 20, 40, 41))
    assert (e.tp, e.fp, e.fn) == (2, 2, 1)
    assert e.precision == 0.5 and e.recall == 2 / 3
    assert round(e.f1, 3) == round(2 * 0.5 * (2 / 3) / (0.5 + 2 / 3), 3)
    assert Evaluation((), ()).precision == 0.0 and Evaluation((), ()).f1 == 0.0


def test_isolated_spikes_are_caught_with_few_false_alarms() -> None:
    series = synthetic_series(600)
    positions = spike_positions(600, 12)
    e = evaluate(inject(series, positions, 4.0), positions, detector_factory=EwmaBandDetector)

    assert e.recall == 1.0, f"missed {e.fn} of the injected spikes: {e}"
    assert e.precision >= 0.6, f"too many false alarms: {e.fp}"


def test_winsorising_keeps_consecutive_spikes_visible() -> None:
    series = synthetic_series(400)
    run = [100, 101, 102, 103]
    spiked = inject_level(series, run, 70)  # equal spikes: the plain band swallows the later ones

    winsorised = evaluate(spiked, run, detector_factory=lambda: EwmaBandDetector(winsorize=True))
    plain = evaluate(spiked, run, detector_factory=lambda: EwmaBandDetector(winsorize=False))

    assert winsorised.recall == 1.0
    assert plain.recall <= 0.5, "spikes entering the baseline unclipped hide the ones after them"


def test_standard_scenarios_and_table() -> None:
    scenarios = standard_scenarios(windows=300, spikes=6)
    assert [s.name for s in scenarios][:3] == [
        "6 isolated spikes x4, k=2.5",
        "6 isolated spikes x4, k=3",
        "6 isolated spikes x4, k=4",
    ]
    table = format_table(scenarios)
    assert table.splitlines()[0].startswith("scenario") and "winsorised baseline" in table
    winsorised, plain = scenarios[-2].evaluation, scenarios[-1].evaluation
    assert winsorised.recall == 1.0 and plain.recall <= 0.5
    assert len(table.splitlines()) == 2 + len(scenarios)


def test_eval_cli_prints_the_table(capsys) -> None:
    assert main(["eval", "--windows", "300", "--spikes", "6"]) == 0
    out = capsys.readouterr().out
    assert "precision" in out and "plain baseline" in out
