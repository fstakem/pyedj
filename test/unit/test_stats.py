import pytest
from datetime import datetime, timedelta

from pyedj.compute.operations.stats import Mean, Median, Range, Percentile, Variance, Std, Min, Max

from event_helper import generate_events


def test_mean():
    start_time = datetime.utcnow()
    values = [1, 2, 3, 4, 10]
    events = generate_events(start_time, values)

    print()
    for e in events:
        print(e.timestamp)

    mean_op = Mean(None)
    out_events = mean_op(events)
    expects = [4.0]

    assert out_events[0].timestamp == start_time + timedelta(seconds=len(events)-1)
    assert out_events[0].sample == expects[0]


def test_median():
    start_time = datetime.utcnow()
    values = [1, 2, 3, 10]
    events = generate_events(start_time, values)

    median_op = Median(None)
    out_events = median_op(events)
    expects = [2.5]

    assert out_events[0].timestamp == start_time + timedelta(seconds=len(events)-1)
    assert out_events[0].sample == expects[0]


def test_range():
    start_time = datetime.utcnow()
    values = [1, 2, 3, 10]
    events = generate_events(start_time, values)

    range_op = Range(None)
    out_events = range_op(events)
    expects = [9]

    assert out_events[0].timestamp == start_time + timedelta(seconds=len(events)-1)
    assert out_events[0].sample == expects[0]


def test_percentile():
    start_time = datetime.utcnow()
    values = [1, 2, 3, 10]
    events = generate_events(start_time, values)

    pct_op = Percentile(None)
    out_events = pct_op(events, 90)
    expects = [7.9]

    assert out_events[0].timestamp == start_time + timedelta(seconds=len(events)-1)
    assert out_events[0].sample == pytest.approx(expects[0])


def test_variance():
    start_time = datetime.utcnow()
    values = [1, 2, 3, 10]
    events = generate_events(start_time, values)

    var_op = Variance(None)
    out_events = var_op(events)
    expects = [12.5]

    assert out_events[0].timestamp == start_time + timedelta(seconds=len(events)-1)
    assert out_events[0].sample == pytest.approx(expects[0])


def test_std():
    start_time = datetime.utcnow()
    values = [1, 2, 3, 10]
    events = generate_events(start_time, values)

    std_op = Std(None)
    out_events = std_op(events)
    expects = [3.535]

    assert out_events[0].timestamp == start_time + timedelta(seconds=len(events)-1)
    assert out_events[0].sample == pytest.approx(expects[0], rel=1e-3)


def test_min():
    start_time = datetime.utcnow()
    values = [1, 2, 3, 10]
    events = generate_events(start_time, values)

    min_op = Min(None)
    out_events = min_op(events)
    expects = [1]

    assert out_events[0].timestamp == start_time + timedelta(seconds=len(events)-1)
    assert out_events[0].sample == pytest.approx(expects[0])


def test_max():
    start_time = datetime.utcnow()
    values = [1, 2, 3, 10]
    events = generate_events(start_time, values)

    max_op = Max(None)
    out_events = max_op(events)
    expects = [10]

    assert out_events[0].timestamp == start_time + timedelta(seconds=len(events)-1)
    assert out_events[0].sample == pytest.approx(expects[0])


