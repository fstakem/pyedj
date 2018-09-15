import pytest
from datetime import datetime, timedelta

from pyedj.compute.operations.mean import Mean

from event_helper import generate_events


def test_mean():
    start_time = datetime.utcnow()
    a_values = [1, 2, 3, 4]
    a_events = generate_events(start_time, a_values)

    mean_op = Mean(None)
    events = mean_op(a_events)
    expects = [2.5]

    assert events[0].timestamp == start_time + timedelta(seconds=len(events))
    assert events[0].sample == expects[0]