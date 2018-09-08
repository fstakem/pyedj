import pytest
from datetime import datetime, timedelta

from pyedj.compute.operations.divide import Divide

from event_helper import generate_events


def test_divide():
    start_time = datetime.utcnow()
    a_values = [10, 20, 30, -40]
    a_events = generate_events(start_time, 1, a_values)
    b_values = [5, -40, 6, 4]
    b_events = generate_events(start_time, 1, b_values)

    divide_op = Divide(None)
    events = divide_op([a_events, b_events])
    expects = [2, -0.5, 5, -10]

    for i, e in enumerate(events):
        assert e.timestamp == start_time + timedelta(seconds=i)
        assert e.sample == expects[i]