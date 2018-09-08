import pytest
from datetime import datetime, timedelta

from pyedj.compute.operations.multiply import Multiply

from event_helper import generate_events


def test_multiply():
    start_time = datetime.utcnow()
    a_values = [1, -2, 3, 4]
    a_events = generate_events(start_time, 1, a_values)
    b_values = [5, 6, 7, 8]
    b_events = generate_events(start_time, 1, b_values)

    add_op = Multiply(None)
    events = add_op([a_events, b_events])
    expects = [5, -12, 21, 32]

    for i, e in enumerate(events):
        assert e.timestamp == start_time + timedelta(seconds=i)
        assert e.sample == expects[i]