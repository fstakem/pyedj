import pytest
from datetime import datetime, timedelta

from pyedj.compute.operations.sub import Sub

from event_helper import generate_events


def test_sub():
    start_time = datetime.utcnow()
    a_values = [10, 2, 20, 5]
    a_events = generate_events(start_time, a_values)
    b_values = [5, -6, 12, 20]
    b_events = generate_events(start_time, b_values)
    c_values = [1, 6, 14, -8]
    c_events = generate_events(start_time, c_values)

    sub_op = Sub(None)
    events = sub_op([a_events, b_events, c_events])
    expects = [4, 2, -6, -7]

    for i, e in enumerate(events):
        assert e.timestamp == start_time + timedelta(seconds=len(events)-i)
        assert e.sample == expects[i]