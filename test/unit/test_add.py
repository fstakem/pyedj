import pytest
from datetime import datetime, timedelta

from pyedj.compute.operations.add import Add

from event_helper import generate_events


def test_add():
    start_time = datetime.utcnow()
    a_values = [1, 2, 3, 4]
    a_events = generate_events(start_time, a_values)
    b_values = [5, 6, 7, 8]
    b_events = generate_events(start_time, b_values)

    add_op = Add(None)
    events = add_op([a_events, b_events])
    expects = [6, 8, 10, 12]

    for i, e in enumerate(events):
        assert e.timestamp == start_time + timedelta(seconds=len(events)-i)
        assert e.sample == expects[i]