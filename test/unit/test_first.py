import pytest
from datetime import datetime, timedelta

from pyedj.compute.operations.first import First

from event_helper import generate_events


def test_first():
    start_time = datetime.utcnow()
    a_values = [10, 2, 20, 5]
    a_events = generate_events(start_time, a_values)

    first_op = First(None)
    events = first_op([a_events, b_events, c_events])
    expects = [4, 2, -6, -7]

    assert events[0].timestamp == start_time
    assert events[0].sample == expects[0]