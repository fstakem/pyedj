import pytest
from datetime import datetime, timedelta

from pyedj.compute.operations.filter import FillLast, GreaterThan
from pyedj.compute.event import Event

from event_helper import generate_events


def test_greater_than_fill_last():
    start_time = datetime.utcnow()
    l_values = [1, 2, 3, 4]
    l_events = generate_events(start_time, l_values)
    r_values = [0, 6, 3, 1]
    r_events = generate_events(start_time, r_values)

    fill = FillLast(0)
    gte = GreaterThan(None, fill)
    events = gte.execute(l_events, r_events)
    expects = [1, 1, 1, 4]

    for i, e in enumerate(events):
        assert e.timestamp == start_time + timedelta(seconds=len(events)-i-1)
        assert e.sample == expects[i]


def test_greater_than_fill_last_with_fill():
    start_time = datetime.utcnow()
    l_values = [0, 2, 3, 4]
    l_events = generate_events(start_time, l_values)
    r_values = [1, 6, 3, 1]
    r_events = generate_events(start_time, r_values)

    fill = FillLast(0)
    gte = GreaterThan(None, fill)
    events = gte.execute(l_events, r_events)
    expects = [0, 0, 0, 4]

    for i, e in enumerate(events):
        assert e.timestamp == start_time + timedelta(seconds=len(events)-i-1)
        assert e.sample == expects[i]


def test_greater_than_fill_last_left_num():
    start_time = datetime.utcnow()
    l_values = 3
    l_events = generate_events(start_time, l_values)
    r_values = [0, 6, 3, 1]
    r_events = generate_events(start_time, r_values)

    fill = FillLast(0)
    gte = GreaterThan(None, fill)
    events = gte.execute(l_events, r_events)
    expects = [1, 1, 1, 4]

    for i, e in enumerate(events):
        assert e.timestamp == start_time + timedelta(seconds=len(events)-i-1)
        assert e.sample == expects[i]


def test_greater_than_fill_last_right_num():
    start_time = datetime.utcnow()
    l_values = [1, 2, 3, 4]
    l_events = generate_events(start_time, l_values)
    r_values = 3
    r_events = generate_events(start_time, r_values)

    fill = FillLast(0)
    gte = GreaterThan(None, fill)
    events = gte.execute(l_events, r_events)
    expects = [1, 1, 1, 4]

    for i, e in enumerate(events):
        assert e.timestamp == start_time + timedelta(seconds=len(events)-i-1)
        assert e.sample == expects[i]


def test_greater_than_fill_last_no_list():
    start_time = datetime.utcnow()
    l_event = Event(start_time, 4)
    r_event = Event(start_time, 3)

    fill = FillLast(0)
    gte = GreaterThan(None, fill)
    events = gte.execute(l_event, r_event)
    expects = [1, 1, 1, 4]

    for i, e in enumerate(events):
        assert e.timestamp == start_time + timedelta(seconds=len(events)-i-1)
        assert e.sample == expects[i]