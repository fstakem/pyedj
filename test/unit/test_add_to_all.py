import pytest
from datetime import datetime, timedelta

from pyedj.compute.operations.add_to_all import AddToAll

from event_helper import generate_events


def test_add_to_all_integer():
    start_time = datetime.utcnow()
    a_values = [1, 2, 3, 4]
    a_events = generate_events(start_time, 1, a_values)
    b_values = [5, 6, 7, 8]
    b_events = generate_events(start_time, 1, b_values)

    add_to_all_op = AddToAll(None)
    streams = add_to_all_op([a_events, b_events], 10)
    expects = [[11, 12, 13, 14],
               [15, 16, 17, 18]]

    for i, s in enumerate(streams):
        for j, e in enumerate(s):
            assert e.timestamp == start_time + timedelta(seconds=i)
            assert e.sample == expects[i][j]


def test_add_to_all_stream():
    start_time = datetime.utcnow()
    a_values = [1, 2, 3, 4]
    a_events = generate_events(start_time, 1, a_values)
    b_values = [5, 6, 7, 8]
    b_events = generate_events(start_time, 1, b_values)
    c_values = [9, 10, 11, 12]
    c_events = generate_events(start_time, 1, c_values)

    add_to_all_op = AddToAll(None)
    streams = add_to_all_op([a_events, b_events], 10)
    expects = [[10, 12, 14, 16],
               [14, 16, 18, 20]]

    for i, s in enumerate(streams):
        for j, e in enumerate(s):
            assert e.timestamp == start_time + timedelta(seconds=i)
            assert e.sample == expects[i][j]