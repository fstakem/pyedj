import pytest
from datetime import datetime, timedelta

from pyedj.compute.operations.multiply_by_all import MultiplyByAll

from event_helper import generate_events


def test_multiply_by_all_integer():
    start_time = datetime.utcnow()
    a_values = [1, -2, 3, 4]
    a_events = generate_events(start_time, a_values)
    b_values = [5, 6, -7, 8]
    b_events = generate_events(start_time, b_values)

    multiply_by_all_op = MultiplyByAll(None)
    streams = multiply_by_all_op([a_events, b_events], 10)
    expects = [[10, -20, 30, 40],
               [50, 60, -70, 80]]
    for i, s in enumerate(streams):
        for j, e in enumerate(s):
            assert e.timestamp == start_time + timedelta(seconds=len(s)-j)
            assert e.sample == expects[i][j]


def test_multiply_by_all_stream():
    start_time = datetime.utcnow()
    a_values = [1, 2, 3, -4]
    a_events = generate_events(start_time, a_values)
    b_values = [5, 6, 7, 8]
    b_events = generate_events(start_time, b_values)
    c_values = [-1, 10, 6, -4]
    c_events = generate_events(start_time, c_values)

    multiply_by_all_op = MultiplyByAll(None)
    streams = multiply_by_all_op([a_events, b_events], c_events)
    expects = [[-1, 20, 18, 16],
               [-5, 60, 42, -32]]

    for i, s in enumerate(streams):
        for j, e in enumerate(s):
            assert e.timestamp == start_time + timedelta(seconds=len(s)-j)
            assert e.sample == expects[i][j]