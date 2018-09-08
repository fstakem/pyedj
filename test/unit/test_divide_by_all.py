import pytest
from datetime import datetime, timedelta

from pyedj.compute.operations.divide_by_all import DivideByAll

from event_helper import generate_events


def test_divide_by_all_integer():
    start_time = datetime.utcnow()
    a_values = [10, -12, 5, 4]
    a_events = generate_events(start_time, 1, a_values)
    b_values = [5, 6, 7, 8]
    b_events = generate_events(start_time, 1, b_values)

    divide_by_all_op = DivideByAll(None)
    streams = divide_by_all_op([a_events, b_events], 10)
    expects = [[1, -1.2, 0.5, 0.4],
               [0.5, 0.6, 0.7, 0.8]]

    for i, s in enumerate(streams):
        for j, e in enumerate(s):
            assert e.timestamp == start_time + timedelta(seconds=i)
            assert e.sample == expects[i][j]


def test_divide_by_all_stream():
    start_time = datetime.utcnow()
    a_values = [10, -12, 6, 4]
    a_events = generate_events(start_time, 1, a_values)
    b_values = [5, 6, -12, 8]
    b_events = generate_events(start_time, 1, b_values)
    c_values = [2, 10, 3, 2]
    c_events = generate_events(start_time, 1, c_values)

    divide_by_all_op = DivideByAll(None)
    streams = divide_by_all_op([a_events, b_events], 10)
    expects = [[5, -1.2, 2, 2],
               [2.5, 0.6, 2, 2]]

    for i, s in enumerate(streams):
        for j, e in enumerate(s):
            assert e.timestamp == start_time + timedelta(seconds=i)
            assert e.sample == expects[i][j]