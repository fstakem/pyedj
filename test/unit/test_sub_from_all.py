import pytest
from datetime import datetime, timedelta

from pyedj.compute.operations.sub_from_all import SubFromAll

from event_helper import generate_events


def test_sub_from_all_integer():
    start_time = datetime.utcnow()
    a_values = [10, 2, -3, 12]
    a_events = generate_events(start_time, 1, a_values)
    b_values = [5, 16, 2, -8]
    b_events = generate_events(start_time, 1, b_values)

    sub_from_all_op = SubFromAll(None)
    streams = sub_from_all_op([a_events, b_events], 10)
    expects = [[0, -8, -13, 2],
               [-5, 6, -8, -18]]

    for i, s in enumerate(streams):
        for j, e in enumerate(s):
            assert e.timestamp == start_time + timedelta(seconds=i)
            assert e.sample == expects[i][j]


def test_sub_from_all_stream():
    start_time = datetime.utcnow()
    a_values = [10, 2, -3, 12]
    a_events = generate_events(start_time, 1, a_values)
    b_values = [5, 16, 2, -8]
    b_events = generate_events(start_time, 1, b_values)
    c_values = [9, 10, 11, 12]
    c_events = generate_events(start_time, 1, c_values)

    sub_from_all_op = SubFromAll(None)
    streams = sub_from_all_op([a_events, b_events], 10)
    expects = [[1, -8, -5, 20],
               [-4, -8, -14, 0]]

    for i, s in enumerate(streams):
        for j, e in enumerate(s):
            assert e.timestamp == start_time + timedelta(seconds=i)
            assert e.sample == expects[i][j]