import pytest
from collections import namedtuple
from datetime import datetime, timedelta

from pyedj.compute.operations.sum import Sum
from pyedj.compute.event import Event


MultiSensorEvent = namedtuple('MultiSensorEvent', 'timestamp temp humidity')
TempEvent = namedtuple('TempEvent', 'timestamp temperature')


def create_diff_events(start_time, step_sec, temp_values, multi_values):
    soil_events = []
    multi_events = []

    for i in range(len(temp_values)):
        t = start_time + timedelta(seconds=i)
        soil_events.append(TempEvent(t, temp_values[i]))
        multi_events.append((MultiSensorEvent(t, *multi_values[i])))

    return (soil_events, multi_events)


def create_events(start_time, step_sec, values):
    events = []

    for i in range(len(values)):
        t = start_time + timedelta(seconds=i)
        events.append(Event(t, values[i]))

    return events


def test_sum():
    start_time = datetime.utcnow()
    a_values = [1, 2, 3, 4]
    a_events = create_events(start_time, 1, a_values)
    b_values = [5, 6, 7, 8]
    b_events = create_events(start_time, 1, b_values)

    sum_op = Sum(None)
    output = sum_op.compute([a_events, b_events])

    import ipdb
    ipdb.set_trace()