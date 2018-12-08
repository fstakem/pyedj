import pytest
from datetime import datetime, timedelta

from pyedj.compute.operations.math import Add, Subtract, Multiply, Divide

from event_helper import generate_events


def test_add_event_event():
    start_time = datetime.utcnow()
    l_values = [1, 2, 3, 4]
    l_events = generate_events(start_time, l_values)
    r_values = [5, 6, 7, 8]
    r_events = generate_events(start_time, r_values)

    add_op = Add(None)
    events = add_op.execute(l_events, r_events)
    expects = [6, 8, 10, 12]

    for i, e in enumerate(events):
        assert e.timestamp == start_time + timedelta(seconds=len(events)-i-1)
        assert e.sample == expects[i]


def test_add_event_const():
    start_time = datetime.utcnow()
    l_values = [1, 2, 3, 4]
    l_events = generate_events(start_time, l_values)

    add_op = Add(None)
    events = add_op.execute(l_events, 10)
    expects = [11, 12, 13, 14]

    for i, e in enumerate(events):
        assert e.timestamp == start_time + timedelta(seconds=len(events)-i-1)
        assert e.sample == expects[i]


def test_add_const_event():
    start_time = datetime.utcnow()
    r_values = [1, 2, 3, 4]
    r_events = generate_events(start_time, r_values)

    add_op = Add(None)
    events = add_op.execute(10, r_events)
    expects = [11, 12, 13, 14]

    for i, e in enumerate(events):
        assert e.timestamp == start_time + timedelta(seconds=len(events)-i-1)
        assert e.sample == expects[i]


def test_add_const_const():
    add_op = Add(None)

    with pytest.raises(SyntaxError):
        add_op.execute(10, 12)


def test_sub_event_event():
    start_time = datetime.utcnow()
    l_values = [1, 2, 13, 14]
    l_events = generate_events(start_time, l_values)
    r_values = [5, 6, 7, 8]
    r_events = generate_events(start_time, r_values)

    sub_op = Subtract(None)
    events = sub_op.execute(l_events, r_events)
    expects = [-4, -4, 6, 6]

    for i, e in enumerate(events):
        assert e.timestamp == start_time + timedelta(seconds=len(events)-i-1)
        assert e.sample == expects[i]


def test_sub_event_const():
    start_time = datetime.utcnow()
    l_values = [1, 2, 13, 14]
    l_events = generate_events(start_time, l_values)

    sub_op = Subtract(None)
    events = sub_op.execute(l_events, 10)
    expects = [-9, -8, 3, 4]

    for i, e in enumerate(events):
        assert e.timestamp == start_time + timedelta(seconds=len(events)-i-1)
        assert e.sample == expects[i]


def test_sub_const_event():
    start_time = datetime.utcnow()
    r_values = [1, 2, 13, 14]
    r_events = generate_events(start_time, r_values)

    sub_op = Subtract(None)
    events = sub_op.execute(10, r_events)
    expects = [9, 8, -3, -4]

    for i, e in enumerate(events):
        assert e.timestamp == start_time + timedelta(seconds=len(events)-i-1)
        assert e.sample == expects[i]


def test_sub_const_const():
    sub_op = Subtract(None)

    with pytest.raises(SyntaxError):
        sub_op.execute(10, 12)


def test_multiply_event_event():
    start_time = datetime.utcnow()
    l_values = [1, -2, 3, 4]
    l_events = generate_events(start_time, l_values)
    r_values = [5, 6, 7, 8]
    r_events = generate_events(start_time, r_values)

    multiply_op = Multiply(None)
    events = multiply_op.execute(l_events, r_events)
    expects = [5, -12, 21, 32]

    for i, e in enumerate(events):
        assert e.timestamp == start_time + timedelta(seconds=len(events)-i-1)
        assert e.sample == expects[i]


def test_multiply_event_const():
    start_time = datetime.utcnow()
    l_values = [1, -2, 3, 4]
    l_events = generate_events(start_time, l_values)

    multiply_op = Multiply(None)
    events = multiply_op.execute(l_events, 10)
    expects = [10, -20, 30, 40]

    for i, e in enumerate(events):
        assert e.timestamp == start_time + timedelta(seconds=len(events)-i-1)
        assert e.sample == expects[i]


def test_multiply_const_event():
    start_time = datetime.utcnow()
    r_values = [1, -2, 3, 4]
    r_events = generate_events(start_time, r_values)

    multiply_op = Multiply(None)
    events = multiply_op.execute(10, r_events)
    expects = [10, -20, 30, 40]

    for i, e in enumerate(events):
        assert e.timestamp == start_time + timedelta(seconds=len(events)-i-1)
        assert e.sample == expects[i]


def test_multiply_const_const():
    multiply_op = Multiply(None)

    with pytest.raises(SyntaxError):
        multiply_op.execute(10, 12)


def test_divide_event_event():
    start_time = datetime.utcnow()
    l_values = [10, -20, 30, 40]
    l_events = generate_events(start_time, l_values)
    r_values = [5, 2, 6, 8]
    r_events = generate_events(start_time, r_values)

    divide_op = Divide(None)
    events = divide_op.execute(l_events, r_events)
    expects = [2, -10, 5, 5]

    for i, e in enumerate(events):
        assert e.timestamp == start_time + timedelta(seconds=len(events)-i-1)
        assert e.sample == expects[i]


def test_divide_event_const():
    start_time = datetime.utcnow()
    l_values = [10, -20, 30, 40]
    l_events = generate_events(start_time, l_values)

    divide_op = Divide(None)
    events = divide_op.execute(l_events, 10)
    expects = [1, -2, 3, 4]

    for i, e in enumerate(events):
        assert e.timestamp == start_time + timedelta(seconds=len(events)-i-1)
        assert e.sample == expects[i]


def test_divide_const_event():
    start_time = datetime.utcnow()
    r_values = [10, -20, 20, -10]
    r_events = generate_events(start_time, r_values)

    divide_op = Divide(None)
    events = divide_op.execute(10, r_events)
    expects = [1, -0.5, 0.5, -1]

    for i, e in enumerate(events):
        assert e.timestamp == start_time + timedelta(seconds=len(events)-i-1)
        assert e.sample == expects[i]


def test_divide_const_const():
    divide_op = Divide(None)

    with pytest.raises(SyntaxError):
        divide_op.execute(10, 12)