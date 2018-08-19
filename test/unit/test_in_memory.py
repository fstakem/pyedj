import pytest
from collections import deque
from datetime import datetime, timedelta
from operator import attrgetter

from pyedj.data_store.in_memory import InMemmory

from event_helper import generate_events


def create_store(events=None):
    store = InMemmory()

    if events:
        store.buffer = deque(events)

    return store


def show_events(events):
    for e in events:
        print(e)


def show_buffer(store):
    buffer = list(store.buffer)
    show_events(buffer)


def test_add_events_empty_to_empty():
    store = create_store()
    store.add_events([])

    assert len(store.buffer) == 0


def test_add_events_empty():
    num_events = 2
    events = generate_events(num_events, 2)
    store = create_store(events)
    store.add_events([])

    assert len(store.buffer) == num_events

    for i in range(num_events):
        assert events[i] == store.buffer[i]


def test_add_events_empty_buffer():
    store = create_store()
    num_events = 2
    events = generate_events(num_events, 2)
    store.add_events(events)

    assert len(store.buffer) == num_events

    for i in range(num_events):
        assert events[i] == store.buffer[i]


def test_add_events_newer_ordered():
    start_time = datetime.utcnow()
    num_buffer_events = 2
    buffer_events = generate_events(num_buffer_events, 2, start_time)
    store = create_store(buffer_events)

    num_new_events = 3
    start_time = start_time + timedelta(seconds=5)
    new_events = generate_events(num_new_events, 2, start_time)

    store.add_events(new_events)

    assert len(store.buffer) == num_buffer_events + num_new_events

    for i in range(num_new_events):
        assert new_events[i] == store.buffer[i]

    for i in range(num_buffer_events):
        assert buffer_events[i] == store.buffer[i+num_new_events]


def test_add_events_newer_unordered():
    start_time = datetime.utcnow()
    num_buffer_events = 2
    buffer_events = generate_events(num_buffer_events, 2, start_time)
    store = create_store(buffer_events)

    num_new_events = 3
    start_time = start_time + timedelta(seconds=5)
    new_events = generate_events(num_new_events, 2, start_time, random=True, in_order=False)

    store.add_events(new_events)

    assert len(store.buffer) == num_buffer_events + num_new_events

    test_new_events = sorted(new_events, key=attrgetter("timestamp"), reverse=True)

    for i in range(num_new_events):
        assert test_new_events[i] == store.buffer[i]

    for i in range(num_buffer_events):
        assert buffer_events[i] == store.buffer[i+num_new_events]


def test_add_interleaved_events_new_before():
    start_time = datetime.utcnow()
    num_buffer_events = 3
    buffer_events = generate_events(num_buffer_events, 3, start_time)
    store = create_store(buffer_events)

    num_new_events = 5
    start_time = start_time + timedelta(seconds=3)
    new_events = generate_events(num_new_events, 1, start_time, random=True)

    store.add_events(new_events)

    test_events = []
    test_events.extend(buffer_events)
    test_events.extend((new_events))
    test_events = sorted(test_events, key=attrgetter("timestamp"), reverse=True)

    assert len(store.buffer) == len(test_events)

    for i in range(len(test_events)):
        assert test_events[i] == store.buffer[i]


def test_add_interleaved_events_new_mixed():
    start_time = datetime.utcnow()
    num_buffer_events = 3
    buffer_events = generate_events(num_buffer_events, 3, start_time)
    store = create_store(buffer_events)

    num_new_events = 5
    start_time = start_time + timedelta(seconds=5)
    new_events = generate_events(num_new_events, 1, start_time, random=True)

    store.add_events(new_events)

    test_events = []
    test_events.extend(buffer_events)
    test_events.extend((new_events))
    test_events = sorted(test_events, key=attrgetter("timestamp"), reverse=True)

    assert len(store.buffer) == len(test_events)

    for i in range(len(test_events)):
        assert test_events[i] == store.buffer[i]


def test_add_interleaved_events_new_after():
    start_time = datetime.utcnow()
    num_buffer_events = 3
    buffer_events = generate_events(num_buffer_events, 3, start_time)
    store = create_store(buffer_events)

    num_new_events = 2
    start_time = start_time - timedelta(seconds=10)
    new_events = generate_events(num_new_events, 1, start_time, random=True)

    store.add_events(new_events)

    test_events = []
    test_events.extend(buffer_events)
    test_events.extend((new_events))
    test_events = sorted(test_events, key=attrgetter("timestamp"), reverse=True)

    assert len(store.buffer) == len(test_events)

    for i in range(len(test_events)):
        assert test_events[i] == store.buffer[i]


def test_get_n_events():
    start_time = datetime.utcnow()
    num_buffer_events = 20
    buffer_events = generate_events(num_buffer_events, 2, start_time)
    store = create_store(buffer_events)

    num_events = 6
    events = store.get_n_events(num_events)

    assert num_events == len(events)

    for i in range(num_events):
        assert events[i] == store.buffer[i]


def test_get_n_events_empty():
    start_time = datetime.utcnow()
    num_buffer_events = 20
    buffer_events = generate_events(num_buffer_events, 2, start_time)
    store = create_store(buffer_events)

    num_events = 0
    events = store.get_n_events(num_events)

    assert num_events == len(events)


def test_get_n_events_empty_buffer():
    buffer_events = []
    store = create_store(buffer_events)

    num_events = 0
    events = store.get_n_events(num_events)

    assert num_events == len(events)


def test_get_events():
    start_time = datetime.utcnow() - timedelta(seconds=80)
    num_buffer_events = 10
    buffer_events = generate_events(num_buffer_events, 2, start_time)
    store = create_store(buffer_events)

    num_new_events = 10
    start_time = datetime.utcnow() - timedelta(seconds=12)
    new_events = generate_events(num_new_events, 1, start_time, random=True)

    store.add_events(new_events)

    events = store.get_events(60*1000)

    assert num_new_events == len(events)

    for i in range(num_new_events):
        assert events[i] == new_events[i]


def test_get_events_too_old():
    start_time = datetime.utcnow() - timedelta(seconds=80)
    num_buffer_events = 10
    buffer_events = generate_events(num_buffer_events, 2, start_time)
    store = create_store(buffer_events)

    events = store.get_events(60*1000)

    assert 0 == len(events)


def test_get_events_empty_buffer():
    store = create_store([])
    events = store.get_events(60*1000)

    assert 0 == len(events)


def test_get_events_incorrect_window():
    start_time = datetime.utcnow() - timedelta(seconds=80)
    num_buffer_events = 10
    buffer_events = generate_events(num_buffer_events, 2, start_time)
    store = create_store(buffer_events)

    events = store.get_events(-60*1000)

    assert 0 == len(events)