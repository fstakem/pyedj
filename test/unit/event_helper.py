from datetime import datetime, timedelta
import random as rand

from pyedj.compute.event import Event
from pyedj.compute.stream import Stream


def generate_events(start_time, offset_sec, events=None, num=0, in_order=True):
    if start_time:
        timestamp = start_time
    else:
        timestamp = datetime.utcnow()

    end_time = num * offset_sec
    timestamps = [timestamp + timedelta(seconds=i) for i in range(end_time, 0, -offset_sec)]

    if not in_order:
        rand.shuffle(timestamps)

    if not events:
        events = [rand.randint(1, 101) for i in range(num)]

    gen_events = [Event(t, s) for t, s in zip(timestamps, events)]

    return gen_events


def data_to_stream(name, data, offset_sec, start_time=None, in_order=True):
    if start_time:
        timestamp = start_time
    else:
        timestamp = datetime.utcnow()

    num = len(data)
    end_time = num * offset_sec
    timestamps = [timestamp + timedelta(seconds=i) for i in range(end_time, 0, -offset_sec)]

    if not in_order:
        rand.shuffle(timestamps)

    events = [Event(t, s) for t, s in zip(timestamps, data)]

    return Stream(name, events)
