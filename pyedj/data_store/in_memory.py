from collections import deque
from operator import attrgetter
from datetime import datetime
from threading import Semaphore

from pyedj.compute.event import Timestamp


class InMemory(object):
    # Data stored in objects: column store could be more efficient
    # Objects stored in deque according to timestamp
    #   left being most recent and right being oldest

    def __init__(self, max_time=100):
        self.buffer = deque()
        self.semaphore = Semaphore()
        self.max_time = max_time

    def add_events(self, events):
        if not len(events):
            return

        try:
            last_event = self.buffer[0]
        except IndexError:
            last_event = None

        if not last_event:
            events = sorted(events, key=attrgetter("timestamp"), reverse=True)

            self.semaphore.acquire()
            self.buffer.extend(events)
            self.semaphore.release()
        else:
            events = sorted(events, key=attrgetter("timestamp"))
            oldest_new_event = events[0]

            if Timestamp.after(oldest_new_event, last_event):
                self.semaphore.acquire()
                self.buffer.extendleft(events)
                self.semaphore.release()
            else:
                events = sorted(events, key=attrgetter("timestamp"), reverse=True)
                self.add_interleaved_events(events)

    def add_interleaved_events(self, events):
        new_buffer = []
        stored_index = 0
        new_index = 0
        num_new_events = len(events)

        self.semaphore.acquire()
        buffer = list(self.buffer)
        num_stored_events = len(buffer)

        while True:
            stored_event = None
            new_event = None

            if stored_index < num_stored_events:
                stored_event = buffer[stored_index]

            if new_index < num_new_events:
                new_event = events[new_index]

            if stored_event and new_event:
                try:
                    if Timestamp.after(stored_event, new_event):
                        while Timestamp.after(stored_event, new_event):
                            new_buffer.append(stored_event)
                            stored_index += 1
                            stored_event = buffer[stored_index]
                    elif Timestamp.before(stored_event, new_event):
                        while Timestamp.before(stored_event, new_event):
                            new_buffer.append(new_event)
                            new_index += 1
                            new_event = events[new_index]
                    else:
                        new_buffer.append(stored_event)
                        new_buffer.append(new_event)
                        stored_index += 1
                        new_index += 1
                except IndexError as ie:
                    pass

            elif stored_event:
                new_buffer.extend(buffer[stored_index:])
                stored_index = num_stored_events
            elif new_event:
                new_buffer.extend(events[new_index:])
                new_index = num_stored_events
            else:
                break

        self.buffer = deque(new_buffer)
        self.semaphore.release()

    def add_block(self, block):
        pass

    def get_events(self, window_len_ms):
        if not len(self.buffer) or window_len_ms < 1:
            return []

        index = 0
        start_time = datetime.utcnow()

        self.semaphore.acquire()
        buffer = list(self.buffer)

        while index < len(buffer):
            event = buffer[index]
            time_delta = (start_time - event.timestamp).total_seconds() * 1000

            if time_delta > window_len_ms:
                self.semaphore.release()

                return buffer[:index]

            index += 1

        self.semaphore.release()

        return buffer

    def get_n_events(self, n):
        self.semaphore.acquire()
        buffer = list(self.buffer)
        self.semaphore.release()

        return buffer[:n]

    def trim(self, trim_size_ms):
        events = self.get_events(trim_size_ms)

        self.semaphore.acquire()
        self.buffer = deque(events)
        self.semaphore.release()
