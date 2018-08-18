import pytest

from pyedj.compute.operations.sum import Sum
from event_helper import generate_events, data_to_stream


def test_sum():
    a = [1, 2, 3, 4]
    b = [5, 6, 7, 8]
    #streams = [data_to_stream('a', a), data_to_stream('b', b)]

    #s = Sum(None)
    #out = s.compute(streams)

    #print(out)