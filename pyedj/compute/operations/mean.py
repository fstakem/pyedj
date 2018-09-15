from numpy import mean

from pyedj.compute.operations.abstract import Abstract
from pyedj.compute.event import Event


class Mean(Abstract):

    def __call__(self, stream):
        values = [e.sample for e in stream]

        return [Event(stream[-1].timestamp, mean(values))]