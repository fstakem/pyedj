from functools import reduce

from pyedj.compute.operations.abstract import Abstract
from pyedj.compute.event import Event


class Divide(Abstract):

    def __call__(self, streams):
        output = []
        num_samples = len(streams[0])

        for i in range(num_samples):
            timestamp = streams[0][i].timestamp
            values = [s[i].sample for s in streams]
            out = reduce(lambda x, y: x/y, values)
            output.append(Event(timestamp, out))

        return output