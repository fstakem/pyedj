from pyedj.compute.operations.abstract import Abstract
from pyedj.compute.event import Event


class Sum(Abstract):

    def compute(self, streams):
        output = []
        num_samples = len(streams[0])

        for i in range(num_samples):
            timestamp = streams[0][i].timestamp
            values = [s[i].sample for s in streams]
            out = sum(values)
            output.append(Event(timestamp, out))

        return output