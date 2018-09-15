from pyedj.compute.operations.abstract import Abstract
from pyedj.compute.event import Event


class Sub(Abstract):

    def __call__(self, streams):
        output = []
        num_samples = len(streams[0])

        for i in range(num_samples):
            timestamp = streams[0][i].timestamp
            val = streams[0][i].sample

            for j, s in enumerate(streams[1:]):
                val = val - s[i].sample

            output.append(Event(timestamp, val))

        return output