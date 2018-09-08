from pyedj.compute.operations.abstract import Abstract
from pyedj.compute.event import Event


class SubFromAll(Abstract):

    def __call__(self, streams, other):
        num_samples = len(streams[0])

        if type(other) != list:
            other = [other] * num_samples
        else:
            other = [o.sample for o in other]

        for i in range(num_samples):
            timestamp = streams[0][i].timestamp

            for j in enumerate(streams):
                streams[j][i] = Event(timestamp, streams[j][i] - other[i])

        return streams