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

            for j, s in enumerate(streams):
                s[i] = Event(timestamp, s[i].sample - other[i])

        return streams