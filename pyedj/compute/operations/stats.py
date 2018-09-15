from numpy import mean, median, std, var, ptp, percentile, min, max

from pyedj.compute.operations.abstract import Abstract
from pyedj.compute.event import Event


class Mean(Abstract):

    def __call__(self, stream):
        values = [e.sample for e in stream]

        return [Event(stream[0].timestamp, mean(values))]


class Median(Abstract):

    def __call__(self, stream):
        values = [e.sample for e in stream]

        return [Event(stream[0].timestamp, median(values))]


class Range(Abstract):

    def __call__(self, stream):
        values = [e.sample for e in stream]

        return [Event(stream[0].timestamp, ptp(values))]


class Percentile(Abstract):

    def __call__(self, stream, pct):
        values = [e.sample for e in stream]

        return [Event(stream[0].timestamp, percentile(values, pct))]


class Variance(Abstract):

    def __call__(self, stream):
        values = [e.sample for e in stream]

        return [Event(stream[0].timestamp, var(values))]


class Std(Abstract):

    def __call__(self, stream):
        values = [e.sample for e in stream]

        return [Event(stream[0].timestamp, std(values))]


class Min(Abstract):

    def __call__(self, stream):
        values = [e.sample for e in stream]

        return [Event(stream[0].timestamp, min(values))]


class Max(Abstract):

    def __call__(self, stream):
        values = [e.sample for e in stream]

        return [Event(stream[0].timestamp, max(values))]
