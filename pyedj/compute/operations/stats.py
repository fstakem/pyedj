from numpy import mean, median, std, var, ptp, percentile, min, max

from pyedj.compute.operations.abstract import Abstract
from pyedj.compute.event import Event


class Mean(Abstract):

    def execute(self, stream):
        values = [e.sample for e in stream]

        return [Event(stream[0].timestamp, mean(values))]


class Median(Abstract):

    def execute(self, stream):
        values = [e.sample for e in stream]

        return [Event(stream[0].timestamp, median(values))]


class Range(Abstract):

    def execute(self, stream):
        values = [e.sample for e in stream]

        return [Event(stream[0].timestamp, ptp(values))]


class Percentile(Abstract):

    def __init__(self, parent, pct):
        super().__init__(parent)
        self.pct = pct

    def execute(self, stream):
        values = [e.sample for e in stream]

        return [Event(stream[0].timestamp, percentile(values, self.pct))]


class Variance(Abstract):

    def execute(self, stream):
        values = [e.sample for e in stream]

        return [Event(stream[0].timestamp, var(values))]


class Std(Abstract):

    def execute(self, stream):
        values = [e.sample for e in stream]

        return [Event(stream[0].timestamp, std(values))]


class Min(Abstract):

    def execute(self, stream):
        values = [e.sample for e in stream]

        return [Event(stream[0].timestamp, min(values))]


class Max(Abstract):

    def execute(self, stream):
        values = [e.sample for e in stream]

        return [Event(stream[0].timestamp, max(values))]
