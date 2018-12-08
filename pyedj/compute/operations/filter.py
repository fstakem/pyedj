from enum import Enum
from operator import ge, le, gt, lt

from pyedj.compute.operations.abstract import Abstract
from pyedj.compute.event import Event


class FillAlg(Enum):
    LAST = 1
    VALUE = 2
    LERP = 3    # Linear interpolation


class FillLast(object):

    def __init__(self, last_value=0):
        self.last_value = last_value
        self.operator_func = None

    def __call__(self, left, right):
        if type(left) == list and type(right) == list:
            events = []
            last = self.last_value

            for l, r in zip(left, right):
                if self.operator_func(l.sample, r.sample):
                    last = l.sample
                    events.append(l)
                else:
                    events.append(Event(l.timestamp, last))

            return events

        elif type(left) == list:
            events = []
            last = self.last_value

            for l in left:
                if self.operator_func(l.sample, right.sample):
                    last = l.sample
                    events.append(l)
                else:
                    events.append(Event(l.timestamp, last))

            return events

        elif type(right) == list:
            events = []
            last = self.last_value

            for r in right:
                if self.operator_func(left.sample, r.sample):
                    last = left.sample
                    events.append(left)
                else:
                    events.append(Event(left.timestamp, last))

            return events

        msg = f'One side of operand must a list of Event\n' \
              '    Left  side: {type(left)}\n' \
              '    Right side: {type(right}\n'
        raise SyntaxError(msg)


class FillValue(object):
    pass


class FillLinearInterp(object):
    pass


class GreaterThan(Abstract):

    def __init__(self, parent, algorithm):
        super().__init__(parent)
        algorithm.operator_func = gt
        self.execute = algorithm

    def execute(self):
        pass


class LessThan(Abstract):

    def __init__(self, parent, algorithm):
        super().__init__(parent)
        algorithm.operator_func = lt
        self.execute = algorithm

    def execute(self):
        pass


class GreaterThanOrEqual(Abstract):

    def __init__(self, parent, algorithm):
        super().__init__(parent)
        algorithm.operator_func = ge
        self.execute = algorithm

    def execute(self):
        pass


class LessThanOrEqual(Abstract):

    def __init__(self, parent, algorithm):
        super().__init__(parent)
        algorithm.operator_func = le
        self.execute = algorithm

    def execute(self):
        pass


class Equal(Abstract):

    def __init__(self, parent, fill=FillAlg.LAST, value=0):
        super().__init__(parent)
        self.value = value

        func_name = f'fill_{fill.name.lower()}'
        self.execute = getattr(self, func_name)

    def execute(self):
        pass

    def fill_last(self, left, right):
        pass

    def fill_value(self, left, right):
        pass

    def fill_lerp(self, left, right):
        pass


class Filter(Abstract):

    def __init__(self, parent, fill=FillAlg.LAST, value=0):
        super().__init__(parent)
        self.value = value

        func_name = f'fill_{fill.name.lower()}'
        self.execute = getattr(self, func_name)

    def execute(self):
        pass

    def fill_last(self, left, right):
        pass

    def fill_value(self, left, right):
        pass

    def fill_lerp(self, left, right):
        pass
