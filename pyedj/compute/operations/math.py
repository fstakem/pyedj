from pyedj.compute.operations.abstract import Abstract
from pyedj.compute.event import Event


class Add(Abstract):

    def __call__(self, left, right):
        if type(left) == list and type(right) == list:
            return [Event(l.timestamp, l.sample + r.sample) for l, r in zip(left, right)]
        elif type(left) == list:
            return [Event(e.timestamp, right + e.sample) for e in left]
        elif type(right) == list:
            return [Event(e.timestamp, left + e.sample) for e in right]

        msg = f'One side of operand must a list of Event\n' \
              '    Left  side: {type(left)}\n' \
              '    Right side: {type(right}\n'
        raise SyntaxError(msg)


class Subtract(Abstract):

    def __call__(self, left, right):
        if type(left) == list and type(right) == list:
            return [Event(l.timestamp, l.sample - r.sample) for l, r in zip(left, right)]
        elif type(left) == list:
            return [Event(e.timestamp, e.sample - right) for e in left]
        elif type(right) == list:
            return [Event(e.timestamp, left - e.sample) for e in right]

        msg = f'One side of operand must a list of Event\n' \
              '    Left  side: {type(left)}\n' \
              '    Right side: {type(right}\n'
        raise SyntaxError(msg)


class Multiply(Abstract):

    def __call__(self, left, right):
        if type(left) == list and type(right) == list:
            return [Event(l.timestamp, l.sample * r.sample) for l, r in zip(left, right)]
        elif type(left) == list:
            return [Event(e.timestamp, e.sample * right) for e in left]
        elif type(right) == list:
            return [Event(e.timestamp, left * e.sample) for e in right]

        msg = f'One side of operand must a list of Event\n' \
              '    Left  side: {type(left)}\n' \
              '    Right side: {type(right}\n'
        raise SyntaxError(msg)


class Divide(Abstract):

    def __call__(self, left, right):
        if type(left) == list and type(right) == list:
            return [Event(l.timestamp, l.sample / r.sample) for l, r in zip(left, right)]
        elif type(left) == list:
            return [Event(e.timestamp, e.sample / right) for e in left]
        elif type(right) == list:
            return [Event(e.timestamp, left / e.sample) for e in right]

        msg = f'One side of operand must a list of Event\n' \
              '    Left  side: {type(left)}\n' \
              '    Right side: {type(right}\n'
        raise SyntaxError(msg)