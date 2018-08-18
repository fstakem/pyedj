from collections import namedtuple


Event = namedtuple('Event', 'timestamp sample')


class Timestamp(object):

    @classmethod
    def at(cls, event, other):
        if event.timestamp == other.timestamp:
            return True

        return False

    @classmethod
    def not_at(cls, event, other):
        if event.timestamp != other.timestamp:
            return True

        return False

    @classmethod
    def before(cls, event, other):
        if event.timestamp < other.timestamp:
            return True

        return False

    @classmethod
    def after(cls, event, other):
        if event.timestamp > other.timestamp:
            return True

        return False

    @classmethod
    def before_or_at(cls, event, other):
        if event.timestamp <= other.timestamp:
            return True

        return False

    @classmethod
    def after_or_at(cls, event, other):
        if event.timestamp >= other.timestamp:
            return True

        return False