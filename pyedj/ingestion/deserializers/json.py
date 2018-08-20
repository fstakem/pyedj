from collections import namedtuple
from datetime import datetime

from pyedj.compute.event_type_factory import EventTypeFactory


class Json(object):

    def __init__(self, stream_info):
        name = stream_info['name']
        schema = stream_info['schema']
        checked = True

        if not stream_info['debug']:
            checked = False

        self.event_class = EventTypeFactory(name, schema, checked)

        if type(self.event_class) == namedtuple:
            self.__call__ = self.parse_namedtuple
        else:
            self.__call__ = self.parse_class

    def parse_namedtuple(self, msg):
        timestamp = datetime.strptime(msg['timestamp'])
        value = None
        event = self.event_class(msg['timestamp'], msg['value'])

        return [event]

    def parse_class(self, msg):
        event = self.event_class(msg['timestamp'], msg['value'])

        return [event]