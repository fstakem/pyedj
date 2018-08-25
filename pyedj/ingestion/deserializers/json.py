from collections import namedtuple
from datetime import datetime
import json

from pyedj.compute.event_type_factory import EventTypeFactory


class Json(object):

    def __init__(self, stream_info):
        self.name = stream_info['name']
        self.schema = stream_info['schema']
        self.checked = True
        self.types = {}
        self.parsers = {}
        self.fields = []

        if not stream_info['debug']:
            self.checked = False

        fields = stream_info['schema']['fields']

        for k, v in fields.items():
            if v['type'] == 'timestamp':
                self.parsers[k] = v['parser']

            self.types[k] = v['type']
            self.fields.append(k)

        self.event_class = EventTypeFactory(self.name, self.schema, self.checked)

        if type(self.event_class) == namedtuple:
            self.__class__.__call__ = self.parse_namedtuple
        else:
            self.__class__.__call__ = self.parse_class

    def parse_namedtuple(self, msg):
        fields = self.parse_msg(msg)
        event = self.event_class(*fields)

        return [event]

    def parse_class(self, msg):
        fields = self.parse_msg(msg)
        event = self.event_class(*fields)

        return [event]

    def parse_msg(self, msg):
        out = []
        data = json.loads(msg)

        for f in self.fields:
            field_type = self.types[f]
            field = data[f]

            if field_type == 'timestamp':
                o = datetime.strptime(field, self.parsers[f])
                out.append(o)
            elif field_type == 'float':
                out.append(float(field))
            elif field_type == 'int':
                out.append(int(field))
            else:
                raise Exception('Unknown field type')

        return out