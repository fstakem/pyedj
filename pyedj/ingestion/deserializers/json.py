from datetime import datetime
import json

from pyedj.compute.event_type_factory import EventTypeFactory


class Json(object):

    def __init__(self, service_info):
        self.name = service_info['name']
        self.schema = service_info['schema']
        self.checked = True
        self.types = {}
        self.parsers = {}
        self.fields = []
        self.event_class = None

        self.parse_service_info(service_info)

    def parse_service_info(self, service_info):
        if not service_info['debug']:
            self.checked = False

        fields = service_info['schema']['fields']

        for k, v in fields.items():
            if v['type'] == 'timestamp':
                self.parsers[k] = v['parser']

            self.types[k] = v['type']
            self.fields.append(k)

        self.event_class = EventTypeFactory(self.name, self.schema, self.checked)

    def __call__(self, msg):
        fields = self.parse_msg(msg)
        event = self.event_class(**fields)

        return [event]

    def parse_msg(self, msg):
        out = {}
        data = json.loads(msg)

        for f in self.fields:
            field_type = self.types[f]
            field = data[f]

            if field_type == 'timestamp':
                out[f] = datetime.strptime(field, self.parsers[f])
            elif field_type == 'float':
                out[f] = float(field)
            elif field_type == 'int':
                out[f] = int(field)
            else:
                raise Exception('Unknown field type')

        return out