from collections import namedtuple

from pyedj.compute.fields.field_factory import FieldFactory


class EventTypeFactory(type):

    def __new__(self, name, schema, checked=True):
        name = name + 'Event'
        fields = [k for k, v in schema['fields'].items()]

        if checked:
            slot_vars = ['_' + k for k, v in schema['fields'].items()]
            slots = {'__slots__': slot_vars}
            inst_vars = {k: FieldFactory(k, v) for k, v in schema['fields'].items()}
            all_attrs = {**slots, **inst_vars}
            X = type(name, (object,), all_attrs)

            def init(self, *args, **kwargs):
                x = {k: v for k, v in self.__class__.__dict__.items() if not k.startswith('__') and k not in self.__slots__}

                for k, v in x.items():
                    setattr(self, k, v.default)

                for k, v in kwargs.items():
                    setattr(self, k, v)

            X.__init__ = init

        else:
            X = namedtuple(name, fields)

        return X

    def __init__(self, name, schema, checked=True):
        pass