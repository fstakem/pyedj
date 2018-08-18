import json

from pyedj.compute.fields.int_field import IntField, BoundedIntField
from pyedj.compute.fields.float_field import FloatField, BoundedFloatField
from pyedj.compute.fields.str_field import StrField, BoundedStrField
from pyedj.compute.fields.timestamp_field import TimestampField, BoundedTimestampField
from pyedj.compute.fields.enum_field import EnumField


class FieldFactory(type):

    def __new__(cls, name, schema):
        name = '_' + name
        schema = cls.parse_schema(schema)

        if schema['type'] == 'integer':
            return cls.get_integer(name, schema)
        elif schema['type'] == 'float':
            return cls.get_float(name, schema)
        elif schema['type'] == 'string':
            return cls.get_string(name, schema)
        elif schema['type'] == 'timestamp':
            return cls.get_timestamp(name, schema)
        elif schema['type'] == 'enum':
            return cls.get_enum(name, schema)
        else:
            return None

    def __init__(self, name, schema):
        pass

    @classmethod
    def get_integer(cls, name, schema):
        default_value = schema['default_value']
        bounds_check = ('lower_bound' in schema or 'upper_bound' in schema)

        if schema['type_check'] and bounds_check:
            l_bound, u_bound = cls.get_bounds(schema)
            field_class = BoundedIntField(name, default_value, l_bound, u_bound)

            return field_class

        elif schema['type_check']:
            field_class = IntField(name, default_value)

            return field_class

        else:
            return None

    @classmethod
    def get_float(cls, name, schema):
        default_value = schema['default_value']
        bounds_check = ('lower_bound' in schema or 'upper_bound' in schema)

        if schema['type_check'] and bounds_check:
            l_bound, u_bound = cls.get_bounds(schema)
            field_class = BoundedFloatField(name, default_value, l_bound, u_bound)

            return field_class

        elif schema['type_check']:
            field_class = FloatField(name, default_value)

            return field_class

        else:
            return None

    @classmethod
    def get_string(cls, name, schema):
        default_value = schema['default_value']
        bounds_check = ('lower_bound' in schema or 'upper_bound' in schema)

        if schema['type_check'] and bounds_check:
            l_bound, u_bound = cls.get_bounds(schema)
            field_class = BoundedStrField(name, default_value, l_bound, u_bound)

            return field_class

        elif schema['type_check']:
            field_class = StrField(name, default_value)

            return field_class

        else:
            return None

    @classmethod
    def get_timestamp(cls, name, schema):
        default_value = schema['default_value']
        bounds_check = ('lower_bound' in schema or 'upper_bound' in schema)

        if schema['type_check'] and bounds_check:
            l_bound, u_bound = cls.get_bounds(schema)
            field_class = BoundedTimestampField(name, default_value, l_bound, u_bound)

            return field_class

        elif schema['type_check']:
            field_class = TimestampField(name, default_value)

            return field_class

        else:
            return None

    @classmethod
    def get_enum(cls, name, schema):
        choices = schema['choices']
        default_value = schema['default_value']

        if default_value in choices:
            field_class = EnumField(name, default_value, choices)

            return field_class
        else:
            return None

    @classmethod
    def get_bounds(cls, schema):
        l_bound = None
        u_bound = None

        if 'lower_bound' in schema:
            l_bound = schema['lower_bound']

        if 'upper_bound' in schema:
            u_bound = schema['upper_bound']

        return (l_bound, u_bound)

    @classmethod
    def parse_schema(cls, schema):
        if type(schema) == dict:
            return schema
        elif type(schema) == str:
            try:
                return json.loads(schema)
            except ValueError as ve:
                raise AttributeError('Incorrect schema')
        else:
            raise AttributeError('Unknown schema type')
