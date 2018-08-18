from pyedj.compute.fields.field_factory import FieldFactory


def create_field(type_field, value, typed=False, l_bound=None, u_bound=None):
    name = 'field'
    schema = {
        'type': type_field,
        'default_value': value,
        'type_check': typed}

    if l_bound is not None:
        schema['lower_bound'] = l_bound

    if u_bound is not None:
        schema['upper_bound'] = u_bound

    class Foo(object):
        field = FieldFactory(name, schema)
        __slots__ = ['_field']

        def __init__(self):
            self._field = value

    foo = Foo()

    return foo


def create_enum_field(value, choices):
    name = 'field'
    schema = {
        'type': 'enum',
        'default_value': value,
        'choices': choices}

    class Foo(object):
        field = FieldFactory(name, schema)
        __slots__ = ['_field']

        def __init__(self):
            self._field = value
    foo = Foo()

    return foo
