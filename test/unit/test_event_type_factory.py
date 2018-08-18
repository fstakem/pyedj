import pytest

from pyedj.compute.event_type_factory import EventTypeFactory


def create_schema(name, field_names, checked):
    schema = {
        'streams': {
            name: {
                'fields': {}
            }
        }
    }

    for f in field_names:
        field = {
            'type': 'integer',
            'default_value': 11,
            'type_check': True,
            'lower_bound': 0
        }
        schema['streams'][name]['fields'][f] = field

    return schema


def test_factory():
    name = 'PirSensor'
    schema = create_schema(name, ['level', 'count'], False)
    Event = EventTypeFactory(name, schema['streams'][name], False)
    event = Event(10, 20)

    assert event.level == 10
    assert event.count == 20

    with pytest.raises(AttributeError):
        event.level = 100


def test_factory_checked_default():
    name = 'PirSensor'
    schema = create_schema(name, ['level', 'count'], True)
    Event = EventTypeFactory(name, schema['streams'][name], True)
    event = Event()

    assert event.level == 11
    assert event.count == 11

    event.level = 100
    event.count = 200

    assert event.level == 100
    assert event.count == 200


def test_factory_checked():
    name = 'PirSensor'
    schema = create_schema(name, ['level', 'count'], True)
    Event = EventTypeFactory(name, schema['streams'][name], True)
    event = Event(level=21, count=22)

    assert event.level == 21
    assert event.count == 22

    event.level = 100
    event.count = 200

    assert event.level == 100
    assert event.count == 200