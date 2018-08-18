import pytest
from datetime import datetime, timedelta

from field_helper import create_field
from pyedj.compute.fields.util import BoundsError


# Typed Timestamp
# ---------------------------------------------------------------------------------------------------
def test_typed_timestamp():
    timestamp = datetime.utcnow()
    foo = create_field('timestamp', timestamp, True)
    assert foo.field == timestamp

    timestamp = datetime.utcnow()
    foo.field = timestamp
    assert foo.field == timestamp

    with pytest.raises(TypeError):
        foo.field = 'hello'

    with pytest.raises(TypeError):
        foo = create_field('timestamp', 'hello', True)


# Typed Bounded Timestamp
# ---------------------------------------------------------------------------------------------------
def test_typed_bounded_timestamp():
    timestamp = datetime.utcnow()
    lower = timestamp - timedelta(seconds=5)
    upper = timestamp + timedelta(seconds=5)
    foo = create_field('timestamp', timestamp, True, lower, upper)
    assert foo.field == timestamp

    new_timestamp = timestamp + timedelta(seconds=1)
    foo.field = new_timestamp
    assert foo.field == new_timestamp

    with pytest.raises(TypeError):
        foo.field = 'hello'

    with pytest.raises(TypeError):
        foo = create_field('timestamp', 'hello', True, lower, upper)

    with pytest.raises(BoundsError):
        foo.field = timestamp - timedelta(seconds=6)

    with pytest.raises(BoundsError):
        foo.field = timestamp + timedelta(seconds=6)

    with pytest.raises(BoundsError):
        new_timestamp = timestamp - timedelta(seconds=6)
        foo = create_field('timestamp', new_timestamp, True, lower, upper)

    with pytest.raises(BoundsError):
        new_timestamp = timestamp + timedelta(seconds=6)
        foo = create_field('timestamp', new_timestamp, True, lower, upper)


def test_typed_lower_bounded_timestamp():
    timestamp = datetime.utcnow()
    lower = timestamp - timedelta(seconds=5)
    foo = create_field('timestamp', timestamp, True, l_bound=lower)
    assert foo.field == timestamp

    new_timestamp = timestamp + timedelta(seconds=6)
    foo.field = new_timestamp
    assert foo.field == new_timestamp

    with pytest.raises(TypeError):
        foo.field = 'hello'

    with pytest.raises(TypeError):
        foo = create_field('timestamp', 'hello', True, l_bound=lower)

    with pytest.raises(BoundsError):
        foo.field = timestamp - timedelta(seconds=6)

    with pytest.raises(BoundsError):
        new_timestamp = timestamp - timedelta(seconds=6)
        foo = create_field('timestamp', new_timestamp, True, l_bound=lower)


def test_typed_upper_bounded_timestamp():
    timestamp = datetime.utcnow()
    upper = timestamp + timedelta(seconds=5)
    foo = create_field('timestamp', timestamp, True, u_bound=upper)
    assert foo.field == timestamp

    new_timestamp = timestamp - timedelta(seconds=6)
    foo.field = new_timestamp
    assert foo.field == new_timestamp

    with pytest.raises(TypeError):
        foo.field = 'hello'

    with pytest.raises(TypeError):
        foo = create_field('timestamp', 'hello', True, u_bound=upper)

    with pytest.raises(BoundsError):
        foo.field = timestamp + timedelta(seconds=6)

    with pytest.raises(BoundsError):
        new_timestamp = timestamp + timedelta(seconds=6)
        foo = create_field('timestamp', new_timestamp, True, u_bound=upper)

