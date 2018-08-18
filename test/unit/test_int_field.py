import pytest

from field_helper import create_field
from pyedj.compute.fields.util import BoundsError


# Typed Int
# ---------------------------------------------------------------------------------------------------
def test_typed_int():
    foo = create_field('integer', 2, True)
    assert foo.field == 2

    foo.field = 3.0
    assert foo.field == 3.0

    with pytest.raises(TypeError):
        foo.field = 'hello'

    with pytest.raises(TypeError):
        foo = create_field('integer', 'hello', True)


# Typed Bounded Int
# ---------------------------------------------------------------------------------------------------
def test_typed_bounded_int():
    foo = create_field('integer', 2, True, 0, 10)
    assert foo.field == 2

    foo.field = 3.0
    assert foo.field == 3.0

    with pytest.raises(TypeError):
        foo.field = 'hello'

    with pytest.raises(TypeError):
        foo = create_field('integer', 'hello', True, 0, 10)

    with pytest.raises(BoundsError):
        foo.field = -1

    with pytest.raises(BoundsError):
        foo.field = 11

    with pytest.raises(BoundsError):
        foo = create_field('integer', -1, True, 0, 10)

    with pytest.raises(BoundsError):
        foo = create_field('integer', 11, True, 0, 10)


def test_typed_lower_bounded_int():
    foo = create_field('integer', 2, True, l_bound=0)
    assert foo.field == 2

    foo.field = 12.0
    assert foo.field == 12.0

    with pytest.raises(TypeError):
        foo.field = 'hello'

    with pytest.raises(TypeError):
        foo = create_field('integer', 'hello', True, l_bound=0)

    with pytest.raises(BoundsError):
        foo.field = -1

    with pytest.raises(BoundsError):
        foo = create_field('integer', -1, True, l_bound=0)


def test_typed_upper_bounded_int():
    foo = create_field('integer', 2, True, u_bound=10)
    assert foo.field == 2

    foo.field = -1.0
    assert foo.field == -1.0

    with pytest.raises(TypeError):
        foo.field = 'hello'

    with pytest.raises(TypeError):
        foo = create_field('integer', 'hello', True, u_bound=10)

    with pytest.raises(BoundsError):
        foo.field = 11

    with pytest.raises(BoundsError):
        foo = create_field('integer', 11, True, u_bound=10)

