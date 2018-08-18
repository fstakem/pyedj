import pytest

from field_helper import create_field
from pyedj.compute.fields.util import BoundsError


# Typed Float
# ---------------------------------------------------------------------------------------------------
def test_typed_float():
    foo = create_field('float', 2.0, True)
    assert foo.field == 2.0

    foo.field = 3
    assert foo.field == 3

    with pytest.raises(TypeError):
        foo.field = 'hello'

    with pytest.raises(TypeError):
        foo = create_field('float', 'hello', True)


# Typed Bounded Float
# ---------------------------------------------------------------------------------------------------
def test_typed_bounded_float():
    foo = create_field('float', 2.0, True, 0.0, 10.0)
    assert foo.field == 2.0

    foo.field = 3
    assert foo.field == 3

    with pytest.raises(TypeError):
        foo.field = 'hello'

    with pytest.raises(TypeError):
        foo = create_field('float', 'hello', True, 0.0, 10.0)

    with pytest.raises(BoundsError):
        foo.field = -1.0

    with pytest.raises(BoundsError):
        foo.field = 11.0

    with pytest.raises(BoundsError):
        foo = create_field('float', -1.0, True, 0.0, 10.0)

    with pytest.raises(BoundsError):
        foo = create_field('float', 11.0, True, 0.0, 10.0)


def test_typed_lower_bounded_float():
    foo = create_field('float', 2.0, True, l_bound=0.0)
    assert foo.field == 2.0

    foo.field = 12
    assert foo.field == 12

    with pytest.raises(TypeError):
        foo.field = 'hello'

    with pytest.raises(TypeError):
        foo = create_field('float', 'hello', True, l_bound=0.0)

    with pytest.raises(BoundsError):
        foo.field = -1.0

    with pytest.raises(BoundsError):
        foo = create_field('float', -1.0, True, l_bound=0.0)


def test_typed_upper_bounded_float():
    foo = create_field('float', 2.0, True, u_bound=10.0)
    assert foo.field == 2.0

    foo.field = -1
    assert foo.field == -1

    with pytest.raises(TypeError):
        foo.field = 'hello'

    with pytest.raises(TypeError):
        foo = create_field('float', 'hello', True, u_bound=10.0)

    with pytest.raises(BoundsError):
        foo.field = 11.0

    with pytest.raises(BoundsError):
        foo = create_field('float', 11.0, True, u_bound=10.0)

