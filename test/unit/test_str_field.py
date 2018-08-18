import pytest

from field_helper import create_field
from pyedj.compute.fields.util import BoundsError


# Typed Str
# ---------------------------------------------------------------------------------------------------
def test_typed_str():
    foo = create_field('string', 'test', True)
    assert foo.field == 'test'

    foo.field = 'other'
    assert foo.field == 'other'

    with pytest.raises(TypeError):
        foo.field = 1

    with pytest.raises(TypeError):
        foo = create_field('string', 2, True)


# Typed Bounded Str
# ---------------------------------------------------------------------------------------------------
def test_typed_bounded_str():
    foo = create_field('string', 'test', True, 'te', 'testtt')
    assert foo.field == 'test'

    foo.field = 'testt'
    assert foo.field == 'testt'

    with pytest.raises(TypeError):
        foo.field = 1

    with pytest.raises(TypeError):
        foo = create_field('string', 1, True, 'te', 'testtt')

    with pytest.raises(BoundsError):
        foo.field = 't'

    with pytest.raises(BoundsError):
        foo.field = 'testttt'

    with pytest.raises(BoundsError):
        foo = create_field('string', 't', True, 'te', 'testtt')

    with pytest.raises(BoundsError):
        foo = create_field('string', 't', True, 'te', 'testtt')


def test_typed_lower_bounded_str():
    foo = create_field('string', 'test', True, l_bound='te')
    assert foo.field == 'test'

    foo.field = 'testt'
    assert foo.field == 'testt'

    with pytest.raises(TypeError):
        foo.field = 1

    with pytest.raises(TypeError):
        foo = create_field('string', 1, True, l_bound='te')

    with pytest.raises(BoundsError):
        foo.field = 't'

    with pytest.raises(BoundsError):
        foo = create_field('string', 't', True, l_bound='te')


def test_typed_upper_bounded_str():
    foo = create_field('string', 'test', True, u_bound='testtt')
    assert foo.field == 'test'

    foo.field = 'tes'
    assert foo.field == 'tes'

    with pytest.raises(TypeError):
        foo.field = 1

    with pytest.raises(TypeError):
        foo = create_field('string', 1, True, u_bound='testtt')

    with pytest.raises(BoundsError):
        foo.field = 'testttt'

    with pytest.raises(BoundsError):
        foo = create_field('string', 'testttt', True, u_bound='testtt')

