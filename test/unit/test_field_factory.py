import pytest


from pyedj.compute.fields.field_factory import FieldFactory
from pyedj.compute.fields.util import BoundsError


# def test_typed_int_field():
#     value = 2.0
#     schema = {
#         'type': 'integer',
#         'default_value': value,
#         'type_check': True,
#         'bounds_check': False,
#         'upper_bound': 0,
#         'lower_bound': 0}

#     class Foo(object):
#         field = FieldFactory(schema)
#     foo = Foo()

#     assert foo.field == value
#     assert foo.field != 3
#     assert foo.field < 3
#     assert foo.field > 1
#     assert foo.field <= 2.0
#     assert foo.field >= 2.0

#     new_value = 3
#     foo.field = new_value

#     assert foo.field == new_value

#     with pytest.raises(TypeError):
#         foo.field = 'str'


# def test_bounded_int_field():
#     value = 2.0
#     schema = {
#         'type': 'integer',
#         'default_value': value,
#         'type_check': False,
#         'bounds_check': True,
#         'upper_bound': 5,
#         'lower_bound': 0}

#     class Foo(object):
#         field = FieldFactory(schema)
#     foo = Foo()

#     assert foo.field == value

#     new_value = 3
#     foo.field = new_value

#     assert foo.field == new_value

#     with pytest.raises(BoundsError):
#         foo.field = 0

#     with pytest.raises(BoundsError):
#         foo.field = 10


# def test_typed_bounded_int_field():
#     value = 2.0
#     schema = {
#         'type': 'integer',
#         'default_value': value,
#         'type_check': True,
#         'bounds_check': True,
#         'upper_bound': 5,
#         'lower_bound': 0}

#     class Foo(object):
#         field = FieldFactory(schema)
#     foo = Foo()

#     assert foo.field == value

#     new_value = 3
#     foo.field = new_value

#     assert foo.field == new_value

#     with pytest.raises(TypeError):
#         foo.field = 'str'

#     with pytest.raises(BoundsError):
#         foo.field = 0

#     with pytest.raises(BoundsError):
#         foo.field = 10