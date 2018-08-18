import pytest

from field_helper import create_enum_field


# Enum
# ---------------------------------------------------------------------------------------------------
def test_enum():
    foo = create_enum_field(0, choices = [0, 1, 2, 3])
    assert foo.field == 0

    foo.field = 2
    assert foo.field == 2


def test_enum_bounds():
    foo = create_enum_field(0, choices = [0, 1, 2, 3])
    assert foo.field == 0

    with pytest.raises(TypeError):
        foo.field = -1

