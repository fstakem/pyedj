from pyedj.compute.fields.field import Field
from pyedj.compute.fields.util import BoundsError


class EnumField(Field):

    def __init__(self, name, default, choices):
        self.choices = choices
        super().__init__(name, default)

    def __set__(self, instance, value):
        if value in self.choices:
            setattr(instance, self.name, value)
        else:
            msg = 'Value({}) not in enum'.format(value)
            raise TypeError(msg)
