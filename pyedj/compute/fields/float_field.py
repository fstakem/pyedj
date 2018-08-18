from pyedj.compute.fields.field import Field
from pyedj.compute.fields.util import BoundsError


class FloatField(Field):
    allowed_types = [int, float]

    def __init__(self, name, default):
        if type(default) in FloatField.allowed_types:
            super().__init__(name, float(default))
        else:
            msg = 'Cannot set float to type {}'.format(type(default))
            raise TypeError(msg)

    def __set__(self, instance, value):
        if type(value) in FloatField.allowed_types:
            setattr(instance, self.name, float(value))
        else:
            msg = 'Cannot set float to type {}'.format(type(value))
            raise TypeError(msg)


class BoundedFloatField(Field):
    allowed_types = [int, float]

    def __init__(self, name, default, lower_bound=None, upper_bound=None):
        self.lower_bound = lower_bound
        self.upper_bound = upper_bound
        self.bounds_msg = '{} < x < {}'.format(self.lower_bound, self.upper_bound)

        if type(default) in BoundedFloatField.allowed_types:
            if self.lower_bound is not None or self.upper_bound is not None:
                if self.lower_bound is not None and default < self.lower_bound:
                    msg = "Value({}) is outside the field's bounds: {}".format(default, self.bounds_msg)
                    raise BoundsError(msg)

                if self.upper_bound is not None and default > self.upper_bound:
                    msg = "Value({}) is outside the field's bounds: {}".format(default, self.bounds_msg)
                    raise BoundsError(msg)

            super().__init__(name, float(default))
        else:
            msg = 'Cannot set float to type {}'.format(type(default))
            raise TypeError(msg)

    def __set__(self, instance, value):
        if type(value) in BoundedFloatField.allowed_types:
            if self.lower_bound is not None or self.upper_bound is not None:
                if self.lower_bound is not None and value <= self.lower_bound:
                    msg = "Value({}) is outside the field's bounds: {}".format(value, self.bounds_msg)
                    raise BoundsError(msg)

                if self.upper_bound is not None and value >= self.upper_bound:
                    msg = "Value({}) is outside the field's bounds: {}".format(value, self.bounds_msg)
                    raise BoundsError(msg)

            setattr(instance, self.name, float(value))
        else:
            msg = 'Cannot set int to type {}'.format(type(value))
            raise TypeError(msg)
