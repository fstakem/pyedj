from pyedj.compute.fields.field import Field
from pyedj.compute.fields.util import BoundsError


class StrField(Field):
    allowed_types = [str]

    def __init__(self, name, default):
        if type(default) in StrField.allowed_types:
            super().__init__(name, default)
        else:
            msg = 'Cannot set str to type {}'.format(type(default))
            raise TypeError(msg)

    def __set__(self, instance, value):
        if type(value) in StrField.allowed_types:
            setattr(instance, self.name, str(value))
        else:
            msg = 'Cannot set str to type {}'.format(type(value))
            raise TypeError(msg)


class BoundedStrField(Field):
    allowed_types = [str]

    def __init__(self, name, default, lower_bound=None, upper_bound=None):
        self.lower_bound = lower_bound
        self.upper_bound = upper_bound
        self.bounds_msg = '{} < x < {}'.format(self.lower_bound, self.upper_bound)

        if type(default) in BoundedStrField.allowed_types:
            if self.lower_bound is not None or self.upper_bound is not None:
                if self.lower_bound is not None and default < self.lower_bound:
                    msg = "Value({}) is outside the field's bounds: {}".format(default, self.bounds_msg)
                    raise BoundsError(msg)

                if self.upper_bound is not None and default > self.upper_bound:
                    msg = "Value({}) is outside the field's bounds: {}".format(default, self.bounds_msg)
                    raise BoundsError(msg)

            super().__init__(name, default)
        else:
            msg = 'Cannot set str to type {}'.format(type(default))
            raise TypeError(msg)

    def __set__(self, instance, value):
        if type(value) in BoundedStrField.allowed_types:
            if self.lower_bound is not None or self.upper_bound is not None:
                if self.lower_bound is not None and value <= self.lower_bound:
                    msg = "Value({}) is outside the field's bounds: {}".format(value, self.bounds_msg)
                    raise BoundsError(msg)

                if self.upper_bound is not None and value >= self.upper_bound:
                    msg = "Value({}) is outside the field's bounds: {}".format(value, self.bounds_msg)
                    raise BoundsError(msg)

            setattr(instance, self.name, str(value))
        else:
            msg = 'Cannot set str to type {}'.format(type(value))
            raise TypeError(msg)
