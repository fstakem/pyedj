

class Field(object):

    __slots__ = ['name', 'default']

    def __init__(self, name, default):
        self.name = name
        self.default = default

    def __get__(self, instance, owner):
        return getattr(instance, self.name)

    def __set__(self, instance, value):
        setattr(instance, self.name, value)
