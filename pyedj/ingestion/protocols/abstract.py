from abc import ABC, abstractmethod
from importlib import import_module


class ProtocolError(Exception):
    pass


class Abstract(ABC):
    supported_deserializer = ['json']

    def __init__(self, service_info):
        self.name = service_info['name']
        self.service_info = service_info
        self.streams = {}
        self.deserializer = Abstract.create_deserializer(service_info)

    @abstractmethod
    def start(self):
        """Start receiving data."""
        return

    @abstractmethod
    def stop(self):
        """Stop receiving data."""
        return

    def handle_msg(self, msg):
        return self.deserializer(msg['msg'])

    @classmethod
    def create_deserializer(cls, service_info):
        deserializer_type = service_info['deserializer']['type']

        if deserializer_type not in cls.supported_deserializer:
            raise ProtocolError(f'Cannot create deserializer with type({deserializer_type})')

        mod = import_module('pyedj.ingestion.deserializers.' + deserializer_type)
        klass_name = ''.join([d.capitalize() for d in deserializer_type.split('_')])
        klass = getattr(mod, klass_name)
        deserializer = klass(service_info)

        return deserializer
