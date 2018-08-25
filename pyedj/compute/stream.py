import pyedj.data_store.in_memory
import pyedj.ingestion.deserializers.json
import pyedj.ingestion.enrichers.simple


class StreamError(Exception):
    pass


class Stream(object):
    supported_stores = ['in_memory']
    supported_deserializer = ['json']
    supported_enricher = ['simple']

    def __init__(self, stream_info):
        self.name = stream_info['name']
        self.syncrhonizers = {}
        self.store = Stream.create_store(stream_info)
        self.deserializer = Stream.create_deserializer(stream_info)
        self.enricher = Stream.create_enricher(stream_info)

    @classmethod
    def create_store(cls, stream_info):
        store_type = stream_info['store']['type']

        if store_type not in cls.supported_stores:
            raise StreamError(f'Cannot create store with type({store_type})')

        mod = getattr(pyedj.data_store, store_type)
        klass_name = ''.join([s.capitalize() for s in store_type.split('_')])
        klass = getattr(mod, klass_name)
        store = klass(stream_info)

        return store

    @classmethod
    def create_deserializer(cls, stream_info):
        deserializer_type = stream_info['deserializer']['type']

        if deserializer_type not in cls.supported_deserializer:
            raise StreamError(f'Cannot create deserializer with type({deserializer_type})')

        mod = getattr(pyedj.ingestion.deserializers, deserializer_type)
        klass_name = ''.join([d.capitalize() for d in deserializer_type.split('_')])
        klass = getattr(mod, klass_name)
        deserializer = klass(stream_info)

        return deserializer

    @classmethod
    def create_enricher(cls, stream_info):
        enricher_type = stream_info['enricher']['type']

        if enricher_type not in cls.supported_enricher:
            raise StreamError(f'Cannot create enricher with type({enricher_type})')

        mod = getattr(pyedj.ingestion.enrichers, enricher_type)
        klass_name = ''.join([d.capitalize() for d in enricher_type.split('_')])
        klass = getattr(mod, klass_name)
        enricher = klass(stream_info)

        return enricher

    def add_synchronizer(self, synch):
        self.synchronizers[synch.name] = synch

    def remove_synchronizer(self, name):
        synch = self.syncrhonizers[name]
        del self.syncrhonizers[name]

        return synch

    def get_synchronizer(self, name):
        return self.syncrhonizers[name]

    def num_synchronizers(self):
        return len(self.syncrhonizers)

    def handle_msg(self, msg):
        data = self.deserializer(msg['msg'])
        events = self.enricher(data)
        self.store.add_events(events)
