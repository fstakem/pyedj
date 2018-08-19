import pyedj.data_store


class StreamError(Exception):
    pass


class Stream(object):
    supported_stores = ['in_memory']

    def __init__(self, stream_info):
        self.name = stream_info['name']
        self.ingress_enricher = None
        self.syncrhonizers = {}
        self.store = Stream.create_store(stream_info)

    @classmethod
    def create_store(cls, stream_info):
        store = stream_info['store_type']

        if store not in cls.supported_stores:
            raise StreamError(f'Cannot create store with type({store})')

        mod = getattr(pyedj.data_store, store)
        klass_name = ''.join([s.capitalize() for s in store.split('_')])
        klass = getattr(mod, klass_name)
        store = klass(stream_info)

        return store

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
        pass
