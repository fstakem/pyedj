

class Stream(object):

    def __init__(self, name, data_store, enricher):
        self.name = name
        self.data_store = data_store
        self.ingress_enricher = enricher
        self.syncrhonizers = {}

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

    def add_events(self, events):
        enriched_events = self.ingress_enricher.enriched_events(events)
        self.data_store.add_events(enriched_events)

    def add_interleaved_events(self, events):
        enriched_events = self.ingress_enricher.enriched_events(events)
        self.data_store.add_interleaved_events(enriched_events)

    def add_block(self, block):
        enriched_block = self.ingress_enricher.enriched_block(block)
        self.data_store.add_block(enriched_block)

    def get_events(self, window_len_ms):
        return self.data_store.get_events(window_len_ms)

    def get_n_events(self, n):
        return self.data_store.get_n_events(n)

    def trim_store(self, trim_size_ms):
        self.data_store.trim(trim_size_ms)
