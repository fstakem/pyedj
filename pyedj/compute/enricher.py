from datetime import datetime


class Enricher(object):

    def __init__(self, metadata_store):
        self.metadata_store = metadata_store

    def enrich_events(self, events):
        ingest_time = datetime.utcnow()

        return events

    def enrich_block(self, block):
        ingest_time = datetime.utcnow()

        return block