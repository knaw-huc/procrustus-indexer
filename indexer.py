from elasticsearch import Elasticsearch

class Indexer:
    def __init__(self, config):
        self.config = config
        self.es = Elasticsearch([{"host": self.config["url"], "port": self.config["port"]}])

    # Insert single document to index
    def add_to_index(self, data):
        result = self.es.index(index = self.config["index"], body = data)

    # Insert set of documents to index
    def bulk_to_index(self, data):
        result = self.es.bulk(index = self.config["index"], body=data, refresh='wait_for');

    def add_to_index_with_id(self, data, data_id):
        result = self.es.index(index = self.config["index"], body = data, id=data_id)

