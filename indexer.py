from elasticsearch import Elasticsearch

class Indexer:
    def __init__(self, config):
        self.config = config
        self.es = Elasticsearch([{"host": self.config["url"], "port": self.config["port"]}])

    def add_to_index(self, data):
        result = self.es.index(index = self.config["doc_type"], body = data)
        print(result['result'])

