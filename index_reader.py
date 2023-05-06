from elasticsearch import Elasticsearch


class IndexReader:
    def __init__(self, config):
        self.config = config
        self.es = Elasticsearch([{"host": self.config["url"], "port": self.config["port"]}])

    def get_batch(self, start, length):
        response = self.es.search(
            index=self.config["index"],
            body={"query": {
                "match_all": {}},
                "size": length,
                "from": start,
                "_source": ["title", "uri"],
            }
        )
        return response
