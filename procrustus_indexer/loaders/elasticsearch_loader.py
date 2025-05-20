from elasticsearch.helpers import bulk

from procrustus_indexer.loaders import Loader
from elasticsearch import Elasticsearch

class ElasticsearchLoader(Loader):
    """
    Loads data into an Elasticsearch index.
    """

    es: Elasticsearch = None
    config: dict
    index_name: str
    overwrite: bool = True
    id_prop: str = "_id"

    def __init__(self, es: Elasticsearch, config: dict, index_name: str, id_prop: str = "_id"):
        self.es = es
        self.config = config
        self.index_name = index_name
        self.id_prop = id_prop


    def setup(self) -> None:
        """
        Create the elasticsearch index mapping according to config and return resulting dict.
        :return:
        """
        if self.overwrite:
            self.es.indices.delete(index=self.index_name, ignore=[400, 404])

        properties = {}
        for facet_name in self.config['index']['facet'].keys():
            facet = self.config["index"]["facet"][facet_name]
            property_type = facet.get('type', 'text')
            if property_type == 'text':
                properties[facet_name] = {
                    'type': 'text',
                    'fields': {
                        'keyword': {
                            'type': 'keyword',
                            'ignore_above': 256
                        },
                    }
                }
            elif property_type == 'keyword':
                properties[facet_name] = {
                    'type': 'keyword',
                }
            elif property_type == 'number':
                properties[facet_name] = {
                    'type': 'integer',
                }
            elif property_type == 'date':
                properties[facet_name] = {
                    'type': 'date',
                }

        mappings = {
            'properties': properties
        }

        settings = {
            'number_of_shards': 1,
            'number_of_replicas': 0
        }

        self.es.indices.create(index=self.index_name, mappings=mappings, settings=settings)


    def load(self, data: dict):
        """
        Index a single document.
        :param data:
        :return:
        """
        self.es.index(index=self.index_name, id=data[self.id_prop], document=data)


    def load_bulk(self, data: list[dict]):
        """
        Bulk insert data into Elasticsearch.
        :param data:
        :return:
        """
        actions = [{'_index': self.index_name,
             '_id': item[self.id_prop],
             '_source': item} for item in data]

        bulk(self.es, actions)
