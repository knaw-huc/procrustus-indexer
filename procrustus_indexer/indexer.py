"""
Indexer class
"""
import glob

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from procrustus_indexer.parsers import Parser


class Indexer:
    """
    Class used for creating Elasticsearch indices and indexing documents into them based on a toml
    configuration.
    """

    es: Elasticsearch = None
    config: dict
    index_name: str
    parser: Parser

    def __init__(self, es: Elasticsearch, config: dict, parser: Parser, index_name: str):
        self.es = es
        self.config = config
        self.index_name = index_name
        self.parser = parser


    def create_mapping(self, overwrite: bool = False) -> dict:
        """
        Create the elasticsearch index mapping according to config and return resulting dict.
        :return:
        """
        if overwrite:
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
        return mappings


    def import_files(self, files: list[str]):
        """
        Import files into an elasticsearch index based on the given config.
        :param files: list of files to import
        :param index: Elasticsearch index
        :return:
        """
        actions = []
        for inv in files:
            doc = {}
            with open(inv, encoding='utf-8') as f:
                doc = self.parser.parse_file(f)
                actions.append({'_index': self.index_name, '_id': doc['id'], '_source': doc})
        # add to index:
        bulk(self.es, actions)


    def import_folder(self, folder: str):
        """
        Import all files in a folder.
        :param folder:
        :return:
        """
        input_list = glob.glob(f'{folder}/*')
        self.import_files(input_list)
