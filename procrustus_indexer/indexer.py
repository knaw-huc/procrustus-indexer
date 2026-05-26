"""
Indexer class
"""
import glob

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, BulkIndexError
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
    required_tokenizers: dict

    def __init__(self, es: Elasticsearch, config: dict, parser: Parser, index_name: str):
        self.es = es
        self.config = config
        self.index_name = index_name
        self.parser = parser
        self.required_tokenizers = {}


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
            elif property_type == 'date_range':
                properties[facet_name] = {
                    'type': 'date_range',
                }
            elif property_type == 'tree_pipe':
                properties[facet_name] = {
                    'type': 'text',
                    'fields': {
                        'keyword': {
                            'type': 'text',
                            'fielddata': True,
                            'analyzer': 'tree_pipe',
                        },
                    }
                }
                self.required_tokenizers['tree_pipe'] = True
            elif property_type == 'tree_slash':
                properties[facet_name] = {
                    'type': 'text',
                    'fields': {
                        'keyword': {
                            'type': 'text',
                            'fielddata': True,
                            'analyzer': 'tree_slash',
                        },
                    }
                }

                self.required_tokenizers['tree_slash'] = True

        mappings = {
            'properties': properties
        }

        settings = {
            'number_of_shards': 1,
            'number_of_replicas': 0,
            'analysis': {
                'analyzer': self.get_analyzers(),
                'tokenizer': self.get_tokenizers(),
                'filter': {},
            }
        }

        self.es.indices.create(index=self.index_name, mappings=mappings, settings=settings)
        return mappings


    def get_analyzers(self):
        """
        Get definitions for required analyzers
        :return:
        """
        tmp = {}
        if 'tree_pipe' in self.required_tokenizers:
            tmp['tree_pipe'] = {
                "tokenizer": "tree_pipe_tokenizer"
            }
        if 'tree_slash' in self.required_tokenizers:
            tmp['tree_slash'] = {
                "tokenizer": "tree_slash_tokenizer"
            }
        return tmp


    def get_tokenizers(self):
        """
        Get definitions for required tokenizers
        :return:
        """
        tmp = {}
        if 'tree_pipe' in self.required_tokenizers:
            tmp['tree_pipe_tokenizer'] = {
                "type": "path_hierarchy",
                "delimiter": "|"
            }
        if 'tree_slash' in self.required_tokenizers:
            tmp['tree_slash_tokenizer'] = {
                "type": "path_hierarchy",
                "delimiter": "/"
            }
        return tmp


    def import_files(self, files: list[str]):
        """
        Import files into an elasticsearch index based on the given config.
        :param files: list of files to import
        :param index: Elasticsearch index
        :return:
        """
        actions = []
        for inf in files:
            doc = {}
            with open(inf, encoding='utf-8') as f:
                if not self.parser.should_process(f):
                    continue
                doc = self.parser.parse_file(f)
                actions.append({'_index': self.index_name, '_id': doc['id'], '_source': doc})
        # add to index:
        try:
            bulk(self.es, actions)
        except BulkIndexError as e:
            print(e)
            for error in e.errors:
                print(error)


    def import_folder(self, folder: str):
        """
        Import all files in a folder.
        :param folder:
        :return:
        """
        input_list = glob.glob(f'{folder}/*')
        self.import_files(input_list)
