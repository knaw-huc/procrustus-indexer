"""
Builder for the Indexer class. Creates all requried components based on the configuration and
returns the final Indexer.
"""
import tomllib

from elasticsearch import Elasticsearch
from procrustus_indexer import Indexer
from procrustus_indexer.parsers import JsonParser


def build_indexer(config_file: str, es_index: str, es_client: Elasticsearch) -> Indexer:
    """
    Build a new Indexer based on the given config
    :param config_file: Location of the config toml file
    :param es_index: The name of the Elasticsearch index
    :param es_client: The Elasticsearch client
    :return:
    """
    with open(config_file, "rb") as f:
        config = tomllib.load(f)

    input_format = config["index"]["input"]["format"]
    if input_format == "json":
        parser = JsonParser(config)
    else:
        raise ValueError(f"Invalid input format '{input_format}'")

    return Indexer(es_client, config, parser, es_index)
