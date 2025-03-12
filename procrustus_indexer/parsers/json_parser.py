"""
Contains a Parser for JSON files.
"""
import json
from typing import IO

import jmespath

from procrustus_indexer.parsers import Parser

class JsonParser(Parser):
    """
    JSON-specific Parser class.
    """
    config: dict

    def __init__(self, config: dict):
        if config["index"]["input"]["format"] != "json":
            raise ValueError("JsonParser only supports JSON files")
        self.config = config


    @staticmethod
    def resolve_path(data: dict, path: str):
        if path.startswith("jmes:"):
            # for jmes: 5:
            return jmespath.search(path[5:], data)
        return None


    def parse_file(self, file: IO) -> dict:
        data = json.load(file)

        path_id = self.config['index']['id']['path']
        doc_id = self.resolve_path(data, path_id)
        doc = {'id': doc_id}
        for key in self.config['index']['facet'].keys():
            facet = self.config["index"]["facet"][key]
            path_facet = facet["path"]
            doc[key] = self.resolve_path(data, path_facet)
        return doc
