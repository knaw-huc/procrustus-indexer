"""
Contains a Parser for JSON files.
"""
import json
from typing import IO

import jmespath
from jsonpath_ng import jsonpath, parse

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
        type, path = path.split(":", 1)
        if type == 'jmes':
            return jmespath.search(path, data)
        elif type == 'jsonpath':
            exp = parse(path)
            res = exp.find(data)
            if len(res) > 0:
                return res[0].value
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
