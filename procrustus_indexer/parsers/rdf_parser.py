"""
Contains a Parser for RDF files.
"""
import json
from typing import IO

import jmespath
from jsonpath_ng import jsonpath, parse

from procrustus_indexer.parsers import Parser

from rdflib import Graph

class RdfParser(Parser):
    """
    RDF-specific Parser class.
    """
    config: dict

    def __init__(self, config: dict):
        if config["index"]["input"]["format"] != "rdf":
            raise ValueError("RdfParser only supports RDF files")
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
        g = Graph()
        g.parse(file)
        path_id_query = self.config['index']['id']['path']
        query_res  = g.query(path_id_query)
        for row in query_res :
            path_id = str(row.n)
        doc = { 'id': path_id }
        for key in self.config['index']['facet'].keys():
            qres = g.query(self.config["index"]["facet"][key]['path'])
            for row in qres:
                try:
                    doc[key] = str(row.n)
                except:
                    pass
        return doc
