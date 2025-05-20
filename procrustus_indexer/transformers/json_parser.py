import json

from typing import Optional

import jmespath
from jsonpath_ng import parse

from procrustus_indexer.transformers import Transformer

class JsonParser(Transformer):
    """
    Parses a JSON and returns a dict. Can be configured to return a specific mapping of the
    original JSON.
    """
    mapping: Optional[dict] = None

    def __init__(self, mapping: dict | None = None):
        self.mapping = mapping

    @staticmethod
    def resolve_path(data: dict, path: str):
        """
        Resolve
        :param data:
        :param path:
        :return:
        """
        type, path = path.split(":", 1)
        if type == 'jmes':
            return jmespath.search(path, data)
        if type == 'jsonpath':
            exp = parse(path)
            res = exp.find(data)
            if len(res) > 0:
                return res[0].value
        return None

    def resolve_dict(self, data: dict, paths: dict):
        """

        :param data:
        :param paths:
        :return:
        """
        tmp = {}
        for key, value in paths.items():
            if type(value) == dict:
                tmp[key] = self.resolve_dict(data, value)
            else:
                tmp[key] = self.resolve_path(data, value)
        return tmp


    def transform(self, x: str) -> dict:
        """
        Transforms JSON and returns a dict.
        :param x:
        :return:
        """
        data = json.loads(x)

        if self.mapping is None:
            return data

        return self.resolve_dict(data, self.mapping)
