"""
Contains the Parser ABC.
"""
from abc import ABC
from typing import IO


class Parser(ABC):
    """
    An Abstract Base Class for parsers.
    """
    @staticmethod
    def resolve_path(data: dict, path: str):
        """
        Resolve the path to a specific entry in the data.
        :param data:
        :param path:
        :return:
        """

    def parse_file(self, file: IO) -> dict:
        """
        Process the given file and return a dict with the appropriate fields.
        :param file:
        :return:
        """
