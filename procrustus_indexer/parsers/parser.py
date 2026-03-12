"""
Contains the Parser ABC.
"""
from abc import ABC
from typing import IO, List


class Parser(ABC):
    """
    An Abstract Base Class for parsers.
    """
    config: dict

    def __init__(self, config: dict) -> None:
        format = config['index']['input']['format']
        if format not in self.supported_types():
            raise ValueError(f"Format {format} not supported")
        self.config = config

    def supported_types(self) -> List[str]:
        """
        Return the supported types for this parser
        """

    def should_process(self, file: IO) -> bool:
        """
        Check if a file should be processed or not.
        :param file:
        :return:
        """

    def parse_file(self, file: IO) -> dict:
        """
        Process the given file and return a dict with the appropriate fields.
        :param file:
        :return:
        """
