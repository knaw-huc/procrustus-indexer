"""
Mapping transformation. Takes a csv file with two columns, and when a value from col 1 is
encountered, returns the value from col 2.
"""
import csv
from typing import Dict

from procrustus_indexer.transformations.transformation import Transformation


class CsvMapping(Transformation):
    """
    Maps values from a csv column to another csv column.
    """
    mapping: Dict[str, str]

    def __init__(self, csv_file: str):
        """
        Constructor.
        :param csv_file:
        """
        mapping = {}

        with open(csv_file, encoding="utf-8") as f:
            reader = csv.reader(f, delimiter=";")
            for row in reader:
                mapping[row[0]] = row[1]

        self.mapping = mapping

    def transform(self, value: str) -> str:
        """
        Apply the transformation to value.
        :param value:
        :return:
        """
        if value in self.mapping:
            return self.mapping[value]
        return value
