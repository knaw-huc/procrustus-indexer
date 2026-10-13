"""
Contains the default mapping of transformation names to classes.
"""
from typing import Dict, Type

from procrustus_indexer.transformations.csv_mapping import CsvMapping
from procrustus_indexer.transformations.transformation import Transformation

DEFAULT_TRANSFORMATIONS: Dict[str, Type[Transformation]] = {
    "csv_mapping": CsvMapping,
}
