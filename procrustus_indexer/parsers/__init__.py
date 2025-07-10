"""
Parsers for the indexer. Contains an ABC and implementation-dependend parsers.
"""
from .parser import Parser
from .json_parser import JsonParser
from .xml_parser import XmlParser
from .rdf_parser import RdfParser
