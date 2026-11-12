import tomllib
from unittest.mock import Mock, patch, mock_open
from procrustus_indexer.indexer_builder import build_indexer
from procrustus_indexer.parsers import JsonParser, XmlParser
from procrustus_indexer import Indexer


def test_build_indexer_with_json_format():
    config_data = {
        'index': {
            'input': {'format': 'json'},
            'id': {'path': 'jmes:id'},
            'facet': {}
        }
    }
    
    with patch('builtins.open', mock_open(read_data=b'[index]\ninput.format = "json"')):
        with patch('tomllib.load', return_value=config_data):
            es_client = Mock()
            indexer = build_indexer('config.toml', 'test-index', es_client)
            
            assert isinstance(indexer, Indexer)
            assert isinstance(indexer.parser, JsonParser)
            assert indexer.index_name == 'test-index'
            assert indexer.es == es_client


def test_build_indexer_with_xml_format():
    config_data = {
        'index': {
            'input': {'format': 'xml', 'ns': {}},
            'id': {'path': 'jmes:id'},
            'facet': {}
        }
    }
    
    with patch('builtins.open', mock_open(read_data=b'[index]\ninput.format = "xml"')):
        with patch('tomllib.load', return_value=config_data):
            es_client = Mock()
            indexer = build_indexer('config.toml', 'test-index', es_client)
            
            assert isinstance(indexer, Indexer)
            assert isinstance(indexer.parser, XmlParser)
            assert indexer.index_name == 'test-index'
            assert indexer.es == es_client


def test_build_indexer_invalid_format():
    config_data = {
        'index': {
            'input': {'format': 'yaml'},
            'id': {'path': 'jmes:id'},
            'facet': {}
        }
    }
    
    with patch('builtins.open', mock_open(read_data=b'[index]\ninput.format = "yaml"')):
        with patch('tomllib.load', return_value=config_data):
            es_client = Mock()
            
            try:
                build_indexer('config.toml', 'test-index', es_client)
                assert False, "Should have raised ValueError"
            except ValueError as e:
                assert "Invalid input format" in str(e)
                assert "yaml" in str(e)


def test_build_indexer_reads_config_file():
    config_data = {
        'index': {
            'input': {'format': 'json'},
            'id': {'path': 'jmes:id'},
            'facet': {}
        }
    }
    
    mock_file = mock_open(read_data=b'fake toml content')
    with patch('builtins.open', mock_file):
        with patch('tomllib.load', return_value=config_data):
            es_client = Mock()
            build_indexer('config.toml', 'test-index', es_client)
            
            mock_file.assert_called_once_with('config.toml', 'rb')
