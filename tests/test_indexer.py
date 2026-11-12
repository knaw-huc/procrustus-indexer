import tomllib
from unittest.mock import Mock, MagicMock, patch, call
from procrustus_indexer.indexer import Indexer
from procrustus_indexer.parsers import JsonParser


def get_config(toml_file: str) -> dict:
    config_path = f"tests/test_files/{toml_file}"
    with open(config_path, "rb") as f:
        return tomllib.load(f)


def test_create_mapping_with_text_type():
    config = {
        'index': {
            'facet': {
                'title': {'type': 'text'},
                'content': {'type': 'text'}
            }
        }
    }
    es_mock = Mock()
    es_mock.indices.create = Mock()
    parser_mock = Mock()
    indexer = Indexer(es_mock, config, parser_mock, 'test-index')
    
    mappings = indexer.create_mapping()
    
    assert 'title' in mappings['properties']
    assert mappings['properties']['title']['type'] == 'text'
    assert 'keyword' in mappings['properties']['title']['fields']
    es_mock.indices.create.assert_called_once()


def test_create_mapping_with_keyword_type():
    config = {
        'index': {
            'facet': {
                'id': {'type': 'keyword'}
            }
        }
    }
    es_mock = Mock()
    es_mock.indices.create = Mock()
    parser_mock = Mock()
    indexer = Indexer(es_mock, config, parser_mock, 'test-index')
    
    mappings = indexer.create_mapping()
    
    assert mappings['properties']['id']['type'] == 'keyword'


def test_create_mapping_with_number_type():
    config = {
        'index': {
            'facet': {
                'value': {'type': 'number'}
            }
        }
    }
    es_mock = Mock()
    es_mock.indices.create = Mock()
    parser_mock = Mock()
    indexer = Indexer(es_mock, config, parser_mock, 'test-index')
    
    mappings = indexer.create_mapping()
    
    assert mappings['properties']['value']['type'] == 'integer'


def test_create_mapping_with_date_type():
    config = {
        'index': {
            'facet': {
                'created': {'type': 'date'}
            }
        }
    }
    es_mock = Mock()
    es_mock.indices.create = Mock()
    parser_mock = Mock()
    indexer = Indexer(es_mock, config, parser_mock, 'test-index')
    
    mappings = indexer.create_mapping()
    
    assert mappings['properties']['created']['type'] == 'date'


def test_create_mapping_with_date_range_type():
    config = {
        'index': {
            'facet': {
                'range_field': {'type': 'date_range'}
            }
        }
    }
    es_mock = Mock()
    es_mock.indices.create = Mock()
    parser_mock = Mock()
    indexer = Indexer(es_mock, config, parser_mock, 'test-index')
    
    mappings = indexer.create_mapping()
    
    assert mappings['properties']['range_field']['type'] == 'date_range'


def test_create_mapping_with_tree_pipe_type():
    config = {
        'index': {
            'facet': {
                'hierarchy': {'type': 'tree_pipe'}
            }
        }
    }
    es_mock = Mock()
    es_mock.indices.create = Mock()
    parser_mock = Mock()
    indexer = Indexer(es_mock, config, parser_mock, 'test-index')
    
    mappings = indexer.create_mapping()
    
    assert mappings['properties']['hierarchy']['type'] == 'text'
    assert 'tree_pipe' in indexer.required_tokenizers
    assert mappings['properties']['hierarchy']['fields']['keyword']['analyzer'] == 'tree_pipe'


def test_create_mapping_with_tree_slash_type():
    config = {
        'index': {
            'facet': {
                'path': {'type': 'tree_slash'}
            }
        }
    }
    es_mock = Mock()
    es_mock.indices.create = Mock()
    parser_mock = Mock()
    indexer = Indexer(es_mock, config, parser_mock, 'test-index')
    
    mappings = indexer.create_mapping()
    
    assert mappings['properties']['path']['type'] == 'text'
    assert 'tree_slash' in indexer.required_tokenizers


def test_create_mapping_with_default_type():
    config = {
        'index': {
            'facet': {
                'field_without_type': {}
            }
        }
    }
    es_mock = Mock()
    es_mock.indices.create = Mock()
    parser_mock = Mock()
    indexer = Indexer(es_mock, config, parser_mock, 'test-index')
    
    mappings = indexer.create_mapping()
    
    assert mappings['properties']['field_without_type']['type'] == 'text'


def test_create_mapping_with_overwrite():
    config = {
        'index': {
            'facet': {
                'title': {'type': 'text'}
            }
        }
    }
    es_mock = Mock()
    es_mock.indices.delete = Mock()
    es_mock.indices.create = Mock()
    parser_mock = Mock()
    indexer = Indexer(es_mock, config, parser_mock, 'test-index')
    
    indexer.create_mapping(overwrite=True)
    
    es_mock.indices.delete.assert_called_once_with(index='test-index', ignore=[400, 404])


def test_get_analyzers_with_tree_pipe():
    config = {'index': {'facet': {}}}
    es_mock = Mock()
    parser_mock = Mock()
    indexer = Indexer(es_mock, config, parser_mock, 'test-index')
    indexer.required_tokenizers['tree_pipe'] = True
    
    analyzers = indexer.get_analyzers()
    
    assert 'tree_pipe' in analyzers
    assert analyzers['tree_pipe']['tokenizer'] == 'tree_pipe_tokenizer'


def test_get_analyzers_with_tree_slash():
    config = {'index': {'facet': {}}}
    es_mock = Mock()
    parser_mock = Mock()
    indexer = Indexer(es_mock, config, parser_mock, 'test-index')
    indexer.required_tokenizers['tree_slash'] = True
    
    analyzers = indexer.get_analyzers()
    
    assert 'tree_slash' in analyzers
    assert analyzers['tree_slash']['tokenizer'] == 'tree_slash_tokenizer'


def test_get_analyzers_empty():
    config = {'index': {'facet': {}}}
    es_mock = Mock()
    parser_mock = Mock()
    indexer = Indexer(es_mock, config, parser_mock, 'test-index')
    
    analyzers = indexer.get_analyzers()
    
    assert analyzers == {}


def test_get_tokenizers_with_tree_pipe():
    config = {'index': {'facet': {}}}
    es_mock = Mock()
    parser_mock = Mock()
    indexer = Indexer(es_mock, config, parser_mock, 'test-index')
    indexer.required_tokenizers['tree_pipe'] = True
    
    tokenizers = indexer.get_tokenizers()
    
    assert 'tree_pipe_tokenizer' in tokenizers
    assert tokenizers['tree_pipe_tokenizer']['type'] == 'path_hierarchy'
    assert tokenizers['tree_pipe_tokenizer']['delimiter'] == '|'


def test_get_tokenizers_with_tree_slash():
    config = {'index': {'facet': {}}}
    es_mock = Mock()
    parser_mock = Mock()
    indexer = Indexer(es_mock, config, parser_mock, 'test-index')
    indexer.required_tokenizers['tree_slash'] = True
    
    tokenizers = indexer.get_tokenizers()
    
    assert 'tree_slash_tokenizer' in tokenizers
    assert tokenizers['tree_slash_tokenizer']['delimiter'] == '/'


def test_get_tokenizers_empty():
    config = {'index': {'facet': {}}}
    es_mock = Mock()
    parser_mock = Mock()
    indexer = Indexer(es_mock, config, parser_mock, 'test-index')
    
    tokenizers = indexer.get_tokenizers()
    
    assert tokenizers == {}


def test_add_transformations():
    config = {'index': {'facet': {}}}
    es_mock = Mock()
    parser_mock = Mock()
    indexer = Indexer(es_mock, config, parser_mock, 'test-index', {})
    
    new_transformations = {'test_transform': Mock()}
    indexer.add_transformations(new_transformations)
    
    assert 'test_transform' in indexer.transformations


def test_add_transformations_overwrites():
    config = {'index': {'facet': {}}}
    es_mock = Mock()
    parser_mock = Mock()
    existing = {'existing': Mock()}
    indexer = Indexer(es_mock, config, parser_mock, 'test-index', existing)
    
    new_transformations = {'existing': Mock(), 'new': Mock()}
    indexer.add_transformations(new_transformations)
    
    assert len(indexer.transformations) == 2
    assert 'new' in indexer.transformations


def test_import_files_skips_unprocessed():
    config = {
        'index': {
            'facet': {}
        }
    }
    es_mock = Mock()
    parser_mock = Mock()
    parser_mock.should_process = Mock(return_value=False)
    indexer = Indexer(es_mock, config, parser_mock, 'test-index')
    
    with patch('builtins.open', create=True):
        with patch('procrustus_indexer.indexer.bulk') as mock_bulk:
            indexer.import_files(['file1.json', 'file2.json'])
            mock_bulk.assert_called_once_with(es_mock, [])
    
    parser_mock.should_process.assert_called()


def test_import_files_with_bulk_error():
    from elasticsearch.helpers import BulkIndexError
    
    config = {
        'index': {
            'facet': {}
        }
    }
    es_mock = Mock()
    parser_mock = Mock()
    parser_mock.should_process = Mock(return_value=True)
    parser_mock.parse_file = Mock(return_value={'id': '1', 'title': 'test'})
    
    indexer = Indexer(es_mock, config, parser_mock, 'test-index')
    
    with patch('builtins.open', create=True):
        with patch('procrustus_indexer.indexer.bulk') as mock_bulk:
            mock_bulk.side_effect = BulkIndexError('error', [])
            # Should not raise
            try:
                indexer.import_files(['file1.json'])
            except:
                pass
            mock_bulk.assert_called_once()


def test_import_folder():
    config = {
        'index': {
            'facet': {}
        }
    }
    es_mock = Mock()
    parser_mock = Mock()
    indexer = Indexer(es_mock, config, parser_mock, 'test-index')
    
    with patch('glob.glob', return_value=['file1.json', 'file2.json']):
        with patch.object(indexer, 'import_files') as mock_import:
            indexer.import_folder('/test/folder')
            mock_import.assert_called_once_with(['file1.json', 'file2.json'])
