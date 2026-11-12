import tomllib
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from procrustus_indexer.indexer import Indexer
from procrustus_indexer.parsers import JsonParser
from procrustus_indexer.transformations.default import DEFAULT_TRANSFORMATIONS


JSON_FILES = Path("tests/test_files/json_files")


def get_config(toml_file: str) -> dict:
    config_path = f"tests/test_files/{toml_file}"

    config: dict
    with open(config_path, "rb") as f:
        config = tomllib.load(f)
    return config


def test_create_indexer():
    config = get_config('test-json.toml')
    parser = JsonParser(config)
    assert type(parser) == JsonParser

def test_parse():
    config = get_config('test-json.toml')
    parser = JsonParser(config)

    with open(JSON_FILES / "a.json", "rb") as f:
        result = parser.parse_file(f)
    print(result)

    assert result['id'] == 0
    assert result['title'] == 'This is a test'
    assert result['tag'] == 'label'
    assert result['value'] == 42
    assert result['nested_value'] == 7

def test_parse_jsonpath():
    config = get_config('test-jsonpath.toml')
    parser = JsonParser(config)

    with open(JSON_FILES / "a.json", "rb") as f:
        result = parser.parse_file(f)
    print(result)

    assert result['id'] == 0
    assert result['title'] == 'This is a test'
    assert result['tag'] == 'label'
    assert result['value'] == 42
    assert result['nested_value'] == 7


def test_parse_partial():
    config = get_config('test-json.toml')
    parser = JsonParser(config)

    with open(JSON_FILES / "b.json", "rb") as f:
        result = parser.parse_file(f)

    assert "unused_property" not in result


def test_parse_incomplete():
    config = get_config('test-json.toml')
    parser = JsonParser(config)

    with open(JSON_FILES / "incomplete.json", "rb") as f:
        result = parser.parse_file(f)

    assert result['tag'] == None

def test_parse_incomplete_jsonpath():
    config = get_config('test-jsonpath.toml')
    parser = JsonParser(config)

    with open(JSON_FILES / "incomplete.json", "rb") as f:
        result = parser.parse_file(f)

    assert result['tag'] == None


@pytest.mark.parametrize("config_file", ["test-json.toml", "test-jsonpath.toml"])
def test_parse_batch(config_file):
    parser = JsonParser(get_config(config_file))
    with (JSON_FILES / "batch.json").open(encoding="utf-8") as f:
        result = parser.parse_file(f)

    assert result == [
        {"id": 1, "title": "First", "tag": "label", "value": 42, "nested_value": 7},
        {"id": 2, "title": "Second", "tag": None, "value": 0, "nested_value": None},
    ]


@pytest.mark.parametrize(
    ("filename", "expected"),
    [
        ("empty_batch.json", []),
        ("single_batch.json", [
            {"id": 1, "title": "Only", "tag": None, "value": None, "nested_value": None}
        ]),
    ],
)
def test_parse_batch_preserves_array_shape(filename, expected):
    parser = JsonParser(get_config("test-json.toml"))

    with (JSON_FILES / filename).open(encoding="utf-8") as f:
        result = parser.parse_file(f)

    assert result == expected


def test_import_mixed_json_files_transforms_and_indexes_each_record(tmp_path):
    config = get_config("test-json.toml")
    mapping_file = tmp_path / "mapping.csv"
    mapping_file.write_text("label;new\n", encoding="utf-8")
    config["index"]["facet"]["tag"]["transformations"] = [
        {"type": "csv_mapping", "csv_file": str(mapping_file)}
    ]
    es = Mock()
    indexer = Indexer(es, config, JsonParser(config), "test-index", DEFAULT_TRANSFORMATIONS)

    with patch("procrustus_indexer.indexer.bulk") as mock_bulk:
        indexer.import_files([
            str(JSON_FILES / "a.json"),
            str(JSON_FILES / "batch.json"),
            str(JSON_FILES / "empty_batch.json"),
        ])

    actions = mock_bulk.call_args.args[1]
    assert mock_bulk.call_args.args[0] is es
    assert [action["_index"] for action in actions] == ["test-index"] * 3
    assert [action["_id"] for action in actions] == [0, 1, 2]
    assert [action["_source"]["tag"] for action in actions] == ["new", "new", None]
