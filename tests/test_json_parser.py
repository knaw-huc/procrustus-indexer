import tomllib

import pytest

from procrustus_indexer.parsers import JsonParser


def get_config() -> dict:
    config_path = "tests/test_files/test-json.toml"

    config: dict
    with open(config_path, "rb") as f:
        config = tomllib.load(f)
    return config


def test_create_indexer():
    config = get_config()
    parser = JsonParser(config)
    assert type(parser) == JsonParser

def test_parse():
    config = get_config()
    parser = JsonParser(config)

    with open("tests/test_files/json_files/a.json", "rb") as f:
        result = parser.parse_file(f)
    print(result)

    assert result['id'] == 0
    assert result['title'] == 'This is a test'
    assert result['tag'] == 'label'
    assert result['value'] == 42
    assert result['nested_value'] == 7


def test_parse_partial():
    config = get_config()
    parser = JsonParser(config)

    with open("tests/test_files/json_files/b.json", "rb") as f:
        result = parser.parse_file(f)

    assert "unused_property" not in result


def test_parse_incomplete():
    config = get_config()
    parser = JsonParser(config)

    with open("tests/test_files/json_files/incomplete.json", "rb") as f:
        result = parser.parse_file(f)

    assert result['tag'] == None