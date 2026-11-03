from io import StringIO
import tomllib

from procrustus_indexer.parsers import XmlParser


def get_config(toml_file: str) -> dict:
    config_path = f"tests/test_files/{toml_file}"
    with open(config_path, "rb") as f:
        return tomllib.load(f)


def test_create_xml_parser():
    parser = XmlParser(get_config("test-xml.toml"))

    assert type(parser) is XmlParser
    assert parser.supported_types() == ["xml"]


def test_parse_xml():
    parser = XmlParser(get_config("test-xml.toml"))

    with open("tests/test_files/xml_files/a.xml", encoding="utf-8") as f:
        result = parser.parse_file(f)

    assert result == {
        "id": "record-1",
        "title": "XML indexing test",
        "tags": ["first", "second"],
        "metadata": [{"key": "value"}],
        "missing": None,
    }


def test_should_process_xml_and_rewind_file():
    parser = XmlParser(get_config("test-xml.toml"))

    with open("tests/test_files/xml_files/a.xml", encoding="utf-8") as f:
        assert parser.should_process(f) is True
        assert f.tell() == 0

    unpublished_xml = (
        '<ex:record xmlns:ex="urn:example" status="draft">'
        "<ex:id>record-2</ex:id></ex:record>"
    )
    with StringIO(unpublished_xml) as f:
        assert parser.should_process(f) is False
        assert f.tell() == 0
