from procrustus_indexer.indexer import Indexer
from procrustus_indexer.transformations.csv_mapping import CsvMapping
from procrustus_indexer.transformations.default import DEFAULT_TRANSFORMATIONS


def test_csv_mapping_transforms_known_values_and_preserves_unknown_values(tmp_path):
    mapping_file = tmp_path / "mapping.csv"
    mapping_file.write_text("old;new\nlegacy;current\n", encoding="utf-8")

    transformation = CsvMapping(str(mapping_file))

    assert transformation.transform("old") == "new"
    assert transformation.transform("legacy") == "current"
    assert transformation.transform("unmapped") == "unmapped"


def test_default_transformations_register_csv_mapping():
    assert DEFAULT_TRANSFORMATIONS == {"csv_mapping": CsvMapping}


def test_transform_document_applies_transformations_to_scalars_and_lists(tmp_path):
    mapping_file = tmp_path / "mapping.csv"
    mapping_file.write_text("old;new\nlegacy;current\n", encoding="utf-8")
    config = {
        "index": {
            "facet": {
                "status": {
                    "transformations": [
                        {"type": "csv_mapping", "csv_file": str(mapping_file)}
                    ]
                },
                "tags": {
                    "transformations": [
                        {"type": "csv_mapping", "csv_file": str(mapping_file)}
                    ]
                },
                "untouched": {},
            }
        }
    }
    indexer = Indexer(None, config, None, "test", DEFAULT_TRANSFORMATIONS)
    document = {
        "id": "record-1",
        "status": "old",
        "tags": ["legacy", "unmapped"],
        "untouched": "value",
    }

    result = indexer.transform_document(document)

    assert result == {
        "id": "record-1",
        "status": "new",
        "tags": ["current", "unmapped"],
        "untouched": "value",
    }
