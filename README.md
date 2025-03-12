# Procrustus Indexer
Python app for indexing HuC datasets.

This is a first experimental version, for indexing files based on a TOML configuration.
For now, input files need to be json.

Modules will be added for:
* XML
* Relational databases (MySQL, Postgres)
* Linked data (SPARQL, RDF)
* Tabular data (CSV, MS-Excel)

## Installation

In order to install this library, you can use pip:

```
pip install procrustus-indexer
```

## Usage
In order to use the indexer you need to configure it first. Use the `build_indexer`
function to create an indexer. It requires the configuration TOML file, the name of
the index to use and an Elasticsearch client.

For this example, assume we have the following file structure:

```
json-files/
    a.json
    b.json
config.toml
```

First we create the indexer:

```python
from elasticsearch import Elasticsearch
from procrustus_indexer import build_indexer

indexer = build_indexer('config.toml', 'index-name', Elasticsearch())
```

### Create index and mapping:
We can use the indexer to generate a mapping for the Elasticsearch index.

```python
indexer.create_mapping(overwrite=True)
```
The `overwrite` parameter can be used to re-create the index if it already exists.

### Index json files
Now we can import our json files in Elasticsearch.

```python
indexer.import_folder("json-files")
```

or

```python
indexer.import_files(["json-files/a.json", "json-files/b.json"])
```