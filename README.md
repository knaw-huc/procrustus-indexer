# Procrustus Indexer
Python library for indexing HuC datasets.

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


## TOML Configuration
In order to use this indexer, you need to have a TOML file describing the
required ES index fields, and where in the source files to find these.
Here we will see how to configure this TOML file, and what options there are.

Let's first consider this example configueration:

```toml
[index]
full_text="yes"
record="yes"

[index.input]
format="json"

[index.id]
path="jmes:id"
type="keyword"

[index.facet.title]
path="jmes:title"
type="text"

[index.facet.tag]
path="jmes:tag"
type="text"

[index.facet.value]
path="jmes:value"
type="number"

[index.facet.nested_value]
path="jmes:nested.object.value"
type="number"
```

We will now go over each field and see what it means.

#### `[index]`
This part is not currently processed yet. It contains information about
full-text search processing.

#### `[index.input]`
Here, thie input format is set (along with format-specific configuration in the future). For now,
only `json` is supported.

#### `[index.id]`
Here, the ID used for documents in the Elasticsearch index is specified. The `path` determines
where in the input files to look for the ID. Paths follow the following format: `{type}:{path}`.
Available types depend on the input format. For now, the only input format is JSON and the only
supported path type is `jmes`, which will use [JMESPath](https://jmespath.org).

#### `[index.facet.{name}]`
Additional fields, other than the ID, can be specified in here. You can add any number you like.
These consist of three components: the `{name}`, which specifies the name of the property in the
ES index, the `path`, which works like ID and determines where in the input file to find the value,
and the `type`. This determines how the property will be indexed in ES. Supported types are `text`,
`number` and `keyword`. Text is indexed as a full-text field, keyword as a string used for `terms`
queries, e.g.
