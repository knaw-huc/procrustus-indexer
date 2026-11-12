# Procrustus Indexer
Python library for indexing HuC datasets.

This is a library for indexing JSON and XML files based on a TOML configuration.

Modules will be added for:
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

For example, for a JSON index assume the source files and configuration are arranged as follows:

```
json-files/
    a.json
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

### Index files
Import files matching the configured input format from a folder, or specify individual file paths:

```python
indexer.import_folder("json-files")
```

```python
indexer.import_files(["json-files/a.json", "json-files/b.json"])
```

The input format is selected in the TOML configuration. Use an XML configuration and XML files
when building an XML indexer.

JSON files may contain either a single record or a top-level array of records. Each array element
is indexed separately, and configured ID and facet paths are evaluated relative to that element.
An empty array produces no documents. `JsonParser.parse_file` returns a dictionary for a single
record and a list of dictionaries for an array.

## TOML Configuration
In order to use this indexer, you need to have a TOML file describing the
required ES index fields, and where in the source files to find these.
Here we will see how to configure this TOML file, and what options there are.

Let's first consider this example configuration:

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

[index.facet.the_answer]
path="jmes:value"
type="number"

[index.facet.nested_value]
path="jmes:nested.object.value"
type="number"
```

This will turn the following input file:

```json
{
  "id": 0,
  "title": "This is a test",
  "tag": "label",
  "value": 42,
  "nested": {
    "object": {
      "value": 7
    }
  }
}
```

Into this output file (for inserting in ES):
```json
{
  "id": 0,
  "title": "This is a test",
  "tag": "label",
  "the_answer": 42,
  "nested_value": 7
}
```

We will now go over each field and see what it means.

#### `[index]`
This part is not currently processed yet. It contains information about
full-text search processing.

#### `[index.input]`
Set `format` to `json` or `xml`. The input section may also contain format-specific options, such
as XML namespace declarations and an XPath filter.

#### `[index.id]`
Here, the ID used for documents in the Elasticsearch index is specified. The `path` determines
where in the input files to look for the ID. For JSON, paths use `jmes:` with a
[JMESPath](https://jmespath.org) expression or `jsonpath:` with a JSONPath expression. For XML,
the path is an [XPath](https://www.w3.org/TR/xpath-31/) expression.

#### `[index.facet.{name}]`
Additional fields, other than the ID, can be specified in here. You can add any number you like.
These consist of three components: the `{name}`, which specifies the name of the property in the
ES index, the `path`, which works like ID and determines where in the input file to find the value,
and the `type`. This determines how the property will be indexed in ES. Supported types are `text`,
`number` and `keyword`. Text is indexed as a full-text field, keyword as a string used for `terms`
queries.

### XML example

XML paths are XPath expressions. Declare any namespace prefixes used in those expressions in
`[index.input.ns]`. An optional `when` XPath expression filters files: only files whose expression
evaluates to true are indexed. Each XML facet must set `cardinality` to `single` or `list`.
List-valued facets return all non-empty matching values. Set `parse_json = true` on a list facet
when each selected XML text node contains JSON that should be decoded.

```toml
[index]
full_text = "yes"
record = "yes"

[index.input]
format = "xml"
when = "/ex:record[@status='published']"

[index.input.ns]
ex = "urn:example"

[index.id]
path = "/ex:record/ex:id"
type = "keyword"

[index.facet.title]
path = "/ex:record/ex:title"
type = "text"
cardinality = "single"

[index.facet.tags]
path = "/ex:record/ex:tags/ex:tag"
type = "keyword"
cardinality = "list"

[index.facet.metadata]
path = "/ex:record/ex:metadata"
type = "text"
cardinality = "list"
parse_json = true
```

For example, the XML source can use the declared namespace:

```xml
<ex:record xmlns:ex="urn:example" status="published">
  <ex:id>record-1</ex:id>
  <ex:title>An XML record</ex:title>
  <ex:tags>
    <ex:tag>first</ex:tag>
    <ex:tag>second</ex:tag>
  </ex:tags>
  <ex:metadata>{"source": "archive"}</ex:metadata>
</ex:record>
```

This produces a document with `id` set to `"record-1"`, `title` set to `"An XML record"`,
`tags` set to `["first", "second"]`, and `metadata` set to `[{"source": "archive"}]`.
Build and use the XML indexer in the same way, with the XML configuration and source files:

```python
xml_indexer = build_indexer("xml-config.toml", "xml-index", Elasticsearch())
xml_indexer.create_mapping(overwrite=True)
xml_indexer.import_folder("xml-files")
```

### Transformations

Facet transformations run after values have been extracted. The built-in `csv_mapping`
transformation reads a semicolon-delimited file with two columns: source value and replacement
value. Values absent from the mapping are kept unchanged. For example, add this configuration to a
facet:

```toml
[[index.facet.tags.transformations]]
type = "csv_mapping"
csv_file = "mappings/tags.csv"
```

With `mappings/tags.csv` containing:

```csv
old;new
legacy;current
```

the facet value `"old"` becomes `"new"` (and `"legacy"` becomes `"current"`). Transformations
apply to both scalar and list facet values. The `csv_file` path is resolved from the process's
current working directory. Applications can register additional transformation classes with
`indexer.add_transformations({"name": TransformationClass})`; each class must implement the
transformation interface and accept the configured options as keyword arguments.

## Development and CI

Install the locked dependencies with Poetry and run the unit tests:

```sh
poetry install --no-interaction
poetry run python -m pytest -q
```

Run the same Pylint error checks used in CI:

```sh
poetry run python -m pylint --errors-only --extension-pkg-allow-list=saxonche procrustus_indexer
```

The extension allow-list lets Pylint inspect Saxon's native Python extension. CI currently checks
errors only; existing style and warning diagnostics are not enforced.

GitHub Actions runs independent unit-test and Pylint jobs on Python 3.13 for pushes and pull
requests. The workflow can also be started manually from the Actions tab.
