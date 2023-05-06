import json
from indexer import Indexer
from index_reader import IndexReader



targetConfig = {
    "url" : "localhost",
    "port" : "9200",
    "index" : "switch"
}


targetIndex = Indexer(targetConfig)

def create_switch(index):
    sourceConfig = {
        "url" : "localhost",
        "port" : "9200",
        "index" : index
    }
    print(index)
    sourceIndex = IndexReader(sourceConfig)
    start = 0
    amount = 5000
    result = sourceIndex.get_batch(start, amount)
    hits = result["hits"]["hits"]
    if hits:
       write_to_index(hits, index)
    while hits:
        start = start + amount
        print(start)
        result = sourceIndex.get_batch(start, amount)
        hits = result["hits"]["hits"]
        if hits:
            write_to_index(hits, index)

def write_to_index(hits, index):
    data = ""
    header = json.dumps({"index": {}})
    #print(hits)
    for hit in hits:
        buffer = hit["_source"]
        buffer["index"] = index
        data = data + header + "\n" + json.dumps(buffer) + "\n"
    #print(data)
    targetIndex.bulk_to_index(data)

def run():
    indexes = ["notarissennetwerk_pnv_personname"]
    for index in indexes:
        create_switch(index)

run()
