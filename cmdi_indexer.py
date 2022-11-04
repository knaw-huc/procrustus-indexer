# -*- coding: utf-8 -*-
import sys
from indexer import Indexer
import json
from lxml import etree
import glob, os, codecs
import unicodedata

index = "cmdi"
fields = "./fields.json"
records = [ "./records" ]

# Count the arguments
argc = len(sys.argv) - 1

if (argc >= 1):
    index = sys.argv[1]

if (argc >= 2):
    fields = sys.argv[2]

if (argc > 2):
    pos = 3
    records = []
    while (argc >= pos):
        records.append(sys.argv[pos])
        pos = pos + 1

print ("index[" + index + "]")
print ("fields[" + fields + "]")
print ("records[" + str(records) + "]")

f = open(fields)
index_object = json.load(f)




config = {
    "url" : "localhost",
    "port" : "9200",
    "index" : index
}
indexer = Indexer(config)



def grab_value(path, root, ns):
    content = root.findall(path, ns)
    if content and content[0].text is not None:
        return unicodedata.normalize("NFKD", content[0].text).strip()
    else:
        return ""

def grab_list(name, path, root, ns):
    ret_arr = []
    content = root.findall(path, ns)
    for item in content:
        buffer = {name : unicodedata.normalize("NFKD", item.text).strip()}
        if buffer not in ret_arr:
            ret_arr.append(buffer)
    return ret_arr

def make_json(cmdi):
    retDict = {}
    retDict["record"] = cmdi
    file = etree.parse(cmdi)
    root = file.getroot()
    ns = {"cmd": "http://www.clarin.eu/cmd/","xml": "http://www.w3.org/XML/1998/namespace"}
    for field in index_object:
        if field["fields"][0]["unbounded"] == "no":
            retDict[field["name"]] = grab_value(field["fields"][0]["path"], root, ns)
        else:
            retDict[field["name"]] = grab_list(field["name"] ,field["fields"][0]["path"], root, ns)
    print(retDict)
    indexer.add_to_index(retDict)

def processDir(dir):
    os.chdir(dir)
    for file in glob.glob("*.cmdi"):
        make_json(file)

for dir in records:
    processDir(dir)






