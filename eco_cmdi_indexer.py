from indexer import Indexer
import json
from lxml import etree
import glob, os, codecs

index_object = [
    {"name": "title", "path": "./cmd:Components/cmd:eCodices/cmd:Title/cmd:title", "unbounded": "no"},
    {"name": "mmdc_title", "path": "./cmd:Components/cmd:eCodices/cmd:Title/cmd:mmdc_title", "unbounded": "no"},
    {"name": "settlement",
     "path": "./cmd:Components/cmd:eCodices/cmd:Source/cmd:Identifier/cmd:Settlement/cmd:settlement",
     "unbounded": "no"},
    {"name": "repository",
     "path": "./cmd:Components/cmd:eCodices/cmd:Source/cmd:Identifier/cmd:Repository/cmd:repository",
     "unbounded": "no"}
]



config = {
    "url" : "localhost",
    "port" : "9200",
    "index" : "manuscript"
}
indexer = Indexer(config)

def make_json(cmdi):
    retDict = {}
    #file = etree.parse("/Users/robzeeman/Documents/DI_code/DATA/ecodices/new_index_cmd/1055.xml")
    file = etree.parse(cmdi)
    root = file.getroot()
    ns = {"cmd": "http://www.clarin.eu/cmd/"}
    for field in index_object:
        content = root.findall(field["path"], ns)
        if field["unbounded"] == "no":
            if content and content[0].text is not None:
                retDict[field["name"]] = content[0].text
            else:
                retDict[field["name"]] = ""

    indexer.add_to_index(retDict)
    print(retDict)

def processDir(dir):
    os.chdir(dir)
    for file in glob.glob("*.xml"):
        make_json(file)

#make_json()
processDir("/Users/robzeeman/Documents/DI_code/DATA/ecodices/records/cmd_out/1")






