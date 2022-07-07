from indexer import Indexer
import json
from lxml import etree
import glob, os, codecs
import unicodedata

index_object = [
    {"name": "title", "path": "./cmd:Components/cmd:eCodices/cmd:Title/cmd:title", "unbounded": "no"},
    {"name": "mmdc_title", "path": "./cmd:Components/cmd:eCodices/cmd:Title/cmd:mmdc_title", "unbounded": "no"},
    {"name": "settlement",
     "path": "./cmd:Components/cmd:eCodices/cmd:Source/cmd:Identifier/cmd:Settlement/cmd:settlement",
     "unbounded": "no"},
    {"name": "repository",
     "path": "./cmd:Components/cmd:eCodices/cmd:Source/cmd:Identifier/cmd:Repository/cmd:repository",
     "unbounded": "no"},
    {"name": "origDate",
     "path": "./cmd:CMD/cmd:Components/cmd:eCodices/cmd:Source/cmd:Head/cmd:OrigDate/cmd:mmdc_origDate",
     "unbounded": "no"},
    {"name": "language",
     "path": "./cmd:CMD/cmd:Components/cmd:eCodices/cmd:Source/cmd:Contents/cmd:textLang/cmd:textLang",
     "unbounded": "no"},
    {"name": "type",
     "path": "./cmd:CMD/cmd:Components/cmd:eCodices/cmd:Source/cmd:PhysDesc/cmd:ObjectDesc/cmd:form",
     "unbounded": "no"},
    {"name": "binding",
     "path": "./cmd:CMD/cmd:Components/cmd:eCodices/cmd:Source/cmd:PhysDesc/cmd:bindingDesc/cmd:Binding/cmd:binding",
     "unbounded": "no"},
    {"name": "material",
     "path": "./cmd:CMD/cmd:Components/cmd:eCodices/cmd:Source/cmd:PhysDesc/cmd:ObjectDesc/cmd:SupportDesc/cmd:Material/cmd:material",
     "unbounded": "no"},
    {"name": "mmdc_material",
     "path": "./cmd:CMD/cmd:Components/cmd:eCodices/cmd:Source/cmd:PhysDesc/cmd:ObjectDesc/cmd:SupportDesc/cmd:Material/cmd:mmdc_material",
     "unbounded": "no"},
    {"name": "musicnotation",
     "path": "./cmd:CMD/cmd:Components/cmd:eCodices/cmd:Source/cmd:PhysDesc/cmd:musicNotation",
     "unbounded": "no"},
    {"name": "decoration",
     "path": "./cmd:CMD/cmd:Components/cmd:eCodices/cmd:Source/cmd:PhysDesc/cmd:DecoDesc/cmd:DecoNote/cmd:decoNote",
     "unbounded": "no"},
    {"name": "mmdc_decoration",
     "path": "./cmd:CMD/cmd:Components/cmd:eCodices/cmd:Source/cmd:PhysDesc/cmd:DecoDesc/cmd:DecoNote/cmd:mmdc_decoNote",
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
                retDict[field["name"]] = unicodedata.normalize("NFKD", content[0].text).strip()
            else:
                retDict[field["name"]] = ""

    # Add collection, which consists of settlement plus repository
    retDict["collection"] = retDict["settlement"] + ', ' + retDict["repository"]
    indexer.add_to_index(retDict)
    print(retDict)

def processDir(dir):
    os.chdir(dir)
    for file in glob.glob("*.xml"):
        make_json(file)

#make json
processDir("/Users/robzeeman/Documents/DI_code/DATA/ecodices/records/cmd/1")
processDir("/Users/robzeeman/Documents/DI_code/DATA/ecodices/records/cmd/2")
processDir("/Users/robzeeman/Documents/DI_code/DATA/ecodices/records/cmd/3")
processDir("/Users/robzeeman/Documents/DI_code/DATA/ecodices/records/cmd/4")
processDir("/Users/robzeeman/Documents/DI_code/DATA/ecodices/records/cmd/5")
processDir("/Users/robzeeman/Documents/DI_code/DATA/ecodices/records/cmd/6")
processDir("/Users/robzeeman/Documents/DI_code/DATA/ecodices/records/cmd/7")
processDir("/Users/robzeeman/Documents/DI_code/DATA/ecodices/records/cmd/8")
processDir("/Users/robzeeman/Documents/DI_code/DATA/ecodices/records/cmd/9")






