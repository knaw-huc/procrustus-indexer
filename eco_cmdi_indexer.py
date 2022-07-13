from indexer import Indexer
import json
from lxml import etree
import glob, os, codecs
import unicodedata

f = open("/Users/robzeeman/surfdrive/Documents/DI/e-codices/indexer/indexed_fields.json")
index_object = json.load(f)

config = {
    "url" : "localhost",
    "port" : "9200",
    "index" : "manuscript"
}
indexer = Indexer(config)

def grab_value(path, root, ns):
    content = root.findall(path, ns)
    if content and content[0].text is not None:
        return unicodedata.normalize("NFKD", content[0].text).strip()
    else:
        return ""

def choose_value(path, mmdc_path, root, ns):
    val = grab_value(path, root, ns)
    mmdc_val = grab_value(mmdc_path, root, ns)
    if val != "":
        return val
    else:
        return mmdc_val

def make_json(cmdi):
    retDict = {}
    #file = etree.parse("/Users/robzeeman/Documents/DI_code/DATA/ecodices/new_index_cmd/1055.xml")
    file = etree.parse(cmdi)
    root = file.getroot()
    ns = {"cmd": "http://www.clarin.eu/cmd/"}
    for field in index_object:
        if len(field["fields"]) == 1:
            retDict[field["name"]] = grab_value(field["fields"][0]["path"], root, ns)
        else:
            retDict[field["name"]] = choose_value(field["fields"][0]["path"], field["fields"][1]["path"], root, ns)

    # Add collection, which consists of settlement plus repository
    retDict["collection"] = retDict["settlement"] + ', ' + retDict["repository"]
    retDict["xml"] = cmdi
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






