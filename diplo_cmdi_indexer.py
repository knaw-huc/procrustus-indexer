# -*- coding: utf-8 -*-
from indexer import Indexer
import json
from lxml import etree
import glob, os, codecs
import unicodedata





old_h = '<cmd:CMD xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:lat="http://lat.mpi.nl/" xmlns:cmd="http://www.clarin.eu/cmd/1" xsi:schemaLocation="http://www.clarin.eu/cmd/ http://catalog.clarin.eu/ds/ComponentRegistry/rest/registry/profiles/clarin.eu:cr1:p_1440426460262/xsd" CMDVersion="1.2">'
new_h = '<cmd:CMD xmlns:cmd="http://www.clarin.eu/cmd/" xmlns:ec="https:huygens.knaw.nl/ecodicesnl" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.clarin.eu/cmd/ https://catalog.clarin.eu/ds/ComponentRegistry/rest/registry/1.1/profiles/clarin.eu:cr1:p_1633000337993/xsd" CMDVersion="1.1">'

f = open("/Users/robzeeman/surfdrive/Documents/DI/diplo/indexed_fields.json")
index_object = json.load(f)




config = {
    "url" : "localhost",
    "port" : "9200",
    "index" : "diplo"
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
    #retDict["rob"] = cmdi
    command = "sed -i.bu 's@" + old_h + "@" + new_h + "@' " + cmdi
    #command = "sed -i.bu 's@" + old_h + "@" + new_h + "@' " + file_name
    os.system(command)
    #os.system('rm ' + cmdi + '.bu')
    #file = etree.parse(file_name)
    file = etree.parse(cmdi)
    root = file.getroot()
    ns = {"cmd": "http://www.clarin.eu/cmd/"}
    for field in index_object:
        if field["fields"][0]["unbounded"] == "no":
            retDict[field["name"]] = grab_value(field["fields"][0]["path"], root, ns)
        else:
            retDict[field["name"]] = grab_list(field["name"] ,field["fields"][0]["path"], root, ns)



    indexer.add_to_index(retDict)

def processDir(dir):
    os.chdir(dir)
    for file in glob.glob("*.cmdi"):
        make_json(file)

# processDir("/Users/robzeeman/Documents/DI_code/DATA/ecodices/records/cmd/1")
# processDir("/Users/robzeeman/Documents/DI_code/DATA/ecodices/records/cmd/2")
# processDir("/Users/robzeeman/Documents/DI_code/DATA/ecodices/records/cmd/3")
# processDir("/Users/robzeeman/Documents/DI_code/DATA/ecodices/records/cmd/4")
# processDir("/Users/robzeeman/Documents/DI_code/DATA/ecodices/records/cmd/5")
# processDir("/Users/robzeeman/Documents/DI_code/DATA/ecodices/records/cmd/6")
# processDir("/Users/robzeeman/Documents/DI_code/DATA/ecodices/records/cmd/7")
# processDir("/Users/robzeeman/Documents/DI_code/DATA/ecodices/records/cmd/8")
# processDir("/Users/robzeeman/Documents/DI_code/DATA/ecodices/records/cmd/9")
processDir("/Users/robzeeman/surfdrive/Documents/DI/diplo/cmdi")






