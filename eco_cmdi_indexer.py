# -*- coding: utf-8 -*-
from indexer import Indexer
import json
from lxml import etree
import glob, os, codecs
import unicodedata





#old_h = '<cmd:CMD xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:lat="http://lat.mpi.nl/" xmlns:cmd="http://www.clarin.eu/cmd/1" xsi:schemaLocation="http://www.clarin.eu/cmd/ http://catalog.clarin.eu/ds/ComponentRegistry/rest/registry/profiles/clarin.eu:cr1:p_1440426460262/xsd" CMDVersion="1.1">'
#old_th = '<cmd:CMD xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:lat="http://lat.mpi.nl/" xmlns:cmd="http://www.clarin.eu/cmd/1" xsi:schemaLocation="http://www.clarin.eu/cmd/ http://catalog.clarin.eu/ds/ComponentRegistry/rest/registry/profiles/clarin.eu:cr1:p_1440426460262/xsd" CMDVersion="1.2">'
#new_h = '<cmd:CMD xmlns:cmd="http://www.clarin.eu/cmd/" xmlns:ec="https:huygens.knaw.nl/ecodicesnl" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.clarin.eu/cmd/ https://catalog.clarin.eu/ds/ComponentRegistry/rest/registry/1.1/profiles/clarin.eu:cr1:p_1633000337993/xsd" CMDVersion="1.1">'

f = open("/Users/robzeeman/surfdrive/Documents/DI/e-codices/indexer/indexed_fields.json")
index_object = json.load(f)

#s = open("./data/misc/eco_shelfmarks.json")
#shelfmarks = json.load(s)
#print(shelfmarks)



config = {
    "url" : "localhost",
    "port" : "9200",
    "index" : "manuscript"
}
indexer = Indexer(config)

def normalize_language(lang):
    languages = {"ara": "Arabic",
                 "eng": "English",
                 "fra": "French",
                 "fry": "Western Frisian",
                 "gla": "Gaelic",
                 "deu": "German",
                 "grc": "Ancient Greek",
                 "heb": "Hebrew",
                 "isl": "Icelandic",
                 "gle": "Irish",
                 "ita": "Italian",
                 "lat": "Latin",
                 "yid": "Yiddish",
                 "gml": "Middle Low German",
                 "mis": "Uncoded languages",
                 "nld": "Dutch",
                 "oci": "Occitan (post 1500)",
                 "pro": "Old Occitan (to 1500) Old Provençal (to 1500)",
                 "spa": "Spanish",
                 "und": "Undetermined"}
    if lang in languages:
        return languages[lang]
    else:
        return lang

def normalize_date(dateStr):
    centuries = {"eighth": "8th century", "ninth": "9th century", "tenth": "10th century", "eleventh": "11th century", "twelfth": "12th century", "thirteenth": "13th century", "fourteenth": "14th century", "fifteenth": "15th century", "sixteenth": "16th century", "seventeenth": "17th century"}
    # grab centuries
    for century in centuries.values():
        if century in dateStr:
            return century

    for century in centuries.keys():
        if century in dateStr:
            return centuries[century]

    # grab years

    year = dateStr[0:2]
    if year.isnumeric():
        if year[0] == '1':
            cent = int(year)
        else:
            cent = int(year[0])
        cent = cent + 1
        return str(cent) + "th century"

    # OK, you asked for it.
    year = ""
    for char in dateStr:
        if char.isdigit():
            year = year + char
            if len(year) == 2:
                cent = int(year)
                cent = cent + 1
                return str(cent) + "th century"

    return dateStr


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
    file = etree.parse(cmdi)
    root = file.getroot()
    ns = {"cmd": "http://www.clarin.eu/cmd/1"}
    for field in index_object:
        if len(field["fields"]) == 1:
            retDict[field["name"]] = grab_value(field["fields"][0]["path"], root, ns)
        else:
            retDict[field["name"]] = choose_value(field["fields"][0]["path"], field["fields"][1]["path"], root, ns)


    # Add collection, which consists of settlement plus repository
    if retDict["settlement"] == "The Hague":
        retDict["settlement"] = "Den Haag"
    if retDict["settlement"] == 'Deventer':
        retDict["collection"] = "Deventer, Athenaeumbibliotheek"
    else:
        retDict["collection"] = retDict["settlement"] + ', ' + retDict["repository"]
    retDict["xml"] = cmdi
    retDict["language"] = normalize_language(retDict["language"])
    retDict["tempDate"] = retDict["origDate"]
    retDict["origDate"] = normalize_date(retDict["origDate"])
    if retDict["musicnotation"].strip() == "":
        retDict["has_musicnotation"] = "no"
    else:
        retDict["has_musicnotation"] = "yes"
    if retDict["decoration"].strip() == "":
        retDict["has_decoration"] = "no"
    else:
        retDict["has_decoration"] = "yes"

    indexer.add_to_index(retDict)

def processDir(dir):
    os.chdir(dir)
    for file in glob.glob("*.xml"):
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
processDir("/Users/robzeeman/Desktop/Werkmap/ecodices/xml_selectie")






