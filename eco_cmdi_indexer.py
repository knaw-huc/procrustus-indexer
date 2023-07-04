# -*- coding: utf-8 -*-
from indexer import Indexer
import json
from lxml import etree
import glob, os, codecs
import unicodedata





old_h = 'xmlns:cmd="http://www.clarin.eu/cmd/1"'
#old_th = '<cmd:CMD xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:lat="http://lat.mpi.nl/" xmlns:cmd="http://www.clarin.eu/cmd/1" xsi:schemaLocation="http://www.clarin.eu/cmd/ http://catalog.clarin.eu/ds/ComponentRegistry/rest/registry/profiles/clarin.eu:cr1:p_1440426460262/xsd" CMDVersion="1.2">'
new_h = 'xmlns:cmd="http://www.clarin.eu/cmd/"'

f = open("/Users/robzeeman/surfdrive/Documents/DI/ecodices/indexer/indexed_fields.json")
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
                 "fry": "Frisian",
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

def normalize_binding(binding):
    txt = binding.lower()
    if "post-medieval" in txt or "17th" in txt or "18th" in txt or "19th" in txt:
        return "Post-medieval"
    else:
        return "Medieval"


def normalize_date(dateStr):
    centuries = {"eighth": "8th century", "ninth": "9th century", "tenth": "10th century", "eleventh": "11th century", "twelfth": "12th century", "thirteenth": "13th century", "fourteenth": "14th century", "fifteenth": "15th century", "sixteenth": "16th century", "seventeenth": "17th century", "c. 900": "9th century"}
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
            if year[0] == '0':
                cent = int(year[1])
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


def grab_value(path, unbounded, root, ns):
    content = root.findall(path, ns)
    if content and content[0].text is not None:
        if unbounded == "yes":
            retList = []
            for item in content:
                retList.append({"item": unicodedata.normalize("NFKD", item.text).strip()})
            return retList
        else:
            return unicodedata.normalize("NFKD", content[0].text).strip()
    else:
        if unbounded == "yes":
            return []
        else:
            return ""

def choose_value(path, mmdc_path, root, ns):
    val = grab_value(path, "no", root, ns)
    mmdc_val = grab_value(mmdc_path, "no", root, ns)
    if val != "":
        return val
    else:
        return mmdc_val

def make_json(cmdi):
    retDict = {}
    command = "sed -i.bu 's@" + old_h + "@" + new_h + "@' " + cmdi
    #command = "sed -i.bu 's@" + old_h + "@" + new_h + "@' " + file_name
    os.system(command)
    file = etree.parse(cmdi)
    root = file.getroot()
    ns = {"cmd": "http://www.clarin.eu/cmd/"}
    for field in index_object:
        if len(field["fields"]) == 1:
            retDict[field["name"]] = grab_value(field["fields"][0]["path"], field["fields"][0]["unbounded"], root, ns)
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
    retDict["binding"] = normalize_binding(retDict["binding"])
    if retDict["musicnotation"].strip() == "":
        retDict["has_musicnotation"] = "no"
        retDict["musicnotation"] = "N.a."
    else:
        retDict["has_musicnotation"] = "yes"
    if retDict["decoration"].strip() == "":
        retDict["has_decoration"] = "no"
    else:
        retDict["has_decoration"] = "yes"
    retDict["decoration"] = normalize_decoration(retDict["decoration"])
    retDict["place"] = normalize_place(retDict["place"])
    record_id = retDict["xml"].replace('.xml', '')
    retDict["permalink"] = "https://db.ecodices.nl/detail/" + record_id + "/overview"
    indexer.add_to_index_with_id(retDict, record_id)

def normalize_decoration(deco):
    if (deco.lower().find("historiated") != -1):
        return "Historiated initials"
    if (deco.lower().find("pen-flourished") != -1):
        return "Pen-flourished initials"
    if (deco.lower().find("pen flourished") != -1):
        return "Pen-flourished initials"
    if (deco.lower().find("miniature") != -1):
        return "Miniatures"
    if (deco.lower().find("decorated") != -1):
        return "Decorated initials"
    if (deco.lower().find("litterae") != -1):
        return "Decorated initials"
    if (deco.lower().find("red and blue initials") != -1):
        return "Decorated initials"
    if (deco.lower().find("drawing") != -1):
        return "Drawings"
    if (deco.lower().find("canon") != -1):
        return "Decorated canon tables"
    if (deco.lower().find("coats of arms") != -1):
        return "Coats of arms"
    if (deco.lower().find("border") != -1):
        return "Border decoration"
    return deco

def normalize_place(place):
    if (place.lower() == 'sine loco'):
        return 'sine loco'
    if (place.lower() == 'netherlands' or place.lower() == 'netherlands?'):
        return 'Netherlands'
    if (place.lower().find('frisia') != -1):
        return 'Frisia'
    if (place.lower().find('ijssel') != -1):
        return 'IJssel region'
    if (place.lower().find('utrecht') != -1):
        return 'Utrecht'
    if (place.lower().find('france') != -1):
        return 'France'
    if (place.lower().find('holland') != -1):
        return 'Holland'
    return place



def processDir(dir):
    os.chdir(dir)
    for file in glob.glob("*.xml"):
        make_json(file)
    print("Manuscripts indexed.")


# processDir("/Users/robzeeman/Documents/DI_code/DATA/ecodices/records/cmd/0")
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






