from indexer import Indexer
from bs4 import BeautifulSoup
import glob, os

# with open('data/TEI/mapping_mmdc.xml', 'r') as f:
#     data = f.read()
#
# Bs_data = BeautifulSoup(data, "xml")
# b_place = Bs_data.find('teiHeader')
#
# print(b_place)


# Functions
def processLanguage(lang):
    retArray = []
    lst = lang.split("/")
    for item in lst:
        if item != "":
            retArray.append({"language": item[1:]})
    if len(retArray) < 1 :
        retArray.append({"language": ""})
    return retArray


def processFile(file, fields, indexer):
    json = {}
    with open(file, 'r') as f:
        data = f.read()

    print(file)
    Bs_data = BeautifulSoup(data, "xml")
    typeMan = Bs_data.select_one('meta[key="0500"]')
    if typeMan.get_text() != 'Bnurp':
        json["xml"] = file;
        for key, value in fields.items():
            item = Bs_data.select_one('meta[key="' + key + '"]')
            if item is not None:
                if key == '1500':
                    json[value] = processLanguage(item.get_text())
                elif key == "4000":
                    text = item.get_text()
                    json[value] = text[1:]
                elif key == 4241:
                    text = item.get_text()
                    json[value] = text[4:]
                elif key == "3250":
                    json[value] = item.get_text()
                    buffer = item.get_text()
                    lst = buffer.split(":")
                    if len(lst) > 1:
                        json["location"] = lst[0].strip()
                        lst.pop(0)
                        json["shelfmark"] = ":".join(lst).strip()
                    else:
                        json["location"] = ""
                        json["shelfmark"] = ""
                else:
                    json[value] = item.get_text()
            else:
                json[value] = ""
        if json["textLang"] == '':
            json["textLang"] = [{"language": "onb"}]
        indexer.add_to_index(json)

def processDir(fields, indexer, dir):
    os.chdir(dir)
    for file in glob.glob("*.xml"):
        processFile(file, fields, indexer)

# Start main program
fields = {
    "0500" : "manuscriptType",
    "3250" : "title",
    "4030" : "origPlace",
    "1100" : "origDate",
    "4205" : "summary",
    "1500" : "textLang",
    "7000" : "idno",
    "7100" : "altIdentifier",
    "3210" : "msName",
    "4000" : "itemTitle",
    "3000" : "itemAuthor",
    "4243" : "filiation",
    "4241" : "main_doc",
    "4063" : "layout",
    "4062" : "measure"

}

config = {
    "url" : "localhost",
    "port" : "9200",
    "index" : "manuscripts"
}

indexer = Indexer(config)

processDir(fields, indexer, "/Users/robzeeman/surfdrive/Documents/DI/e-codices/mmdc_xml/1")
processDir(fields, indexer, "/Users/robzeeman/surfdrive/Documents/DI/e-codices/mmdc_xml/2")
processDir(fields, indexer, "/Users/robzeeman/surfdrive/Documents/DI/e-codices/mmdc_xml/3")
processDir(fields, indexer, "/Users/robzeeman/surfdrive/Documents/DI/e-codices/mmdc_xml/4")
processDir(fields, indexer, "/Users/robzeeman/surfdrive/Documents/DI/e-codices/mmdc_xml/5")
processDir(fields, indexer, "/Users/robzeeman/surfdrive/Documents/DI/e-codices/mmdc_xml/6")
processDir(fields, indexer, "/Users/robzeeman/surfdrive/Documents/DI/e-codices/mmdc_xml/7")
processDir(fields, indexer, "/Users/robzeeman/surfdrive/Documents/DI/e-codices/mmdc_xml/8")
processDir(fields, indexer, "/Users/robzeeman/surfdrive/Documents/DI/e-codices/mmdc_xml/9")



