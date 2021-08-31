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
def processFile(file, fields, indexer):
    json = {}
    with open(file, 'r') as f:
        data = f.read()

    Bs_data = BeautifulSoup(data, "xml")
    typeMan = Bs_data.select_one('meta[key="0500"]')
    if typeMan.get_text() == 'Fav':
        for key, value in fields.items():
            item = Bs_data.select_one('meta[key="' + key + '"]')
            if item is not None:
                json[value] = item.get_text()
            else:
                json[value] = ""
        indexer.add_to_index(json)

def processDir(fields, indexer, dir):
    os.chdir(dir)
    for file in glob.glob("*.xml"):
        processFile(file, fields, indexer)

# Start main program
fields = {
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
    "4063" : "layout",
    "4062" : "measure"

}

config = {
    "url" : "localhost",
    "port" : "9200",
    "doc_type" : "manuscripts"
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



