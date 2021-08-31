from indexer import Indexer
from lxml import etree
import glob, os
import json

#Functions
def processDir(config, indexer):
    os.chdir(config["xml"]["source"]["path"])
    for file in glob.glob("*.xml"):
        processFile(file, config, indexer)

def processFile(file, config, indexer):
    head, tail = os.path.split(file);
    jsonstruc = {"id": tail}
    parser = etree.XMLParser()
    dom = etree.parse(file, parser)
    root = dom.getroot()
    fields = config["xml"]["target"]["fields"]["elements"]

    for item in fields:
        element = root.xpath(item["xpath"], namespaces={'d': 'http://www.tei-c.org/ns/1.0'})
        try:
            jsonstruc[item["name"]] = element[0].text
        except IndexError:
            jsonstruc[item["name"]] = ''

    #print(json.dumps(jsonstruc, indent=4))


    indexer.add_to_index(jsonstruc)


# Main program
with open("data/config/config.json", 'r') as f:
    data = f.read()

config = json.loads(data)
indexer = Indexer(config["config"]["elasticSearch"])
print(config["metadata"]["description"] + " running...")
processDir(config, indexer)
#processFile(config["xml"]["source"]["path"] + "1.xml", config, indexer)
print(config["metadata"]["description"] + " done...")





