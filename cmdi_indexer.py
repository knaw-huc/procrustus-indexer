# -*- coding: utf-8 -*-
import sys
from indexer import Indexer
import json
from lxml import etree
import glob, os
import unicodedata
import elementpath
import getopt

ns = {"cmd": "http://www.clarin.eu/cmd/"}

def grab_value(path, root, ns):
    content = elementpath.select(root, path, ns)
    if content and content[0].text is not None:
        return unicodedata.normalize("NFKC", content[0].text).strip()
    else:
        return ""

def grab_list(path, root, ns):
    ret_arr = []
    content = elementpath.select(root, path, ns)
    for item in content:
        buffer = unicodedata.normalize("NFKC", item.text).strip()
        if buffer not in ret_arr:
            ret_arr.append(buffer)
    return ret_arr

def cmdi_records_generator(records):
    for dir in records:
        os.chdir(dir)
        for cmdi in glob.glob("*.cmdi"):
            yield os.path.abspath(cmdi)

def record_generator(cmdi_records_gen, fields, add_record=None):
    f = open(fields)
    index_object = json.load(f)

    for cmdi in cmdi_records_gen:
        name = os.path.splitext(os.path.basename(cmdi))[0]
        try:
            id = int(name)
        except ValueError:
            id = name

        retDict = {'id': id}
        file = etree.parse(cmdi)
        root = file.getroot()

        for field in index_object:
            if field["fields"][0]["unbounded"] == "no":
                retDict[field["name"]] = grab_value(field["fields"][0]["path"], root, ns)
            else:
                retDict[field["name"]] = grab_list(field["fields"][0]["path"], root, ns)

        if add_record:
            retDict['record'] = add_record(root)

        print(retDict)
        yield retDict


if __name__ == '__main__':
    index = "cmdi"
    fields = "./fields.json"
    records = [ "./records" ]
    host = 'localhost'
    port = '9200'
    user = None
    password = None
    use_ssl = False

    opts, args = getopt.getopt(sys.argv[1:], 'h:p:u:w:s')

    # Count the arguments
    argc = len(args)

    if (argc >= 0):
        index = args[0]

    if (argc >= 1):
        fields = args[1]

    if (argc > 2):
        pos = 2
        records = []
        while (argc > pos):
            records.append(args[pos])
            pos = pos + 1

    for option, value in opts:
        if option == '-h':
            host = value
        elif option == '-p':
            port = value
        elif option == '-u':
            user = value
        elif option == '-w':
            password = value
        elif option == '-s':
            use_ssl = True

    print ("index[" + index + "]")
    print ("fields[" + fields + "]")
    print ("records[" + str(records) + "]")
    print ("host[" + host + "]")
    print ("user[" + (user if user else "<no user>") + "]")
    print ("password[" + (password if password else "<no password>") + "]")
    print ("use_ssl[" + str(use_ssl) + "]")

    indexer = Indexer({
        "host" : host,
        "port" : port,
        "http_auth": (user, password) if user and password else None,
        "index" : index,
        "use_ssl": use_ssl,
        "verify_certs": False
    })

    cmdi_gen = cmdi_records_generator(records)
    record_gen = record_generator(cmdi_gen, fields)
    indexer.add_to_index_bulk(record_gen)
