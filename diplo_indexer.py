import datetime
import getopt
import os
import re
import sys
import unicodedata
import webvtt
import elementpath
from glob import glob
from os.path import basename
from elasticsearch.helpers import bulk
from lxml import etree
from cmdi_indexer import record_generator, grab_value, grab_list, cmdi_records_generator
from indexer import Indexer

ns = {"cmd": "http://www.clarin.eu/cmd/1"}


def makeFlatList(dir_path, wildcard):
    lijst = glob(dir_path + '*')
    new_lijst = []
    for map in lijst:
        l = glob(map + '/' + wildcard)
        for i in l:
            new_lijst.append(i)
    return new_lijst


def get_diplo_record(root):
    # print('rood' , root)
    ttl = grab_value("./cmd:Components/cmd:Interview/cmd:Titel[@xml:lang='nl']", root, ns)
    ttl_en = grab_value("./cmd:Components/cmd:Interview/cmd:Titel[@xml:lang='en']", root, ns)
    titel = grab_value("./cmd:Components/cmd:Interview/cmd:Geinterviewde/cmd:Naam/cmd:titel", root, ns)
    voornaam = grab_value("./cmd:Components/cmd:Interview/cmd:Geinterviewde/cmd:Naam/cmd:voornaam", root, ns)
    achternaam = grab_value("./cmd:Components/cmd:Interview/cmd:Geinterviewde/cmd:Naam/cmd:achternaam", root, ns)
    tussenvoegsel = grab_value("./cmd:Components/cmd:Interview/cmd:Geinterviewde/cmd:Naam/cmd:tussenvoegsel", root, ns)
    loc = grab_list("./cmd:Components/cmd:Interview/cmd:Geinterviewde/cmd:Carriere/cmd:Stationering/cmd:Locatie", root, ns)
    # statio_post = grab_list('locatie', "./cmd:Components/cmd:Interview/cmd:Geinterviewde/cmd:Carriere/cmd:Stationering/cmd:Post", root, ns)

    # opnamedata = grab_list('opnamedatum', "./cmd:Components/cmd:Interview/cmd:Opname/cmd:Opnamedatum", root, ns)

    stationeringen = elementpath.select(root, "./cmd:Components/cmd:Interview/cmd:Geinterviewde/cmd:Carriere/cmd:Stationering",
                                  ns)  # zonder slash pakt hij de xml elementen met slash alles wat eronder hangt
    # stationeringen = grab_list("./cmd:Components/cmd:Interview/cmd:Geinterviewde/cmd:Carriere/cmd:Stationering", ns) # zonder slash pakt hij de xml elementen met slash alles wat eronder hangt
    interviewsessies = elementpath.select(root, "./cmd:Components/cmd:Interview/cmd:Opname", ns)

    biblioinfo = elementpath.select(root, "./cmd:Components/cmd:Interview/cmd:Geinterviewde/cmd:Bibliografie/cmd:Entry", ns)
    print(biblioinfo)
    bibliolist = []
    for item in biblioinfo:
        # print('biblioitem', item)
        entry = {'api': '', 'order': ''}
        for el in item:
            tag = etree.QName(el.tag).localname
            if tag == 'zoterolink':
                item = re.compile('^https://.+?items/([^/]+).+')
                itemids = item.findall(el.text.strip())
                itemid = itemids[0]
                url = 'https://api.zotero.org/groups/5081406/items/' + itemid
                entry["api"] = url
            else:
                entry[tag] = el.text

        # pprint.pprint(entry)
        bibliolist.append(entry)

    bibliolist.sort(key=lambda obj: obj["order"])

    # print(type(stationeringen)
    # pprint.pprint(interviewsessies)
    # pprint.pprint(bibliolist)

    # loop through a list

    statio_list = []

    for item in stationeringen:
        if grab_value('./cmd:Display', item, ns) != 'no':
            # print('item:', item)
            statio = {'Organisatie': '', 'Departement': '', 'Post': '', 'Display': '', 'Type': '',
                      'Periode': {'Van': '', 'Tot': ''}}
            # statio = {}

            for el in item:
                # iek = grab_value("./cmd:departement", item, ns)
                # iek = grab_value("./cmd:departement/cmd:Organisatie", item, ns)
                # print(el, type(el),  "-",el.tag, "-" ,el.text)
                tag = etree.QName(el.tag).localname
                # print('tag: ', tag, 'localname: ', tag.localname)
                if tag == 'Periode':
                    statio[tag] = {'Van': '', 'Tot': ''}
                    van = grab_value("./cmd:Van", el, ns)
                    tot = grab_value("./cmd:Tot", el, ns)
                    statio[tag]['Van'] = van
                    statio[tag]['Tot'] = tot
                else:
                    statio[tag] = el.text

            statio_list.append(statio)

    # pprint.pprint(statio_list)
    # sorted_ls = sorted(statio_list, key=lambda obj: obj["Periode"]["Van"])
    statio_list.sort(key=lambda obj: obj["Periode"]["Van"])
    # https://stackoverflow.com/questions/69647232/how-to-sort-a-list-of-nested-dictionaries-based-on-a-deeply-nested-key

    sessie_list = []
    sessie = {'Opnamedatum': '', 'Volgorde:': '', 'Inhoud': [{'onderwerp': '', 'tijdstip': '', 'periode': ''}]}

    # sessie_list.append(sessie)

    for item in interviewsessies:
        # print('item:', item)
        sessie = {'Opnamedatum': '', 'Volgorde': '', 'Bewerking': '', 'Inhoud': [{'onderwerp': '', 'tijdstip': '', 'periode': ''}]}
        for el in item:
            tag = etree.QName(el.tag).localname
            # print('tag: ', tag, 'localname: ', tag.localname)
            sectielist = []
            if tag == 'Inhoud':
                # sectie = grab_list("./cmd:Sectie", el, ns)
                secties = elementpath.select(el, "./cmd:Sectie", ns)
                for it in secties:
                    sectie = {}
                    onderwerp = grab_value("./cmd:Onderwerp", it, ns)
                    tijdstip = grab_value("./cmd:Tijdstip", it, ns)
                    periodevan = grab_value("./cmd:Periode/cmd:Van", it, ns)
                    periodetot = grab_value("./cmd:Periode/cmd:Tot", it, ns)

                    sectie['onderwerp'] = onderwerp
                    sectie['tijdstip'] = tijdstip
                    sectie['periodevan'] = periodevan  # TODO uitwerken
                    sectie['periodetot'] = periodetot  # TODO uitwerken

                    sectielist.append(sectie)

                # pprint.pprint(sectielist)
                sectielist.sort(key=lambda obj: obj["tijdstip"])
                # https://www.w3docs.com/snippets/python/how-to-sort-a-list-of-objects-based-on-an-attribute-of-the-objects.html
                sessie[tag] = sectielist
            else:
                sessie[tag] = el.text

        sessie_list.append(sessie)

    sessie_list.sort(key=lambda obj: obj["Volgorde"])

    # print(statio_list, sep="\n")
    # print(sessie_list)
    # print('\n'.join(map(str, statio_list)))

    # buffer = {name : unicodedata.normalize("NFKC", item.text).strip()}
    # if buffer not in ret_arr:
    # ret_arr.append(buffer)

    # print(opnamedata[0])

    # stationeringen = grab_list('stationering', "./cmd:Components/cmd:Interview/cmd:Geinterviewde/cmd:Carriere/cmd:Stationering", root, ns)

    # print('loc' ,loc)
    return {
        "titel": ttl,
        "titel_en": ttl_en,
        "locaties": loc,
        "naam_titel": titel,
        "naam_voornaam": voornaam,
        "naam_achternaam": achternaam,
        "naam_tussenvoegsel": tussenvoegsel,
        "naam_volledig": titel + ' ' + voornaam + ' ' + tussenvoegsel + ' ' + achternaam,
        # "opnamedata": opnamedata,
        "stationeringen": statio_list,
        "interviewsessies": sessie_list,
        "bibliografie": bibliolist
    }


def index_vtt(records, vtt_records):
    lijst = []
    for vtt_directory in vtt_records:
        lijst.extend(makeFlatList(vtt_directory, '*.vtt')) # abs path vd directory, bash wildcard

    diplo_texts = {}
    diplo_sessions = {}

    # elke file in de map moet geindexeerd worden dus er doorheen lopen

    reggie = re.compile('([^-]+?)-(\d+?)_(\d+?)\.vtt') # teveel checks?
    nomatchers = []

    for file in lijst: # file is abs path, filename wordt relative name
        filename = os.path.basename(os.path.normpath(file))
        print('filename: ', filename)
        results = reggie.findall(filename) # onstaat een lijst met daarin 1 tupple met 3 elementen met geextraheerde info
        # print(results)
        if(not results):
            print('filename matched niet met reggie', filename)
            nomatchers.append(filename)
            continue

        shortname = results[0][0]
        cmdi = int(results[0][1]) # int (casten?) nummer van de cmdi file dus 1 persoon
        session = int(results[0][2]) # int
        sessiondate = None

        cmdi_gen = cmdi_records_generator(records)
        for cmdi_path in cmdi_gen:
            if basename(cmdi_path) == str(cmdi) + ".cmdi":
                cmdi_file = etree.parse(cmdi_path)
                cmdi_root = cmdi_file.getroot()
                opname = elementpath.select(cmdi_root, f"./cmd:Components/cmd:Interview/cmd:Opname/cmd:Volgorde[text()='{session}']/..", ns)
                sessiondate = grab_value('./cmd:Opnamedatum', opname[0], ns)

        print('id: ', cmdi, 'session: ', session, 'short', shortname, 'sessiondate: ', sessiondate, )
        # txt = ''
        if os.path.exists(file):
            print(file, 'exist')
        else:
            print(file, 'does not exist')

        sessions = diplo_sessions.get(cmdi) if cmdi in diplo_sessions else {}
        sessions[session] = sessiondate
        diplo_sessions[cmdi] = sessions

        global text
        text = diplo_texts.get(cmdi) + ' ' if cmdi in diplo_texts else ''
        init_dt = datetime.datetime(1900, 1, 1)

        def bulk_index():
            global text

            for caption in webvtt.read(file):
                start_dt = datetime.datetime.strptime(caption.start, "%H:%M:%S.%f")
                start_delta = start_dt - init_dt

                end_dt = datetime.datetime.strptime(caption.end, "%H:%M:%S.%f")
                end_delta = end_dt - init_dt

                text = text + ' ' + caption.text

                vtt = {
                    "_index": vtt_index,
                    "id": cmdi,
                    "session": session,
                    "sessiondate": sessiondate,
                    "shortname": shortname,
                    "start": start_delta.total_seconds(),
                    "end": end_delta.total_seconds(),
                    "text": unicodedata.normalize("NFKC", caption.text).strip()
                }

                yield vtt

        bulk(indexer.es, bulk_index())
        diplo_texts[cmdi] = unicodedata.normalize("NFKC", text).strip()

    for cmdi in diplo_texts:
        indexer.es.update(index=diplo_index, id=str(cmdi), doc={
            "transcript": diplo_texts[cmdi],
            "opnames": diplo_sessions[cmdi]
        })

    print('Deze matchden niet:', nomatchers)


diplo_index = "diplo"
vtt_index = "vttplus"
fields = "./fields/diplo.json"
records = []
host = 'localhost'
port = '9200'
user = None
password = None
use_ssl = False
vtt_records = []

opts, args = getopt.getopt(sys.argv[1:], 'h:p:u:w:sv:')

# Count the arguments
argc = len(args)

if (argc >= 0):
    diplo_index = args[0]

if (argc >= 1):
    vtt_index = args[1]

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
    elif option == '-v':
        vtt_records.append(value)

print ("diplo_index[" + diplo_index + "]")
print ("vtt_index[" + vtt_index + "]")
print ("fields[" + fields + "]")
print ("records[" + str(records) + "]")
print ("host[" + host + "]")
print ("port[" + port + "]")
print ("user[" + (user if user else "<no user>") + "]")
print ("password[" + (password if password else "<no password>") + "]")
print ("use_ssl[" + str(use_ssl) + "]")
print ("vtt_records[" + str(vtt_records) + "]")

indexer = Indexer({
    "host" : host,
    "port" : port,
    "http_auth": (user, password) if user and password else None,
    "index" : diplo_index,
    "use_ssl": use_ssl,
    "verify_certs": False
})

indexer.es.indices.delete(index=diplo_index, ignore_unavailable=True)
indexer.es.indices.delete(index=vtt_index, ignore_unavailable=True)

settings = {
    "analysis": {
        "analyzer": {
            "standard_lowercase_asciifolding": {  # Create a custom analyzer with the asciifolding filter enabled
                "tokenizer": "standard",
                "filter": ["lowercase", "asciifolding"]
            }
        }
    }
}

indexer.es.indices.create(index=diplo_index, settings=settings, mappings={
    "properties": {
        "id": {"type": "integer"},
        "titel": {"type": "keyword"},
        "achternaam": {"type": "keyword"},
        "land":  {"type": "keyword"},
        "organisatie":{"type": "keyword"},
        "samenvatting": {"enabled": "false"},
        "opnames": {"enabled": "false"},
        "record": {"enabled": "false"},
        "transcript": {
            "type": "text",
            "analyzer": "standard_lowercase_asciifolding",        # Use a custom analyzer to remove accents on the indexed text
            "search_analyzer": "standard_lowercase_asciifolding"  # Use a custom analyzer to remove accents on the search query
        }
    }
})

indexer.es.indices.create(index=vtt_index, settings=settings, mappings={
    "properties": {
        "id": {"type": "integer"},
        "session": {"type": "short"},
        "sessiondate": {"type": "date"},
        "shortname": {"type": "keyword"},
        "start": {"type": "float"},
        "end": {"type": "float"},
        "text": {
            "type": "text",
            "index_options": "offsets",                             # Use offsets to speed up highlighting
            "analyzer": "standard_lowercase_asciifolding",          # Use a custom analyzer to remove accents on the indexed text
            "search_analyzer": "standard_lowercase_asciifolding"    # Use a custom analyzer to remove accents on the search query
        }
    }
})

cmdi_gen = cmdi_records_generator(records)
record_gen = record_generator(cmdi_gen, fields, add_record=get_diplo_record)
indexer.add_to_index_bulk(record_gen)

index_vtt(records, vtt_records)
