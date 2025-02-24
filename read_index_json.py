# -*- coding: utf-8 -*-
import argparse
from datetime import datetime
from datetime import date
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import glob
import jmespath
import json
import locale
locale.setlocale(locale.LC_ALL, 'nl_NL') 
import re
import sys
import tomllib

def stderr(text):
    sys.stderr.write("{}\n".format(text))

def end_prog(code=0):
    if code!=0:
        stderr(f'afgebroken met code: {code}')
    stderr(datetime.today().strftime("einde: %H:%M:%S"))
    sys.exit(code)

def arguments():
    ap = argparse.ArgumentParser(description='Read json and feed to ElasticSearch')
    ap.add_argument('-d', '-directory',
                    default="data",
                    help="input directory")
    ap.add_argument('-f', '--inputfile',
                    default="SASTA_processed.json",
                    help="input file")
    args = vars(ap.parse_args())
    return args

if __name__ == "__main__":
    stderr(datetime.today().strftime("start: %H:%M:%S"))
    args = arguments()
    es = Elasticsearch() #[config])
    #test()
    inputdir = ''
    inputfile = ''
    try:
        inputdir = args['inputdir']
        stderr(inputdir)
        inputlist = glob.glob(f'{inputdir}/*.json')
        stderr(inputlist)
    except:
        stderr('except')
        inputfile = args['inputfile']
        inputlist = [inputfile]
        stderr(inputlist)
    with open("ineo.toml", "rb") as f:
        jmes = tomllib.load(f)
        stderr("ineo.toml:")
        stderr(jmes)
        id_path = jmespath.search("index.id.path", jmes)
        facet_type = jmespath.search("index.facet.type.path",jmes)
        facet_activities = jmespath.search("index.facet.activities.path",jmes)
        facet_domains =jmespath.search("index.facet.domains.path",jmes)
    for inv in inputlist:
        with open(inv) as f:
            d = json.load(f)
            stderr(id_path[9:])
            # add_to_index
            data_id = jmespath.search(id_path[9:], d[0])
            result = es.index(index = 'text.idx', document = d[0], id = data_id)
            stderr(f"result: {result['result']}")
            # add_to_index
            data_facet_t = jmespath.search(facet_type[9:], d[0])
            result = es.index(index = 'text.idx', document = d[0], id = data_facet_t)
            stderr(f"result: {result['result']}")
            # add_to_index
            data_facet_a = jmespath.search(facet_activities[9:], d[0])
            result = es.index(index = 'text.idx', document = d[0], id = data_facet_a)
            stderr(f"result: {result['result']}")
            # add_to_index
            data_facet_d = jmespath.search(facet_domains[9:], d[0])
            result = es.index(index = 'text.idx', document = d[0], id = data_facet_d)
            stderr(f"result: {result['result']}")
            # add_to_index
            result = es.index(index = 'text.idx', document = d[0])
            stderr(f"result: {result['result']}")

    end_prog(0)

