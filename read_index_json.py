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

def resolve_path(rec, path):
    if(path.startswith("jmes:")):
        # for jmes: 5:
        return jmespath.search(path[5:],rec)

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
    es = Elasticsearch()
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
        config = tomllib.load(f)
        path_id = conf['index']['id']['path']
        for inv in inputlist:
            doc = {}
            with open(inv) as f:
                d = json.load(f)
                id = resolve_path(d,path_id)
                doc['id'] = id
                for key in config['index']['facet'].keys():
                    facet = config["index"]["facet"][key]
                    path_facet = facet["path"]
                    doc[key] = resolve_path(d, path_facet)
                # add_to_index
                result = es.index(index = 'text.idx', document = doc, id = id)
                stderr(f"result: {result['result']}")

    end_prog(0)

