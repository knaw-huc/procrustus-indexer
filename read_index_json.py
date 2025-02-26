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
    ap.add_argument('-d', '--directory',
                    help="input directory")
    ap.add_argument('-t', '--tomlfile',
                    default='ineo.toml',
                    help="input file")
    ap.add_argument('-f', '--inputfile',
                    help="input file")
    args = vars(ap.parse_args())
    return args,ap

if __name__ == "__main__":
    stderr(datetime.today().strftime("start: %H:%M:%S"))
    args,ap = arguments()
    tomlfile = args['tomlfile']
    es = Elasticsearch()
    inputfile = ''
    inputdir = args['directory']
    inputlist = glob.glob(f'{inputdir}/*.json')
    if len(inputlist)==0:
        inputfile = args['inputfile']
        if inputfile==None:
            stderr(ap.print_help())
            end_prog(1)
        inputlist = [inputfile]
    with open(tomlfile, "rb") as f:
        config = tomllib.load(f)
        path_id = config['index']['id']['path']
        actions = []
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
                # add to index list
                actions.append({'_index':'text.idx','_id':id,'_source':doc})
#                result = es.index(index = 'text.idx', document = doc, id = id)
#                stderr(f"result: {result['result']}")
        # add to index:
        result = bulk(es,actions)

    end_prog(0)

