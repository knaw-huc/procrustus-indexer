# -*- coding: utf-8 -*-
import argparse
from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import glob
import jmespath
import json
import locale
locale.setlocale(locale.LC_ALL, 'nl_NL')
from rdflib import Graph
import sys
from saxonche import PySaxonProcessor
import tomllib
from procrustus_indexer import build_indexer


def stderr(text):
    sys.stderr.write("{}\n".format(text))


def end_prog(code=0):
    if code != 0:
        stderr(f'afgebroken met code: {code}')
    stderr(datetime.today().strftime("einde: %H:%M:%S"))
    sys.exit(code)


def arguments():
    ap = argparse.ArgumentParser(description='Read json and feed to ElasticSearch')
    ap.add_argument('-d', '--directory',
                    help="input directory")
    ap.add_argument('-t', '--tomlfile',
                    default='ineo.toml',
                    help="toml file")
    ap.add_argument('-f', '--inputfile',
                    help="input file")
    ap.add_argument('-i', '--index', default='test-index')
    ap.add_argument('--force', action='store_true')
    args = vars(ap.parse_args())
    return args, ap


def main():
    stderr(datetime.today().strftime("start: %H:%M:%S"))
    args, ap = arguments()
    toml_file = args['tomlfile']
    with open(toml_file, "rb") as f:
        config = tomllib.load(f)

    for key in config['index']['facet'].keys():
                    facet = config["index"]["facet"][key]
                    path_facet = facet["path"]

    input_dir = args['directory']
    extension = config['index']['input']['format']
    input_list = glob.glob(f'{input_dir}/*.{extension}')
    if len(input_list) == 0:
        input_file = args['inputfile']
        if input_file is None:
            end_prog(1)
        input_list = [args['input_file']]

    index = args['index']    
    if 'name' in config['index']:
        index = config['index']['name']

    host="http://localhost:9200/"
    if 'host' in config['index']:
        host = config['index']['host']

    #indexer = Indexer(Elasticsearch(hosts=host), config, index)
    indexer = build_indexer(toml_file, 'index-name', Elasticsearch(hosts=host))

    indexer.create_mapping(overwrite=args['force'])
    indexer.import_files(input_list)

    end_prog(0)


if __name__ == "__main__":
    main()
