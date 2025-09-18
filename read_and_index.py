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
import os
from rdflib import Graph
import re
import sys
from saxonche import PySaxonProcessor
import tomllib
from procrustus_indexer import build_indexer


def stderr(text,nl="\n"):
    sys.stderr.write(f"{text}{nl}")


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
                    help="toml file")
    ap.add_argument('-f', '--inputfile',
                    help="input file")
    ap.add_argument('-i', '--index', default='test-index')
    ap.add_argument('--force', action='store_true')
    args = vars(ap.parse_args())
    return args

def read_and_index(toml_file: str, input_dir: str| None=None, input_file: str| None=None, index_name: str | None=None, index_host: str | None=None, force: bool | None=True):
    stderr(datetime.today().strftime("start: %H:%M:%S"))
    with open(toml_file, "rb") as f:
        config = tomllib.load(f)

    extension = config['index']['input']['format']
    stderr(f'extension: {extension}')
    if input_dir:
        input_list = glob.glob(f'{input_dir}/*.{extension}')
    elif input_file:
        input_list = [input_file]
    else:
        stderr(f"ERR: no input file or dir!")
        return "ERR: no input file or dir!"

    index = "test"
    if 'name' in config['index']:
        index = config['index']['name']
    if index_name:
        index = index_name
    stderr(f"INDEX[{index}]")

    host = os.getenv("ES_URL", "http://localhost:9200/")
    if 'host' in config['index']:
        md = re.search(r'^([^/]+//)([^:]+):([^@]+)@(.+)$',config['index']['host'])
        host = f'{md[1]}{md[4]}'
        auth = md[2]
        password = md[3]
    if index_host:
        host = index_host
    stderr(f"HOST[{host}]")

    indexer = build_indexer(toml_file, index, Elasticsearch(hosts=host,basic_auth=(auth, password)))

    indexer.create_mapping(overwrite=force)
    indexer.import_files(input_list)
    return "OK"


def main():
    args = arguments()
    res = read_and_index(toml_file=args['tomlfile'], input_dir=args['directory'], index_name= args['index'], force=args['force'])
    if res=='OK':
        end_prog(0)
    else:
        stderr(res)
        end_prog(1)


if __name__ == "__main__":
    main()
