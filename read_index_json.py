# -*- coding: utf-8 -*-
import argparse
import glob
import locale
import sys
from datetime import datetime

from procrustus_indexer import build_indexer

locale.setlocale(locale.LC_ALL, 'nl_NL')

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
                    help="input file")
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
    input_dir = args['directory']
    index = args['index']
    input_list = glob.glob(f'{input_dir}/*.json')
    if len(input_list) == 0:
        input_file = args['inputfile']
        if input_file is None:
            stderr(ap.print_help())
            end_prog(1)
        input_list = [input_file]

    es = Elasticsearch()

    indexer = build_indexer(toml_file, index, es)

    indexer.create_mapping(overwrite=args['force'])
    indexer.import_files(input_list)

    end_prog(0)


if __name__ == "__main__":
    main()