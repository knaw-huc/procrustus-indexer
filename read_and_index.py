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


class Indexer:
    es: Elasticsearch = None
    config: dict
    index_name: str

    def __init__(self, es: Elasticsearch, config: dict, index_name: str):
        self.es = es
        self.config = config
        self.index_name = index_name

    def parse_json(self, infile: dict) -> dict:
        """
        Process an input JSON file according to config and return resulting dict.
        :param infile:
        :return:
        """
        path_id = self.config['index']['id']['path']
        doc_id = resolve_path(infile, path_id)
        doc = {'id': doc_id}
        for key in self.config['index']['facet'].keys():
            facet = self.config["index"]["facet"][key]
            path_facet = facet["path"]
            doc[key] = resolve_path(infile, path_facet)
        return doc

    def parse_xml(self, infile: str) -> dict:
        """
        Process an input XML file according to config and return resulting dict.
        :param infile:
        :return:
        """
        '''
        when = xpproc.evaluate_single("string((/*:CMD/*:Header/*:MdCreationDate/@clariah:epoch,/*:CMD/*:Header/*:MdCreationDate,'unknown')[1])").get_string_value()
        '''
        with PySaxonProcessor(license=False) as proc:
            xpproc = proc.new_xpath_processor()
            ns_name = list(self.config['index']['input']['ns'].keys())[0]
            ns_adress = self.config['index']['input']['ns'][ns_name]
            xpproc.declare_namespace(ns_name,ns_adress)
            path_id = self.config['index']['id']['path'][6:]
            node = proc.parse_xml(xml_text=infile)
            xpproc.set_context(xdm_item=node)
            doc_id = xpproc.evaluate_single(f"{path_id}").get_string_value()
            doc = {'id': doc_id}
            for key in self.config['index']['facet'].keys():
                facet = self.config["index"]["facet"][key]
                path_facet = facet["path"][6:]
                doc[key] = xpproc.evaluate_single(f"{path_facet}").get_string_value()
            return doc

    def parse_sparql(self, infile: str) -> dict:
        """
        Process an input SPARQL file according to config and return resulting dict.
        :param infile:
        :return:
        """
        g = Graph()
        g.parse(data=infile, format="turtle")
        path_id = self.config['index']['id']['path']
        path_id = g.query(path_id)
        doc = { 'id': path_id }
        for key in self.config['index']['facet'].keys():
            qres = g.query(self.config["index"]["facet"][key]['path'])
            for row in qres:
                stderr(f'row: {row}')
                try:
                    doc['key'] = row.key
                except:
                    pass
        return doc

    def create_mapping(self, overwrite: bool = False) -> dict:
        """
        Create the elasticsearch index mapping according to config and return resulting dict.
        :return:
        """
        if overwrite:
            self.es.indices.delete(index=self.index_name, ignore=[400, 404])

        properties = {}
        for facet_name in self.config['index']['facet'].keys():
            facet = self.config["index"]["facet"][facet_name]
            type = facet.get('type', 'text')
            if type == 'text':
                properties[facet_name] = {
                    'type': 'text',
                    'fields': {
                        'keyword': {
                            'type': 'keyword',
                            'ignore_above': 256
                        },
                    }
                }
            elif type == 'keyword':
                properties[facet_name] = {
                    'type': 'keyword',
                }
            elif type == 'number':
                properties[facet_name] = {
                    'type': 'integer',
                }
            elif type == 'date':
                properties[facet_name] = {
                    'type': 'date',
                }

        mappings = {
            'properties': properties
        }

        settings = {
            'number_of_shards': 2,
            'number_of_replicas': 0
        }

        # misschien aanpassen naar create_if_not_exists
        self.es.indices.create(index=self.index_name, mappings=mappings, settings=settings)
        return mappings


    def import_files(self, files: list[str]):
        """
        Import files into an elasticsearch index based on the given config.
        :param files: list of files to import
        :param index: Elasticsearch index
        :return:
        """
        es = Elasticsearch()
        actions = []
        self.extension = self.config['index']['input']['format']
        for inv in files:
            doc = {}
            with open(inv) as f:
                if self.extension=='json':
                    d = json.load(f)
                    # add to index list
                    doc = self.parse_json(d)
                elif self.extension=='xml':
                    d = f.read()                   
                    doc = self.parse_xml(d)
                elif self.extension=='ttl':
                    d = f.read()                   
                    doc = self.parse_sparql(d)
                else:
                # check if doc exists?
                # just in case someone tries to index something else than json or xml?
                    stderr(f'we dont do {extension} yet.')
                    end_prog(1)
                actions.append({'_index': self.index_name, '_id': doc['id'], '_source': doc})
        # add to index:
        result = bulk(es, actions)


def resolve_path(rec, path):
    if path.startswith("jmes:"):
        # for jmes: 5:
        return jmespath.search(path[5:], rec)


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

    input_dir = args['directory']
    index = args['index']
    extension = config['index']['input']['format']
    stderr(f'extension: {extension}')
    input_list = glob.glob(f'{input_dir}/*.{extension}')
    if len(input_list) == 0:
        input_file = args['inputfile']
        if input_file is None:
            stderr(ap.print_help())
            end_prog(1)
        input_list = [args['input_file']]

    indexer = Indexer(Elasticsearch(), config, index)

    indexer.create_mapping(overwrite=args['force'])
    indexer.import_files(input_list)

    end_prog(0)


if __name__ == "__main__":
    main()
