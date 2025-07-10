"""
Contains a Parser for XML files.
"""
import xmltodict
from typing import IO

import jmespath
from jsonpath_ng import jsonpath, parse

from procrustus_indexer.parsers import Parser

from saxonche import PySaxonProcessor

class XmlParser(Parser):
    """
    XML-specific Parser class.
    """
    config: dict

    def __init__(self, config: dict):
        if config["index"]["input"]["format"] != "xml":
            raise ValueError("XmlParser only supports XML files")
        self.config = config


    @staticmethod
    def resolve_path(data: dict, path: str):
        type, path = path.split(":", 1)
        if type == 'jmes':
            return jmespath.search(path, data)
        elif type == 'jsonpath':
            exp = parse(path)
            res = exp.find(data)
            if len(res) > 0:
                return res[0].value
        return None


    def parse_file(self, file: IO) -> dict:
        """
        Process an input XML file according to config and return resulting dict.
        :param infile:
        :return:
        """
        '''
        when = xpproc.evaluate_single("string((/*:CMD/*:Header/*:MdCreationDate/@clariah:epoch,/*:CMD/*:Header/*:MdCreationDate,'unknown')[1])").get_string_value()
        '''
        with PySaxonProcessor(license=False) as proc:
            xml_text = file.read()
            print(f'xml:\n{xml_text}')
            doc = None
            xpproc = proc.new_xpath_processor()
            node = proc.parse_xml(xml_text=xml_text)
            xpproc.set_context(xdm_item=node)
            for key in self.config['index']['input']['ns'].keys():
                xpproc.declare_namespace(key,self.config['index']['input']['ns'][key])
            do = True
            if 'when' in self.config['index']['input'].keys():
                do = xpproc.effective_boolean_value(f"{self.config['index']['input']['when']}")
            if do:
                path_id = self.config['index']['id']['path']
                doc_id = xpproc.evaluate_single(f"{path_id}").get_string_value() 
                doc = {'id': doc_id}
                for key in self.config['index']['facet'].keys():
                    facet = self.config["index"]["facet"][key]
                    path_facet = facet["path"]
                    cardinality="list"
                    if ('cardinality' in facet.keys()):
                        cardinality = facet['cardinality']
                    if cardinality == 'single':
                        val = xpproc.evaluate_single(f"{path_facet}").get_string_value()
                        if val.strip() != '':
                            doc[key] = val
                    else:
                        res = []
                        for item in xpproc.evaluate(f"{path_facet}"):
                            val = item.get_string_value()
                            if val.strip() != '':
                                res.append(val)
                        doc[key] = res
            return doc

