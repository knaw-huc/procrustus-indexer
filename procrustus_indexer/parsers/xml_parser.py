from typing import IO, List
from saxonche import PySaxonProcessor, PyXPathProcessor

from procrustus_indexer.parsers import Parser


class XmlParser(Parser):
    """
    XML-specific Parser class.
    """
    processor: PySaxonProcessor
    xp_processor: PyXPathProcessor

    def __init__(self, config: dict) -> None:
        super().__init__(config)
        self.processor = PySaxonProcessor(license=False)
        self.xp_processor = self.processor.new_xpath_processor()

        for key in self.config['index']['input']['ns'].keys():
            self.xp_processor.declare_namespace(key, self.config['index']['input']['ns'][key])

    def supported_types(self) -> List[str]:
        return ["xml"]


    def resolve_path(self, path: str, is_array: bool = False):
        """
        Resolve an XPATH in the data
        :param path:
        :param is_array:
        :return:
        """
        if is_array:
            res = []
            for item in self.xp_processor.evaluate(path):
                val = item.get_string_value()
                if val.strip() != '':
                    res.append(val)
            return res
        return self.xp_processor.evaluate_single(path).get_string_value().strip()


    def should_process_file(self, file: IO) -> bool:
        """
        Check if the file meets the conditions for processing. XML files can use an XPath expression
        resulting in a boolean for this.
        :param file:
        :return:
        """
        node = self.processor.parse_xml(xml_text=file)
        self.xp_processor.set_context(xdm_item=node)
        if 'when' in self.config['index']['input'].keys():
            return self.xp_processor.effective_boolean_value(self.config['index']['input']['when'])
        return True


    def parse_file(self, file: IO) -> dict:
        node = self.processor.parse_xml(xml_text=file)
        self.xp_processor.set_context(xdm_item=node)

        for key in self.config['index']['input']['ns'].keys():
            self.xp_processor.declare_namespace(key, self.config['index']['input']['ns'][key])

        id_path = self.config['index']['id']['path']
        doc = {'id': self.resolve_path(id_path)}

        for key in self.config['index']['facet'].keys():
            facet = self.config['index']['facet'][key]
            path = facet["path"]
            cardinality = facet["cardinality"]
            doc[key] = self.resolve_path(path, is_array=cardinality=='list')

        return doc
