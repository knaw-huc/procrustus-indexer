from procrustus_indexer.extractors import Extractor
from procrustus_indexer.transformers import Transformer
from procrustus_indexer.loaders import Loader

class Pipeline:
    """
    Pipeline consisting of an extract step, multiple transform steps and a loader.
    """
    extractor: Extractor
    transformers: list[Transformer]
    loader: Loader

    def __init__(self, extract: Extractor, transformers: list[Transformer], loader: Loader):
        self.extractor = extract
        self.transformers = transformers
        self.loader = loader

    def apply_transformations(self, data: list):
        for transformer in self.transformers:
            data = transformer.transform_batch(data)
        return data


    def run_pipeline(self):
        bulks = self.extractor.generate_batch(500)
        results = (self.apply_transformations(bulk) for bulk in bulks)

        for result in results:
            self.loader.load_bulk(result)


    def insert_single(self, id):
        item = self.extractor.extract_single(id)

        [transformed] = self.apply_transformations([item])

        self.loader.load(transformed)
