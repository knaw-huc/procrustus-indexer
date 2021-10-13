from elasticsearch import Elasticsearch
from indexer import Indexer
from queries import timbuctoo_queries
import os

class Timbuctoo_handler:
    def __init__(self, model):
        self.model = model
        self.qb = timbuctoo_queries.Timbuctoo_queries()
        try:
            self.dataset = self.model["data"]["dataset"]["id"]
            self.server = self.model["data"]["datasource"]["properties"]["server"]
            self.port =self.model["data"]["datasource"]["properties"]["port"]
            self.auth = self.model["data"]["datasource"]["properties"]["user"]
        except:
            print("No dataset properties defined")
            os._exit()

    def fetch_data(self, query):
        pass

    def create_index(self):
        query = self.qb.get_collections(self.dataset)
        print(self.server)



