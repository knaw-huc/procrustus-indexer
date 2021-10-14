from elasticsearch import Elasticsearch
from indexer import Indexer
from queries import timbuctoo_queries
import os
import requests

class Timbuctoo_handler:
    def __init__(self, model):
        self.model = model
        self.qb = timbuctoo_queries.Timbuctoo_queries()
        try:
            self.dataset = self.model["data"]["dataset"]["id"]
            self.server = self.model["data"]["datasource"]["properties"]["server"]
            self.port =self.model["data"]["datasource"]["properties"]["port"]
            self.user = self.model["data"]["datasource"]["properties"]["user"]
        except:
            print("No dataset properties defined")
            os._exit()

    def fetch_data(self, query):
        params = {"query": query}
        return requests.post(self.server, json = params, headers = {"Authorization": "13797e06-9999-43f3-be83-b27e6cf8c6fc",
                                                                    'Content-Type': 'application/json',
                                                                    "VRE_ID": self.dataset})

    def create_index(self):
        #query = self.qb.get_collections(self.dataset)
        query = self.qb.get_basic_collection_items(self.dataset, "schema_PersonList")
        collections = self.fetch_data(query)
        print(collections.text)




