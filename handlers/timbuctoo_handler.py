from elasticsearch import Elasticsearch
from indexer import Indexer
from queries import timbuctoo_queries
import os
import requests
import json

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
        results = requests.post(self.server, json = params, headers = {"Authorization": "b0ed48ca-765a-47b2-9514-94568dc73165",
                                                                    'Content-Type': 'application/json',
                                                                    "VRE_ID": self.dataset})
        return json.loads(results.text)

    def pack_item(self, item):
        collection_item = {}
        collection_item["uri"] = item["uri"]
        collection_item["title"] = item["title"]["value"]
        return json.dumps(collection_item)


    def create_collection_index(self, dataset_id, dataset_name, collection_id, collection_list_id):
        config = {
            "url" : "localhost",
            "port" : "9200",
            "index" : dataset_name.lower() + "_" + collection_id.lower()
        }
        count = 1000
        indexer = Indexer(config)
        query = self.qb.get_basic_collection_items(dataset_id, collection_list_id, "1000")
        items = self.fetch_data(query);
        cursor = items["data"]["dataSets"][dataset_id][collection_list_id]["nextCursor"]
        header = json.dumps({"index": {}})
        data = ""
        for item in items["data"]["dataSets"][dataset_id][collection_list_id]["items"]:
            #print(item["title"]["value"])
            packet = self.pack_item(item)
            data = data + header + "\n" + packet + "\n"
        #print(data)
        indexer.add_to_index(data)
        print(count)
        while cursor:
            count = count + 1000
            data = ""
            query = self.qb.get_basic_collection_items(dataset_id, collection_list_id, "1000", cursor)
            items = self.fetch_data(query);
            #items = json.loads(results.text)
            cursor = items["data"]["dataSets"][dataset_id][collection_list_id]["nextCursor"]
            for item in items["data"]["dataSets"][dataset_id][collection_list_id]["items"]:
                #print(item["title"]["value"])
                packet = self.pack_item(item)
                data = data + header + "\n" + packet + "\n"
            indexer.add_to_index(data)
            print(count)



    def create_index(self):
        query = self.qb.get_collections(self.dataset)
        collections = self.fetch_data(query)
        dataset_name = collections["data"]["dataSetMetadata"]["dataSetName"]
        for item in collections["data"]["dataSetMetadata"]["collectionList"]["items"]:
            if item["collectionId"] != "tim_unknown":
            #if item["collectionId"] == "schema_Organization":
                print("Creating index for " + item["collectionId"])
                self.create_collection_index(collections["data"]["dataSetMetadata"]["dataSetId"], dataset_name, item["collectionId"], item["collectionListId"])
        print("Done...")






