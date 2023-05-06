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
        results = requests.post(self.server, json = params, headers={
                                                                    'Content-Type': 'application/json',
                                                                    "VRE_ID": self.dataset})
        return json.loads(results.text)

    def pack_item(self, item):
        collection_item = {}
        collection_item["uri"] = item["uri"]
        if item["title"]["value"]:
            collection_item["title"] = item["title"]["value"]
        else:
            collection_item["title"] = item["uri"]
        #if item["graphs"]:
        #    collection_item["graphs"] = item["graphs"]
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
        indexer.bulk_to_index(data)
        #print(count)
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
            indexer.bulk_to_index(data)
            #print(count)

    # Create search menu items for browser (store.py in procrustus service)
    def create_store(self):
        retDict = []
        query = self.qb.get_collections(self.dataset)
        collections = self.fetch_data(query)
        dataset_name = collections["data"]["dataSetMetadata"]["dataSetName"]
        for item in collections["data"]["dataSetMetadata"]["collectionList"]["items"]:
            if item["collectionId"] != "tim_unknown":
                buffer = {}
                buffer["collection"] = dataset_name.lower() + "_" + item["collectionId"].lower()
                buffer["collection_id"] = item["collectionId"]
                buffer["label"] = self.create_label(item["collectionId"])
                retDict.append(buffer)
        print(json.dumps(retDict))

    def create_label(self, str):
        retStr = str.split('_')
        retStr.reverse()
        return retStr[0]

    def create_index(self):
        readyColls = ["rpp_Agent",
                      "thes_Boedelpapieren",
                      "thes_Bruidegom",
                      "thes_Confessieboeken",
                      "thes_Geboorte",
                      "thes_LidmatenregisterDoopsgezindeGemeente",
                      "thes_Compagnieschap",
                      "thes_AkteVanExecuteurschap",
                      "thes_Begraven",
                      "rpp_DocumentType",
                      "oa_SpecificResource",
                      "thes_Bodemerij",
                      "thes_Herkomstlocatie",
                      "thes_Contract",
                      "skos_Concept",
                      "thes_Scheepsverklaring",
                      "thes_Transport",
                      "pnv_PersonName",
                      "thes_Bestek",
                      "rpp_Location",
                      "rpp_InventoryBook",
                      "thes_Boedelinventaris",
                      "thes_OverledenenGastPestWerkEnSpinhuis",
                      "thes_Partner",
                      "thes_Schipper",
                      "thes_Borgtocht",
                      "thes_Machtiging",
                      "thes_Relatieomschrijving",
                      "thes_Beroep",
                      "thes_Bijlbrief",
                      "rpp_Creator",
                      "thes_Testament",
                      "rpp_InventoryCollection",
                      "thes_Bevrachtingscontract",
                      "thes_NonPrejuditie",
                      "thes_Dopeling",
                      "thes_Bruid",
                      "thes_Moeder",
                      "thes_Attestatie",
                      "thes_DtbBegraven",
                      "thes_Interrogatie",
                      "thes_Koop",
                      "thes_HuwelijkseVoorwaarden",
                      "rpp_IndexDocument",
                      "thes_Notaris",
                      "thes_Ondertrouw",
                      "thes_Wisselprotest",
                      "thes_Cessie",
                      "thes_Ondertrouwregister",
                      "thes_Voogdij",
                      "thes_Onbekend",
                      "rpp_ScanCollection",
                      "thes_BewijsAanMinderjarigen",
                      "rpp_BookIndex",
                      "rpp_EventType",
                      "thes_Schenking",
                      "thes_Kwijtscheldingen",
                      "rpp_Event",
                      "rpp_Place",
                      "thes_Getuige",
                      "rpp_Archiver",
                      "thes_DtbDopen",
                      "thes_Consent",
                      "oa_Annotation",
                      "thes_Verdachte",
                      "thes_Registratie",
                      "thes_Overledene",
                      "thes_Kind",
                      "thes_ConventieEchtscheiding",
                      "thes_Geregistreerde",
                      "thes_Kwitantie",
                      "thes_Obligatie",
                      "thes_Verhoor",
                      "thes_Koper",
                      "rpp_Observation",
                      "thes_Poorterboeken",
                      "thes_EerdereVrouw",
                      "thes_Procuratie",
                      "thes_Hypotheek",
                      "rpp_DocumentArchiving",
                      "rpp_Relation",
                      "rpp_GroupingCriterion",
                      "thes_Verkoper",
                      "rpp_TimeInterval",
                      "thes_Huur",
                      "thes_Verklaring",
                      "rpp_Person",
                      "thes_Beraad",
                      "rpp_Scan",
                      "rpp_DocumentCreation",
                      "thes_Vader",
                      "rpp_Occupation",
                      "thes_Locatieomschrijving",
                      "rpp_RoleType",
                      "thes_EerdereMan",
                      "thes_NotarieleArchieven",
                      "thes_Trouwbelofte",
                      "thes_Averijgrossen",
                      "thes_Renunciatie",
                      "rpp_IndexCollection",
                      "thes_Overig",
                      "thes_AkteVanVoogdij",
                      "thes_Akkoord",
                      "thes_Revocatie",
                      "thes_Insinuatie",
                      "thes_Quitantie",
                      "thes_Boedelscheiding",
                      "thes_Uitspraak",
                      "thes_Overlijden",
                      "rpp_ArchivalDocument",
                      "thes_BoetesOpBegraven",
                      "thes_Doop",
                      "oa_FragmentSelector",
                      "rdfs_Resource",
                      "http___rdfs_org_ns_void_Dataset",
                      "tim_unknown"]
        query = self.qb.get_collections(self.dataset)
        collections = self.fetch_data(query)
        dataset_name = collections["data"]["dataSetMetadata"]["dataSetName"]
        for item in collections["data"]["dataSetMetadata"]["collectionList"]["items"]:
            if item["collectionId"] not in readyColls:
            #if item["collectionId"] == "edm_ProvidedCHO":
                print("Creating index for " + item["collectionId"])
                self.create_collection_index(collections["data"]["dataSetMetadata"]["dataSetId"], dataset_name, item["collectionId"], item["collectionListId"])
        print("Done...")






