import json
from handlers import timbuctoo_handler

def run():
    content = open("./data/models/stcn.json", )
    model = json.load(content)

    try:
        data_source = model["data"]["datasource"]["type"]
    except KeyError:
        data_source = "no_datasource"

    if data_source.lower() == "timbuctoo":
        handler = timbuctoo_handler.Timbuctoo_handler(model)
        handler.create_store()

run()

