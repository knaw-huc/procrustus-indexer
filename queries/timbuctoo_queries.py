
class Timbuctoo_queries:
    def get_collections(self, dataset_id):
        return '{dataSetMetadata(dataSetId: "' + dataset_id + '") {dataSetId collectionList {items {collectionId collectionListId itemType total title {value}}}}}'

    def get_basic_collection_items(self, dataset_id, collection_id):
        return '{dataSets {' + dataset_id + ' {' + collection_id +  ' {total items {uri title {value}}}}}}'
