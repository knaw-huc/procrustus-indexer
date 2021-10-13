
class Timbuctoo_queries:
    def get_collections(self, dataset_id):
        return '{dataSetMetadata(dataSetId: "' + dataset_id + '") {dataSetId collectionList {items {collectionId collectionListId itemType total title {value}}}}}'

