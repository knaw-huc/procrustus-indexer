
class Timbuctoo_queries:
    def get_collections(self, dataset_id):
        return '{dataSetMetadata(dataSetId: "' + dataset_id + '") {dataSetId dataSetName collectionList {items {collectionId collectionListId itemType total title {value}}}}}'

    def get_basic_collection_items(self, dataset_id, collection_id, count, cursor=""):
        if cursor == "":
            return '{dataSets {' + dataset_id + ' {' + collection_id +  '(count: ' + count + ') {total prevCursor nextCursor items {uri title {value} graphs}}}}}'
        else:
            return '{dataSets {' + dataset_id + ' {' + collection_id +  '(cursor: "' + cursor + '" count: ' + count + ') {total prevCursor nextCursor items {uri title {value} graphs}}}}}'

