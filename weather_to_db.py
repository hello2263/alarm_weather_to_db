from pymongo import MongoClient
from pymongo.cursor import CursorType

def insert_item_one(mongo, data, db_name=None, collection_name=None):
    result = mongo[db_name][collection_name].insert_one(data).inserted_id
    return result

def insert_item_many(mongo, datas, db_name=None, collection_name=None):
    result = mongo[db_name][collection_name].insert_many(datas).inserted_ids
    return result

if __name__ == '__main__':
    host = "172.17.0.3"
    port = "27017"
    mongo = MongoClient(host, int(port))
    print(mongo)
    insert_item_one(mongo, {"name":"dogyu" }, "alarm", "test")
    insert_item_many(mongo, [{"name":"dogyu", "content":"nothing" }, {"name":"min", "content":"true"}], "alarm", "test")
