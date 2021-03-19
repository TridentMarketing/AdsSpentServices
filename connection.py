from pymongo import MongoClient
from bson import ObjectId

def mongodb_connection(clint_connection_str,db_name):
    try:
        client = MongoClient(clint_connection_str)
        db = client[db_name]
        return db
    except Exception as e:
        print (e)