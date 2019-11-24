
import pymongo

myclient = pymongo.MongoClient("mongodb://localhost:27017/")

db = myclient.get_database('ceidg')