import urllib2
import json
import csv
import numpy
import pymongo
from pymongo import MongoClient
import sys
reload(sys)
sys.setdefaultencoding("utf-8")

client = MongoClient()
db = client['searchlogsclean']

# generation of a file from a single collection
def createCollection (collection, fileName):
    with open(fileName, 'w') as csvNew:
        fieldnames = ['_id', 'searchTerms', 'count']
        writer = csv.DictWriter(csvNew, fieldnames=fieldnames)
        writer.writeheader()
        for query in collection.find():
            row = {'_id': query['_id'], 'searchTerms': query['searchTerms'], 'count': query['count']}
            writer.writerow(row)



#createCollection(db.internalDguClean, 'allmongouniqueinternaldgu-csv.csv')
#createCollection(db.internalOnsClean, 'allmongouniqueinternalons-csv.csv')
#createCollection(db.internalAusClean, 'allmongointernalcanada-csv.csv')
createCollection(db.internalCanClean, 'searchlogs.csv')
