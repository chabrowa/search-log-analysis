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

# _id searchTerms searchTermsTrim count
# Creating collection of unique queries
def createCollection (collection, fileName):
    with open(fileName, 'r') as csvTemp:
        readerTemp = csv.DictReader(csvTemp)
        for row in readerTemp:
            qArray = row['searchTermsTrim'].split()
            l = len(qArray)
            result = collection.update_one({
                "searchTerms": row['searchTermsTrim']
            }, {
                "$inc": {"count": int(row['count'])},
                "$setOnInsert": {"noWords": l},
            }, upsert=True)

createCollection(db.organicDguClean, 'searchqueriesDgu.csv')
createCollection(db.organicOnsClean, 'searchqueriesOns.csv')


#
# #_id searchTerms searchTermsTrim count
# def createCollection (collection, fileName):
#     with open(fileName, 'r') as csvTemp:
#         readerTemp = csv.DictReader(csvTemp)
#         for row in readerTemp:
#             isInDb = collection.find_one({"searchTerms": row['searchTermsTrim']})
#             if isInDb:
#                 isInDb['count'] = isInDb['count'] + int(row['count'])
#                 collection.update_one({'_id': isInDb['_id']}, {"$set": {"count": isInDb['count']}})
#                 print "True"
#             else:
#                 row['count'] = row['count'].replace(',', '')
#                 qArray = row['searchTermsTrim'].split()
#                 l = len(qArray)
#                 collection.insert_one({"searchTerms": unicode(row['searchTermsTrim'], errors = 'replace') , "count": int(row['count']), "noWords": l})
#                 print "False"
#
