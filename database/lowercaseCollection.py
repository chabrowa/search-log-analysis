import urllib2
import json
import csv
import numpy
import pymongo
from pymongo import MongoClient
import sys
import os
reload(sys)
sys.setdefaultencoding("utf-8")

client  = MongoClient()
db      = client['searchlogsclean']

def lowercasedb (collection):
    for query in collection.find():
        newQuery = query["searchTerms"].strip().lower()
        collection.update_one({'_id': query['_id']}, {"$set": {"searchTermsClean": newQuery}})


print "E Dgu"
lowercasedb(db.organicDguClean)
print "E Ons"
lowercasedb(db.organicOnsClean)
#print "Dgu"
#lowercasedb(db.internalDguClean)
#print "Ons"
#lowercasedb(db.internalOnsClean)
#print "Can"
#lowercasedb(db.internalCanClean)
#print "Aus"
#lowercasedb(db.internalAusClean)
