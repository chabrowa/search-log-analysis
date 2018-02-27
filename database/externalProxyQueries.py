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
import pickle
import re

client = MongoClient()
db = client['searchlogsclean']

def isProxy(s):
    result = re.search('(\.|\s|^|\(|:|,|\/|_|"|\')ons(\.|\s|$|\)|:|,|\/|_|"|\')', s)
    if result != None:
        return True
    if s.find("gov") != -1 and s.find("uk") != -1:
        return True
    if s.find("o n s") != -1:
        return True
    if s.find("office") != -1 and s.find("national") != -1 and s.find("stat") != -1:
        return True

    return False

def splitQueries (collection):
    for query in collection.find():
        if isProxy(query['searchTerms']):
            collection.update_one({'_id': query['_id']}, {"$set": {"direct": False}})
        else:
            collection.update_one({'_id': query['_id']}, {"$set": {"direct": True}})
#print "DGU:"
#splitQueries(db.organicDguClean)
#print "ONS:"
#splitQueries(db.organicOnsClean)

def numberOfQueries (collection):
    countAll    = 0.00
    countProxy  = 0.00
    countDirect = 0.00
    for query in collection.find():
        if query['searchTerms'] != "(not provided)" and query['searchTerms'] != "(not set)":
            if query['noWords'] <=18 and query['noWords'] >=1:
                countAll = countAll + query['count']
                if query['direct'] == False:
                    countProxy = countProxy + query['count']
                else:
                    countDirect = countDirect + query['count']

    print "All: " + str(countAll)
    print "Proxy: " + str(countProxy)+" " +str(countProxy*100/countAll)
    print "Direct: " + str(countDirect)+" "+ str(countDirect*100/countAll)

#print "DGU:"
#numberOfQueries(db.organicDguClean)
#print "ONS:"
#numberOfQueries(db.organicOnsClean)

def numberOfLinks (collection):
    counter     = 0
    counterAll  = 0
    for query in collection.find():
        if query['searchTerms'] != "(not provided)" and query['searchTerms'] != "(not set)":
            if query['noWords'] <=18 and query['noWords'] >=1:
                if query['direct'] == False:
                    counterAll = counterAll + query['count']
                    s = query['searchTerms']
                    s = s.lower().strip()
                    if (s.find("data.gov.uk") != -1 or
                        s.find("ons.gov.uk") != -1 or
                        s.find("http") != -1 or
                        s.find("www") != -1
                    ):
                        counter = counter + query['count']
    print counter
    print counterAll
    print str(counter*100/counterAll)

#print "DGU:"
#numberOfLinks(db.organicDguClean)
#print "ONS:"
#numberOfLinks(db.organicOnsClean)



def averageProxy (collection):
    countAll    = 0.00
    countWords  = 0.00
    countInstances = 0
    for query in collection.find():
        if query['searchTerms'] != "(not provided)" and query['searchTerms'] != "(not set)":
            if query['noWords'] <=18 and query['noWords'] >=1:
                countInstances = countInstances + 1
                if query["direct"] == True:
                    countWords = countWords + (query['noWords']*query['count'])
                    countAll = countAll + query['count']
    print str(countWords/countAll)
    print countAll

print "DGU:"
averageProxy(db.organicDguClean)
print "ONS:"
averageProxy(db.organicOnsClean)
