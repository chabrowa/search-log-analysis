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

client = MongoClient()
db = client['searchlogsclean']

questionWordsArray = ['what', 'who', 'where', 'when', 'why', 'how', 'which', 'whom', 'whose', 'whether', 'did', 'do', 'does', 'am', 'are', 'is', 'will', 'have', 'has']
def isQuestion (q):
    q = q.lower()
    qArray = q.split()
    for i in qArray:
        if i in questionWordsArray:
            return True
        else:
            return False


def questionQueries (collection):
    isThisQuestion  = False
    counter         = 0
    counterAll      = 0
    for row in collection.find():
        if row['noWords'] <=18 and row['noWords'] >=1:
            counterAll = counterAll + int(row['count'])
            if isQuestion(row['searchTerms']):
                counter = counter + int(row['count'])

    print "Question queries: " + str(counter)
    print "All:" + str(counterAll)

print "DGU"
questionQueries(db.internalDguClean)
print "ONS"
questionQueries(db.internalOnsClean)
print "AUS"
questionQueries(db.internalAusClean)
print "CAN"
questionQueries(db.internalCanClean)
