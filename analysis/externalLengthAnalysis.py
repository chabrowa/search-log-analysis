import urllib2
import json
import csv
import numpy
import pymongo
from pymongo import MongoClient

client = MongoClient()
db = client['searchlogsclean']

def lengthAverage (collection):
    averageU        = 0.00
    noOfQueriesU    = 0.00
    averageA        = 0.00
    noOfQieriesA    = 0.00
    maxnowords      = 0
    for query in collection.find():
        if query['searchTerms'] != "(not provided)" and query['searchTerms'] != "(not set)":
            if maxnowords < query['noWords']:
                maxnowords = query['noWords']
            if query['noWords'] <= 18 and query['noWords'] >= 1:
                #average unique
                averageU = averageU + query['noWords']
                noOfQueriesU = noOfQueriesU + 1
                #average all
                averageA = averageA + (query['count'] * query['noWords'])
                noOfQieriesA = noOfQieriesA + query['count']

    averageUnique = averageU / noOfQueriesU
    averageAll = averageA / noOfQieriesA
    print "Unique: " + str(averageUnique)
    print "All: " + str(averageAll)
    print "MAX: " + str(maxnowords)


#print "ONS:"
#lengthAverage(db.organicOnsClean)
#print "DGU:"
#lengthAverage(db.organicDguClean)

def allDistribution (collection, param):
    lengthDistribution = {}
    for query in collection.find():
        if query['searchTerms'] != "(not provided)" and query['searchTerms'] != "(not set)":
            if str(query["direct"]) == param:
                if query['noWords'] in lengthDistribution:
                    lengthDistribution[query['noWords']] = lengthDistribution[query['noWords']] + query['count']
                else:
                    lengthDistribution[query['noWords']] = query['count']
    return lengthDistribution

def uniqueDistribution (collection, param):
    lengthDistributionUnique = {}
    for query in collection.find():
        if query['searchTerms'] != "(not provided)" and query['searchTerms'] != "(not set)":
            if str(query["direct"]) == param:
                if query['noWords'] in lengthDistributionUnique:
                    lengthDistributionUnique[query['noWords']] = lengthDistributionUnique[query['noWords']] + 1
                else:
                    lengthDistributionUnique[query['noWords']] = 1
    return lengthDistributionUnique

def keyexist (array, key):
    s = 0
    try:
        s = array[i]
    except:
        pass
    return str(s)

aONS = allDistribution(db.organicOnsClean, "True")
aDGU = allDistribution(db.organicDguClean, "True")

uONS = uniqueDistribution(db.organicOnsClean, "True")
uDGU = uniqueDistribution(db.organicDguClean, "True")

i = 0
print "No, ons a, ons u, dgu a, dgu u"
while i <= 157:
    a = keyexist(aONS, i)
    b = keyexist(uONS, i)
    c = keyexist(aDGU, i)
    d = keyexist(uDGU, i)

    print str(i) + ", " + a + ", " + b + ", " + c  + ", " + d

    i = i + 1
