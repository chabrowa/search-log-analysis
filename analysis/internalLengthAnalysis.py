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

    for query in collection.find():
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

print "ONS:"
lengthAverage(db.internalOnsClean)
print "DGU:"
lengthAverage(db.internalDguClean)
print "CAN:"
lengthAverage(db.internalCanClean)
print "AUS:"
lengthAverage(db.internalAusClean)

def allDistribution (collection):
    lengthDistribution = {}
    for query in collection.find():
        if query['noWords'] in lengthDistribution:
            lengthDistribution[query['noWords']] = lengthDistribution[query['noWords']] + query['count']
        else:
            lengthDistribution[query['noWords']] = query['count']
    return lengthDistribution

def uniqueDistribution (collection):
    lengthDistributionUnique = {}
    for query in collection.find():
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

#aONS = allDistribution(db.internalOnsClean)
#aDGU = allDistribution(db.internalDguClean)
#aCan = allDistribution(db.internalCanClean)
#aAus = allDistribution(db.internalAusClean)

#uONS = uniqueDistribution(db.internalOnsClean)
#uDGU = uniqueDistribution(db.internalDguClean)
#uCan = uniqueDistribution(db.internalCanClean)
#uAus = uniqueDistribution(db.internalAusClean)

#i = 0
#print "No, ons a, ons u, dgu a, dgu u, can a, can u, aus a, aus u"
#while i <= 374:
#    a = keyexist(aONS, i)
#    b = keyexist(uONS, i)
#    c = keyexist(aDGU, i)
#    d = keyexist(uDGU, i)
#    e = keyexist(aCan, i)
#    f = keyexist(uCan, i)
#    g = keyexist(aAus, i)
#    h = keyexist(uAus, i)
#    print str(i) + ", " + a + ", " + b + ", " + c  + ", " + d + ", " + e + ", " + f + ", " + g + ", " + h
#
#    i = i + 1
