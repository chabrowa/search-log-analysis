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
db = client['searchlogs']

# Populating database collection with search queries
def dbInsert (collection, fileName):
    with open(fileName) as csvTemp:
        readerTemp = csv.DictReader(csvTemp)
        for row in readerTemp:
            row['Pageviews'] = row['Pageviews'].replace(',', '')
            qArray = row['Search Term'].split()
            l = len(qArray)
            collection.insert_one({"searchTerms": unicode(row['Search Term'], errors = 'replace') , "count": int(row['Pageviews']), "noWords": l})



dbInsert(db.internalQueries, 'search_queries.csv')
