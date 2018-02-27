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
db = client['searchlogsunique']

from google.refine import refine, facet
import pystache

server = refine.RefineServer()
grefine = refine.Refine(server)

# undating collections with results from Open Refine
def cluster (projectID, collection):
    project = grefine.open_project(projectID)
    name_facet = facet.TextFacet('searchTerms')
    facet_response = project.compute_facets(name_facet)
    facets = facet_response.facets[0]
    cluster_response = project.compute_clusters('searchTerms')

    for cluster in cluster_response:
        counter = 0
        firstElement = {}
        for line in cluster:
            if counter == 0:
                firstElement = collection.find_one({"searchTerms": line['value']})
            else:
                try:
                    dead = collection.find_one({"searchTerms": line['value']})
                    firstElement['count'] = firstElement['count'] + dead['count']
                    collection.remove(dead['_id'])
                    collection.update_one({'_id': firstElement['_id']}, {"$set": {"count": firstElement['count']}})
                except:
                    pass

            counter = counter + 1

#cluster('2260847371624', db.internalDGU)
cluster('1777211981967', db.internalONS)
