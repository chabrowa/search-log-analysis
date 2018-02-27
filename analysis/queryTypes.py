import urllib2
import json
import csv
import numpy
import wget
import os
import pprint
import pymongo
from pymongo import MongoClient
import sys
import os
reload(sys)
sys.setdefaultencoding("utf-8")

client  = MongoClient()
db      = client['searchlogsclean']


def hasNumbers(inputString):
    return any(char.isdigit() for char in inputString)

def hasOnlyNumbers(inputString):
    try:
        float(inputString)
        return True
    except ValueError:
        return False

def findNumber (s):
    for number in numbers:
        if s.find(number) != -1:
            return False
    if hasOnlyNumbers(s):
        return True
    else:
        return False

def findTime (s):
    s = s.lower()
    for number in numbers:
        if s.find(number) != -1:
            return True
    if (s.find("month") != -1 or
        s.find("year") != -1 or
        s.find("week") != -1 or
        s.find("jan") != -1 or
        s.find("feb") != -1 or
        s.find("mar") != -1 or
        s.find("apr") != -1 or
        s.find("may") != -1 or
        s.find("jun") != -1 or
        s.find("jul") != -1 or
        s.find("aug") != -1 or
        s.find("sep") != -1 or
        s.find("oct") != -1 or
        s.find("nov") != -1 or
        s.find("dec") != -1 or
        s.find("january") != -1 or
        s.find("february") != -1 or
        s.find("march") != -1 or
        s.find("april") != -1 or
        s.find("june") != -1 or
        s.find("july") != -1 or
        s.find("august") != -1 or
        s.find("september") != -1 or
        s.find("october") != -1 or
        s.find("november") != -1 or
        s.find("december") != -1 or
        s.find("monday") != -1 or
        s.find("tuesday") != -1 or
        s.find("wednesday") != -1 or
        s.find("thursday") != -1 or
        s.find("friday") != -1 or
        s.find("saturday") != -1 or
        s.find("sunday") != -1 or
        s.find("summer") != -1 or
        s.find("winter") != -1 or
        s.find("spring") != -1 or
        s.find("autumn") != -1 or
        s.find("q1") != -1 or
        s.find("q2") != -1 or
        s.find("q3") != -1 or
        s.find("q4") != -1 or
        s.find("annual") != -1 or
        s.find("fortnight") != -1 or
        s.find("biannual") != -1 or
        s.find("bimonth") != -1 or
        s.find("day") != -1 or
        s.find("quarter") != -1 or
        s.find("biyear") != -1 or
        s.find("season") != -1
        ):
        return True
    else:
        return False

numbers = ["1920","1921","1922","1923","1924","1925","1926","1927","1928","1929","1930","1931","1932","1933","1934","1935"
,"1936","1937","1938","1939","1940","1941","1942","1943","1944","1945","1946","1947","1948","1949","1950","1951"
,"1952","1953","1954","1955","1956","1957","1958","1959","1960","1961","1962","1963","1964","1965","1966","1967"
,"1968","1969","1970","1971","1972","1973","1974","1975","1976","1977","1978","1979","1980","1981","1982","1983"
,"1984","1985","1986","1987","1988","1989","1990","1991","1992","1993","1994","1995","1996","1997","1998","1999"
,"2000","2001","2002","2003","2004","2005","2006","2007","2008","2009","2010","2011","2012","2013","2014","2015"
,"2016","2017","2018","2019","2020","2021","2022"]

abbreviations = ["rpi","cpi","gdp","lidar","defra","acnc","gnaf","asic","lsoa","psma","fdi","awe","ssi",
"dss","gva","ppi","chaw","abn","eu","ccg","gis","gp","efd","hmrc","uprn","ato","imd","sme",
"bop","mod","tfl","ccj","tpo","epims","dfId","dtm","fco","sic","pbs","nsw","ufmfsm","lsoa",
"bis","copd","hpi","gva","rip","aonb","dclg","abs","vosa","nptg","msoa","gpc","voa","decc",
"hes","mbs","chmk","abmi","bgs","abr","minap","sme","cefas","lfs","dem","ukea","rpi","cpi",
"gdp","lidar","defra","acnc","gnaf","mot","asic","ons","lsoa","sparql","psma","fdi","hs2",
"awe","ssi","ds","api","gva","ppi","c.h.a.w","abn","eu","gis","ato","lga","imd","tfl"]
def hasAbr (s):
    s = s.lower()
    sArray = s.split(" ")
    for word in sArray:
        if word in abbreviations:
            return True
    return False

abrArray = ["rpi", "cpi", "gdp", "lidar", "defra", "acnc", "gnaf", "asic", "lsoa", "psma", "fdi", "awe", "ssi",
"dss","gva", "ppi", "chaw", "abn", "eu", "ccg", "gis", "efd", "hmrc", "uprn", "ato", "imd", "sme", "bop", "tfl", "gis"]
def isAbr (s):
    s = s.lower().strip()
    if s in abrArray:
        return True
    else:
        return False

def hasData2 (s):
    s = s.lower()
    if (s.find("data") != -1 or
        s.find("dataset") != -1 or
        s.find("average") != -1 or
        s.find("index") != -1 or
        s.find("statistic") != -1 or
        s.find("database") != -1 or
        s.find("graph") != -1 or
        s.find("table") != -1 or
        s.find("indice") != -1 or
        s.find("rpi") != -1 or
        s.find("download") != -1 or
        s.find("api") != -1 or
        s.find("cpi") != -1 or
        s.find("r.p.i") != -1 or
        s.find("stat") != -1 or
        s.find("survey") != -1 or
        s.find("map") != -1 or
        s.find("rate") != -1 or
        s.find("total") != -1 or
        #s.find("file") != -1 or
        s.find("review") != -1 or
        s.find("trend") != -1 or
        s.find("information") != -1 or
        s.find("html") != -1 or
        s.find("csv") != -1 or
        s.find("wms") != -1 or
        s.find("wcs") != -1 or
        s.find("xls") != -1 or
        s.find("pdf") != -1 or
        s.find("json") != -1 or
        s.find("wfs") != -1 or
        s.find("esri rest") != -1 or
        s.find("zip") != -1

        ):
        return True
    else:
        return False

def hasHuman (s):
    s = s.lower()
    if (s.find("student") != -1 or
        s.find("retire") != -1 or
        s.find("birth") != -1 or
        s.find("older") != -1 or
        s.find("children") != -1
    ):
        return True
    else:
        return False

def hasCost (s):
    s = s.lower()
    #dept? deficit? rpi cpi gdp
    if (s.find("price") != -1 or
        s.find("cost") != -1 or
        s.find("spend") != -1 or
        s.find("tax") != -1 or
        s.find("earn") != -1 or
        s.find("spend") != -1 or
        s.find("pay") != -1 or
        s.find("pension") != -1 or
        s.find("income") != -1 or
        s.find("depth") != -1 or
        s.find("salary") != -1 or
        s.find("salaries") != -1 or
        s.find("inflation") != -1 or
        s.find("wage") != -1 or
        s.find("sales") != -1 or
        s.find("wealth") != -1 or
        s.find("benefits") != -1 or
        s.find("financ") != -1 or
        s.find("budget") != -1 or
        s.find("fund") != -1 or
        s.find("saving") != -1 or
        s.find("recession") != -1 or
        s.find("mortgage") != -1 or
        s.find("vat") != -1

        ):
        return True
    else:
        return False


def hasFormat (s):
    s = s.lower()
    if s.find("html") != -1 or s.find("csv") != -1 or s.find("wms") != -1 or s.find("wcs") != -1 or s.find("xls") != -1 or s.find("pdf") != -1 or s.find("json") != -1 or s.find("wfs") != -1 or s.find("esri rest") != -1 or s.find("zip") != -1:
        return True
    else:
        return False

def hasFormatSmall (s):
    s = s.lower()
    if s.find("html") != -1 or s.find("csv") != -1 or s.find("wms") != -1 or s.find("wcs") != -1 or s.find("xls") != -1 or s.find("json") != -1 or s.find("wfs") != -1 or s.find("esri rest") != -1:
        return True
    else:
        return False

def findLocation (s):
    s = s.lower()
    if (
        s.find("london") != -1 or
        s.find("ireland") != -1 or
        s.find("england") != -1 or
        s.find("wales") != -1 or
        s.find("scotland") != -1 or
        s.find("bristol") != -1 or
        s.find("manchester") != -1 or
        s.find("liverpool") != -1 or
        s.find("leeds") != -1 or
        s.find("leicester") != -1 or
        s.find("plymouth") != -1 or
        s.find("devon") != -1 or
        s.find("newcastle") != -1 or
        s.find("wolverhampton") != -1 or
        s.find("hertfordshire") != -1 or
        s.find("nottingham") != -1 or
        s.find("birmingham") != -1 or
        s.find("york") != -1 or
        s.find("cambridge") != -1 or
        s.find("southampton") != -1 or
        s.find("brownfield") != -1 or
        s.find("edinburgh") != -1 or
        s.find("kent") != -1 or
        s.find("hampshire") != -1 or
        s.find("norfolk") != -1 or
        s.find("rochdale") != -1 or
        s.find("oldham") != -1 or
        s.find("salford") != -1 or
        s.find("bradford") != -1 or
        s.find("durham") != -1 or
        s.find("lichfield") != -1 or
        s.find("southwark") != -1 or
        s.find("staffordshire") != -1 or
        s.find("somerset") != -1 or
        s.find("bath ") != -1 or
        s.find("suffolk") != -1 or
        s.find("luton") != -1 or
        s.find("warwickshire") != -1 or
        s.find("westminster") != -1 or
        s.find("cardiff") != -1 or
        s.find("cumbria") != -1 or
        s.find("wycombe") != -1 or
        s.find("cornwall") != -1 or
        s.find("derbyshire") != -1 or
        s.find("glasgow") != -1 or
        s.find("guildford") != -1 or
        s.find("lancashire") != -1 or
        s.find("milton keynes") != -1 or
        s.find("north lincolnshire") != -1 or
        s.find("oxfordshire") != -1 or
        s.find("portsmouth") != -1 or
        s.find("surrey") != -1 or
        s.find("wakefield") != -1 or
        s.find("wigan") != -1 or
        s.find("calderdale") != -1 or
        s.find("doncaster") != -1 or
        s.find("gloucestershire") != -1 or
        s.find("islington") != -1 or
        s.find("lambeth") != -1 or
        s.find("lincolnshire") != -1 or
        s.find("shire") != -1
        ):
        return True
    else:
        return False

def findCategories (s):
    s = s.lower()
    if (
        s.find("environment") != -1 or
        s.find("town") != -1 or
        s.find("cities") != -1 or
        s.find("city") != -1 or
        s.find("mapping") != -1 or
        s.find("government") != -1 or
        s.find("society") != -1 or
        s.find("health") != -1 or
        s.find("spending") != -1 or
        s.find("education") != -1 or
        s.find("business") != -1 or
        s.find("economy") != -1 or
        s.find("transport") != -1
        ):

        return True
    else:
        return False

def findLicence (s):
    s = s.lower()
    if (
        s.find("licence") != -1 or
        s.find("license") != -1
        ):
        return True
    else:
        return False

def hasE (s):
    s = s.lower().strip()
    if s.find("data") != -1:
        return True
    else:
        return False

def hasAbreviation (s):
    s = s.lower().strip().replace(" ", "")
    if (s.find("rpi") != -1 or
        s.find("cpi") != -1 or
        s.find("gdp") != -1 or
        s.find("lidar") != -1 or
        s.find("defra") != -1 or
        s.find("acnc") != -1 or
        s.find("gnaf") != -1 or
        s.find("mot") != -1 or
        s.find("asic") != -1 or
        s.find("ons") != -1 or
        s.find("lsoa") != -1 or
        s.find("sparql") != -1 or
        s.find("psma") != -1 or
        s.find("fdi") != -1 or
        s.find("hs2") != -1 or
        s.find("awe") != -1 or
        s.find("ssi") != -1 or
        s.find("ds") != -1 or
        s.find("gp") != -1 or
        s.find("g.p") != -1 or
        s.find("api") != -1 or
        s.find("r p i") != -1 or
        s.find("gva") != -1 or
        s.find("ppi") != -1 or
        s.find("c.h.a.w") != -1 or
        s.find("abn") != -1 or
        s.find("eu") != -1 or
        s.find("gis") != -1 or
        s.find("ato") != -1 or
        s.find("lga") != -1 or
        s.find("imd") != -1 or
        s.find("tfl") != -1
        ):
        return True
    else:
        return False

dataArray = ["data","dataset","average","index","statistic","database","graph","table","indice",
"rpi","download","api","cpi","r.p.i","stat","survey","map","rate","total","file","review","trend",
"information","html","csv","wms","wcs","xls","pdf","json","wfs","esri rest","zip"]
def hasData (s):
    s = s.lower()
    sArray = s.split(" ")
    for word in sArray:
        if word in dataArray:
            return True
    return False

# function used for various query types count;
#'if hasAbr(query['searchTerms'])' needs to be updated with a specific function
def getQueryType(collection):
    counter = 0.00
    counterAll = 0.00
    for query in collection.find():
        counterAll = counterAll + query['count']
        if hasAbr(query['searchTerms']):
            counter = counter + int(query['count'])

    print "number: " + str(counter)
    print "all: " + str(counterAll)
    return counter


x1 = createHeadersCollection(db.internalDguClean)
x2 = createHeadersCollection(db.internalOnsClean)
x3 = createHeadersCollection(db.internalCanClean)
x4 = createHeadersCollection(db.internalAusClean)



# generating list of queries with numbers for further analysis
def getquerieslist (collection, fileName):
    with open(fileName, 'w') as csvNew:
        fieldnames = ['searchTerms']
        writer = csv.DictWriter(csvNew, fieldnames=fieldnames)
        #writer.writeheader()
        for query in collection.find():
            if hasNumbers(query['searchTerms']):
                row = {'searchTerms': query['searchTerms']}
                writer.writerow(row)

x1 = getquerieslist(db.internalDguClean, "dguNumbers.csv")
x2 = getquerieslist(db.internalOnsClean, "onsNumbers.csv")
x3 = getquerieslist(db.internalCanClean, "canNumbers.csv")
x4 = getquerieslist(db.internalAusClean, "ausNumbers.csv")
