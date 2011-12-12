import pymongo
import csv
import urllib2
import cjson
import random
import numpy

c = pymongo.Connection("ec2-50-112-6-22.us-west-2.compute.amazonaws.com", 1000)

for x in ['actor_ids.txt']:#["american_musical_theater_actors_ids.txt","desserts_ids.txt","graph_theory_ids.txt"]:
    f = open(x)
    actors = set([int(line.strip()) for line in f])
    f2 = open("non_"+x, 'w')

    for i in range(100):
        queryResults = cjson.decode(urllib2.urlopen('http://en.wikipedia.org/w/api.php?action=query&list=random&rnlimit=50&rnnamespace=0&format=json').read())
        for page in queryResults['query']['random']:
            if c.wp.pages.find({"_id": page['id']}).count() == 1 and page['id'] not in actors:
          	    f2.write("%i\n"%page['id'])
