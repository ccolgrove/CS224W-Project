# example run: python getFeaturesUsingDB.py \
#  "actor_sample_ids_file" "all_sample_ids_file" 
#  "actor_all_ids_file" "actor_all_one_hop_ids_file" "output_file"

import pymongo
import csv
import urllib2
import json
import random
import numpy
import itertools
import sys
from multiprocessing import Pool
from multiprocessing import Lock


SERVER = "ec2-50-112-6-22.us-west-2.compute.amazonaws.com" #new
#SERVER = "ec2-50-112-32-119.us-west-2.compute.amazonaws.com" #old
PORT = 1000

db = pymongo.Connection(SERVER, PORT).wp

LEVELS = 1
haveOneHopCats = False
NUM_THREADS = 1
#ACTOR_CATEGORIES = set()
#fileWriter = csv.writer(open('features_3.csv', 'wb'))
lock = Lock()

def printCats():
  #global ACTOR_CATEGORIES
  print ACTOR_CATEGORIES

def get_features():
  '''
  Calculates the following features and writes them to a csv file.
    
  - number of outlinks to actors category
  - proportion of outlinks to actors category
  - number of inlinks from actors category
  - proportion of inlinks from actors category    

  '''
  print 'starting'
  # print db.categories.find_one()
  # return 0
  #page = db.pages.find_one({"_id": 12})
  #print page
  # page = db.pages.find_one({"_id": 43568}) #tom hanks
  # print page
  
  file = open(sys.argv[3], 'wb')
  fileWriter = csv.writer(file)
  labels = ['PageId','Class']
  for level in range(LEVELS):
    labels.append('OutLinkNum'+str(level))
    labels.append('OutLinkProp'+str(level))
  for level in range(LEVELS):
    labels.append('InLinkNum'+str(level))
    labels.append('InLinkProp'+str(level))
  fileWriter.writerow(labels)
  file.close()

  # catReader = csv.reader(open('american_actors_categories_catids_noDuplicates.txt', 'rb'))
  # actorCategoryIds = []
  # for row in catReader:
  #   actorCategoryIds.append(row[0])

  actorPageReader = csv.reader(open(sys.argv[4]))
  allActorPageIds = set()
  for row in actorPageReader:
    allActorPageIds.add(int(row[0]))

  actorPageReader = csv.reader(open(sys.argv[5]))
  allActorAndNeighborPageIds = set()
  for row in actorPageReader:
    allActorAndNeighborPageIds.add(int(row[0]))

  # getting category ids from db
  # outfile = open('american_actors_categories_catids_noDuplicates.txt', 'w')
  # catFile = open('american_actors_categories_titles_noDuplicates.txt', 'rb')
  # actorCategoryIds = []
  # outfile = open('american_musical_theater_actors_catids.txt', 'w')
  # catFile = open('american_musical_theater_actors_categories.txt', 'rb')
  # for row in catFile:
  #   catName = unicode(row[9:-2],'utf-8').replace(' ', '_')
  #   print catName
  #   category = db.categories.find_one({"title":catName})
  #   if category != None:
  #     actorCategoryIds.append(category["_id"])
  #     outfile.write(str(category["_id"])+'\n')
  # outfile.close()
  

  '''
  # getting random nonactor pages using api
  outfile = open('random_nonActors.txt', 'w')
  nonActorPages = []
  for i in range(5):
    queryResults = json.JSONDecoder().decode(urllib2.urlopen('http://en.wikipedia.org/w/api.php?action=query&list=random&rnlimit=10&rnnamespace=0&format=json').read())
    for page in queryResults['query']['random']:
      dbPage = db.pages.find_one({"_id": page['id']})
      if dbPage != None:
        nonActorPages.append(dbPage)
        print 'writing ' + str(page['id'])
        outfile.write(str(page['id'])+'\n')
  '''

  nonActorPages = []
  # nonActorPageIds = [int(line) for line in open('random_nonActors.txt', 'r').readlines()]
  nonActorPageIds = [int(line) for line in open(sys.argv[2], 'r').readlines()]
  # for pageId in nonActorPageIds:
  #   dbPage = db.pages.find_one({"_id": pageId})
  #   if dbPage != None:
  #     nonActorPages.append(dbPage)
  # nonActorPages = nonActorPages[0:50]
  nonActorPages = nonActorPageIds[0:50]

  #db.pages.find({u'categories': {'$nin': actorCategoryIds}}).limit(2000)

  # actorPages = []
  # actorPageIds = [int(line) for line in open('random_actors.txt', 'r').readlines()]
  actorPageIds = [int(line) for line in open(sys.argv[1], 'r').readlines()]
  # for pageId in actorPageIds:
    # dbPage = db.pages.find_one({"_id": pageId})
    # if dbPage != None:
      # actorPages.append(dbPage)
  # actorPages = actorPages[0:50]
  #actorPages = [actorPages[20]]
  actorPages = actorPageIds[0:50]
  
  #db.pages.find({u'categories': {'$in':actorCategoryIds}}).limit(2000)
  
  print 'Found sample set'

  p = Pool(NUM_THREADS)

  args = itertools.izip(actorPages,itertools.repeat(allActorPageIds),itertools.repeat(allActorAndNeighborPageIds))
  p.map(get_features_for_actor_page_star, args)
  args = itertools.izip(nonActorPages,itertools.repeat(allActorPageIds),itertools.repeat(allActorAndNeighborPageIds))
  p.map(get_features_for_nonActor_page_star, args)

def get_features_for_actor_page_star(page_actCat_actPlusCat):
  return get_features_for_actor_page(*page_actCat_actPlusCat)

def get_features_for_nonActor_page_star(page_actCat_actPlusCat):
  return get_features_for_nonActor_page(*page_actCat_actPlusCat)

def get_features_for_actor_page(pageId, actCat, actPlusCat):
  data = [pageId, 'Actor']
  # try: 
  data = calculate_network_features(pageId, actCat, actPlusCat, data)
  print 'Got features!'
  lock.acquire()
  file = open(sys.argv[3], 'a')
  fileWriter = csv.writer(file)
  fileWriter.writerow(data)
  file.close()
  lock.release()
  # except Exception as e:
  #   print 'Couldn\'t get features for ' + str(pageId)

def get_features_for_nonActor_page(pageId, actCat, actPlusCat):
  data = [pageId, 'Nonactor']
  # try:
  data = calculate_network_features(pageId, actCat, actPlusCat, data)
  print 'Got features!'
  lock.acquire()
  file = open(sys.argv[3], 'a')
  fileWriter = csv.writer(file)
  fileWriter.writerow(data)
  file.close()
  lock.release()
  # except Exception as e:
  #   print 'Couldn\'t get features for ' + str(pageId)

def calculate_network_features(pageId, actCat, actPlusCat, data):
  data = calculate_link_features(pageId, actCat, actPlusCat, u'oe', LEVELS, data)
  data = calculate_link_features(pageId, actCat, actPlusCat, u'ie', LEVELS, data)
  print data
  #db.pages.update({"_id": 12}, {"$set": { "field" : "value" } }}
  return data

def calculate_link_features(pageId,  actCat, actPlusCat, feature, levels, data):
  # level 0
  numActorLinks = 0
  numTotalLinks = 0
  nextPageIds = []
  page = db.pages.find_one({"_id": pageId})
  if page != None and feature in page:
    for linkedPageId in page[feature]:
      numTotalLinks += 1
      if linkedPageId in actCat:
        numActorLinks += 1
      nextPageIds.append(linkedPageId)
  data.append(numActorLinks)
  if numTotalLinks == 0:
    data.append(0)
  else: 
    data.append(float(numActorLinks)/numTotalLinks)
  
  # other levels
  if haveOneHopCats:
    curPageIds = [pageId]
  else:
    curPageIds = nextPageIds
  for level in range(levels)[1:]:
    numActorLinks = 0
    numTotalLinks = 0
    nextPageIds = []
    pageCursor = db.pages.find({"_id":{'$in': curPageIds}})
    for page in pageCursor:
      if feature in page:
        for linkedPageId in page[feature]:
          numTotalLinks += 1
          if (haveOneHopCats and (not linkedPageId in actCat ) and (linkedPageId in actPlusCat))\
              or ((not haveOneHopCats) and (linkedPageId in actCat)):
            numActorLinks += 1
          nextPageIds.append(linkedPageId)
    data.append(numActorLinks)
    if numTotalLinks == 0:
      data.append(0)
    else: 
      data.append(float(numActorLinks)/numTotalLinks)
    #print data
    curPageIds = nextPageIds
  #print data
  return data

# import cProfile
# cProfile.run('get_features()', 'get_featuresPROF')

if __name__ == "__main__":
  get_features()
