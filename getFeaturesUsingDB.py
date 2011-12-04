import pymongo
import csv
import urllib2
import json
import random
import numpy
import itertools
from multiprocessing import Pool
from multiprocessing import Lock


SERVER = "ec2-50-112-6-22.us-west-2.compute.amazonaws.com" #new
#SERVER = "ec2-50-112-32-119.us-west-2.compute.amazonaws.com" #old
PORT = 1000

db = pymongo.Connection(SERVER, PORT).wp

LEVELS = 2
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
  #print db.categories.find_one()
  #page = db.pages.find_one({"_id": 12})
  #print page
  #page = db.pages.find_one({"_id": 43568}) #tom hanks
  #print page
  
  file = open('features_5.csv', 'wb')
  fileWriter = csv.writer(file)
  fileWriter.writerow(['Class', 'OutLinkNum', 'OutLinkProp','OutLinkNum2', 'OutLinkProp2', 'InLinkNum', 'InLinkProp','InLinkNum2', 'InLinkProp2'])
  file.close()

  catReader = csv.reader(open('american_actors_categories_catids_noDuplicates.txt', 'rb'))
  actorCategoryIds = []
  for row in catReader:
    actorCategoryIds.append(row[0])

  ''' 
  # getting category ids from db
  outfile = open('american_actors_categories_catids_noDuplicates.txt', 'w')
  catFile = open('american_actors_categories_titles_noDuplicates.txt', 'rb')
  actorCategoryIds = []
  for row in catFile:
    catName = unicode(row[9:-2],'utf-8').replace(' ', '_')
    print catName
    category = db.categories.find_one({"title":catName})
    if category != None:
      actorCategoryIds.append(category["_id"])
      outfile.write(str(category["_id"])+'\n')
  outfile.close()
  '''

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
  nonActorPageIds = [int(line) for line in open('random_nonActors.txt', 'r').readlines()]
  for pageId in nonActorPageIds:
    dbPage = db.pages.find_one({"_id": pageId})
    if dbPage != None:
      nonActorPages.append(dbPage)
  nonActorPages = nonActorPages[0:50]

  #db.pages.find({u'categories': {'$nin': actorCategoryIds}}).limit(2000)

  actorPages = []
  actorPageIds = [int(line) for line in open('random_actors.txt', 'r').readlines()]
  for pageId in actorPageIds:
    dbPage = db.pages.find_one({"_id": pageId})
    if dbPage != None:
      actorPages.append(dbPage)
  actorPages = actorPages[0:50]
  #actorPages = [actorPages[20]]
  
  #db.pages.find({u'categories': {'$in':actorCategoryIds}}).limit(2000)
  
  print 'Found sample set'

  p = Pool(50)

  args = itertools.izip(actorPages,itertools.repeat(set(actorCategoryIds)))
  p.map(get_features_for_actor_page_star , args)
  args = itertools.izip(nonActorPages,itertools.repeat(set(actorCategoryIds)))
  p.map(get_features_for_nonActor_page_star, args)

def get_features_for_actor_page_star(page_actCat):
  return get_features_for_actor_page(*page_actCat)

def get_features_for_nonActor_page_star(page_actCat):
  return get_features_for_nonActor_page(*page_actCat)

def get_features_for_actor_page(page, actCat):
  data = ['Actor']
  try: 
    data = calculate_network_features(page, actCat, data)
    print 'Got features!'
    lock.acquire()
    file = open('features_5.csv', 'a')
    fileWriter = csv.writer(file)
    fileWriter.writerow(data)
    file.close()
    lock.release()
  except Exception as e:
    if 'title' in page:
      print 'Couldn\'t get features for ' + page['title']
    else:
      print 'Couldn\'t get features for page with missing title'

def get_features_for_nonActor_page(page, actCat):
  data = ['Nonactor']
  try:
    data = calculate_network_features(page, actCat, data)
    print 'Got features!'
    lock.acquire()
    file = open('features_5.csv', 'a')
    fileWriter = csv.writer(file)
    fileWriter.writerow(data)
    file.close()
    lock.release()
  except Exception as e:
    if 'title' in page:
      print 'Couldn\'t get features for ' + page['title']
    else:
      print 'Couldn\'t get features for page with missing title'

def calculate_network_features(page, actCat, data):
  data = calculate_link_features([page], actCat, u'oe', LEVELS, data)
  data = calculate_link_features([page], actCat, u'ie', LEVELS, data)
  # data = calculate_link_features([page], actCat, u'outgoing_edges', LEVELS, data)
  # data = calculate_link_features([page], actCat, u'incoming_edges', LEVELS, data)
  print data
  #db.pages.update({"_id": 12}, {"$set": { "field" : "value" } }}
  return data

def calculate_link_features(pages,  actCat, feature, levels, data):
  curPages = pages
  for level in range(levels):
    # print 'at level '+str(level)
    numActorLinks = 0
    numTotalLinks = 0
    nextPages = []
    for page in curPages:
      # if 'title' in page:
      #   print 'looking at page '+page['title']
      # else:
      #   print 'looking at page without title'
      if feature in page:
        pageCursor = db.pages.find({"_id":{'$in': page[feature]}})
        # print pageCursor.count()
        for ind in range(pageCursor.count()):
          numTotalLinks += 1
          dbLinkedPage = pageCursor[ind]
          nextPages.append(dbLinkedPage)
          if dbLinkedPage != None and u'categories' in dbLinkedPage:
            #print 'here'
            otherPageCats = dbLinkedPage[u'categories']
            sameCats = actCat.intersection(set(otherPageCats))
            #print sameCats
            if len(sameCats) != 0:
              numActorLinks += 1
    data.append(numActorLinks)
    if numTotalLinks == 0:
      data.append(0)
    else: 
      data.append(float(numActorLinks)/numTotalLinks)
    curPages = nextPages
  # print data
  return data

# import cProfile
# cProfile.run('get_features()', 'get_featuresPROF')

if __name__ == "__main__":
  get_features()
