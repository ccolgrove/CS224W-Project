import pymongo
import csv
import urllib2
import json
import random
import numpy

SERVER = 'ec2-50-112-32-119.us-west-2.compute.amazonaws.com'
PORT = 1000

db = pymongo.Connection(SERVER, PORT).wp

def calculate_network_features(page, actCat, data):
  numOutLinks = 0
  if u'outgoing_edges' in page:
    for linkId in page[u'outgoing_edges']:
      dbLinkedPage = db.pages.find_one({"_id": linkId})
      if dbLinkedPage != None and u'categories' in dbLinkedPage:
        otherPageCats = dbLinkedPage[u'categories']
        sameCats = set(actCat).intersection(set(otherPageCats))
        if len(sameCats) != 0:
          numOutLinks += 1
  data.append(numOutLinks)
  if (not (u'outgoing_edges' in page)) or (len(page[u'outgoing_edges']) == 0):
    data.append(0)
  else: 
    data.append(float(numOutLinks)/len(page[u'outgoing_edges']))

  numInLinks = 0
  if u'incoming_edges' in page:
    for linkId in page[u'incoming_edges']:
      dbLinkedPage = db.pages.find_one({"_id": linkId})
      if dbLinkedPage != None and u'categories' in dbLinkedPage:
        otherPageCats = dbLinkedPage[u'categories']
        sameCats = set(actCat).intersection(set(otherPageCats))
        if len(sameCats) != 0:
          numInLinks += 1
  data.append(numInLinks)
  if (not (u'incoming_edges' in page)) or (len(page[u'incoming_edges']) == 0):
    data.append(0)
  else: 
    data.append(float(numInLinks)/len(page[u'incoming_edges']))

  print data
  #db.pages.update({"_id": 12}, {"$set": { "field" : "value" } }}
  return data


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
  #page = db.pages.find_one({"_id": 420422})
  #print page
  #print db.categories.find_one({"_id": u'12'})
  #print db.pages.find_one({"_id": 32025164})
  

  fileWriter = csv.writer(open('features.csv', 'wb'))
  fileWriter.writerow(['Class', 'OutLinkNum', 'OutLinkProp', 'InLinkNum', 'InLinkProp'])
  
  catReader = csv.reader(open('american_actors_categories_catids_noDuplicates.txt', 'rb'))
  actorCategoryIds = []
  for row in catReader:
    actorCategoryIds.append(row[0])
 
  ''' 
  #getting category ids from db
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

  nonActorPages = []
  for i in range(5):
    queryResults = json.JSONDecoder().decode(urllib2.urlopen('http://en.wikipedia.org/w/api.php?action=query&list=random&rnlimit=10&rnnamespace=0&format=json').read())
    for page in queryResults['query']['random']:
      dbPage = db.pages.find_one({"_id": page['id']})
      if dbPage != None:
      	nonActorPages.append(dbPage)

  #db.pages.find({u'categories': {'$nin': actorCategoryIds}}).limit(2000)

  actorPages = []
  actorPageIds = [int(line) for line in open('random_actors.txt', 'r').readlines()]
  for pageId in actorPageIds:
    dbPage = db.pages.find_one({"_id": pageId})
    if dbPage != None:
      actorPages.append(dbPage)
  actorPages = actorPages[0:50]
  
  #db.pages.find({u'categories': {'$in':actorCategoryIds}}).limit(2000)
  
  print 'Found sample set'

  for page in nonActorPages:
    data = ['Nonactor']
    try:
      data = calculate_network_features(page, actorCategoryIds, data)
      print 'Got features!'
    except Exception as e:
      if 'title' in page:
        print 'Couldn\'t get features for ' + page['title']
      else:
        print 'Couldn\'t get features for page with missing title'
      continue 
    fileWriter.writerow(data)

  for page in actorPages:
    data = ['Actor']
    try: 
      data = calculate_network_features(page, actorCategoryIds, data)
      print 'Got features!'
    except Exception as e:
      if 'title' in page:
        print 'Couldn\'t get features for ' + page['title']
      else:
        print 'Couldn\'t get features for page with missing title'
      continue
    fileWriter.writerow(data)
    

if __name__ == "__main__":
  get_features()
