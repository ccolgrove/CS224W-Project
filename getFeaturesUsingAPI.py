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
  #print page
  queryResults = []
  query = "http://en.wikipedia.org/w/api.php?action=query&prop=links&titles="+page[u'page_title']+"&pllimit=500&format=json"
  query = query.decode('latin-1').encode('utf-8')
  queryResults.append(json.JSONDecoder().decode(urllib2.urlopen(query).read()))
  while 'query-continue' in queryResults[-1]:
    query = "http://en.wikipedia.org/w/api.php?action=query&prop=links&titles="+page[u'page_title']+"&format=json&plcontinue="+queryResults['query-continue'].links.plcontinue
    query = query.decode('latin-1').encode('utf-8')
    queryResults.append(json.JSONDecoder().decode(urllib2.urlopen(query).read()))

  numOutLinksActors = 0
  numOutLinksTotal = 0
  for queryResult in queryResults:
    print queryResult
    for sourcePage in queryResult['query']['pages']:
      for linkedPage in queryResult['query']['pages'][sourcePage]['links']:
        numOutLinksTotal += 1
        title = linkedPage['title'].replace(' ', '_')
        #print linkedPage['title']
        dbLinkedPage = db.pages.find_one({"page_title": title})
        if dbLinkedPage != None and u'categories' in dbLinkedPage:
          otherPageCats = dbLinkedPage[u'categories']
          sameCats = set(actCat).intersection(set(otherPageCats))
          if len(sameCats) != 0:
            numOutLinksActors += 1

  data.append(numOutLinksActors)
  if len(page[u'outgoing_links']) == 0:
    data.append(0.0)
  else: 
    data.append(numOutLinksActors/float(numOutLinksTotal))

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
  #page = db.pages.find_one({"_id": 12})
  #print page
  #page = db.pages.find_one({"_id": 43568}) #tom hanks
  #print page
  #page = db.pages.find_one({"_id": 637643})
  #print page
  #print db.categories.find_one({"_id": u'12'})
  #print db.pages.find_one({"_id": 32025164})
  

  fileWriter = csv.writer(open('features.csv', 'wb'))
  fileWriter.writerow(['Class', 'OutLinkNum', 'OutLinkProp'])
  
  catReader = csv.reader(open('american_actors_categories.txt', 'rb'))
  actorCategories = []
  for row in catReader:
    actorCategories.append(row[0])
  #print actorCategories
  
  catFile = open('american_actors_categories_titles_tiny.txt', 'rb')
  actorCategoryIds = []
  row = catFile.readline()
  while row != "":
    catName = unicode(row[9:-2],'utf-8').replace(' ', '_')
    #print catName
    category = db.categories.find_one({"name":catName})
    #category = db.categories.find_one({"name":u'Actors_from_Washington,_D.C.'})
    #print category
    actorCategoryIds.append(category["_id"])
    row = catFile.readline()
  #print actorCategories

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
  
  
  print 'woohoo'

  for page in nonActorPages:
    data = ['Nonactor']
    try:
      data = calculate_network_features(page, actorCategoryIds, data)
    except Exception as e:
      continue 
    fileWriter.writerow(data)

  for page in actorPages:
    data = ['Actor']
    try: 
      data = calculate_network_features(page, actorCategoryIds, data)
    except Exception as e:
      continue
    #data = calculate_network_features(db.pages.find_one({"_id":12138300}), actorCategories, data)
    fileWriter.writerow(data)
    

if __name__ == "__main__":
  get_features()
