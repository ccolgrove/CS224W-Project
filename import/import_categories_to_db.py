import csv
import pymongo

db = pymongo.Connection('localhost', 1001).wp

CATEGORY_CSV = '../data/category_links_new.csv'

pageReader = csv.reader(open(CATEGORY_CSV, 'r'), delimiter=',')
count = 0
for row in pageReader:
    if count % 10000 == 0:
        print count

    db.pages.update({
        "page_id":int(row[0])
    }, {
        "$addToSet": {"categories" : int(row[1]) }
    })

    count += 1
    
