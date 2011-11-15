import csv
import pymongo

db = pymongo.Connection('localhost', 1001).wp

CATEGORY_CSV = 'page_info.csv'

pageReader = csv.reader(open(CATEGORY_CSV, 'r'), delimiter=',')
count = 0

for row in pageReader:
    if count % 10000 == 0:
        print count

    db.pages.insert({
        "_id" : int(row[1])
        "name": row[0]
    })

    count += 1
    
