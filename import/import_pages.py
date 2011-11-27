import csv
import pymongo

db = pymongo.Connection('localhost', 1000).wp
db.drop_collection("pages")
#db.pages.create_index("page_id")
#db.pages.create_index("page_title")
pageReader = csv.reader(open('page_info.csv', 'r'), delimiter=',')
count = 0
for row in pageReader:
    if count % 10000 == 0:
        print count
    db.pages.insert({
        "_id": int(row[0]),
        "title": row[1].decode('latin-1').encode('utf-8'),
        "page_is_redirect": bool(row[2]),
        "page_is_new": bool(row[3]),
        "page_latest": bool(row[4]),    
        "page_len": row[5],
    })
    count += 1
    
