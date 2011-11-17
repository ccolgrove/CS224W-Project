import csv
import pymongo

db = pymongo.Connection('localhost', 1000).wp
db.pages.create_index("categories")
catReader = csv.reader(open('category_links.csv', 'r'), delimiter=',')
count = 0
for row in catReader:
    if count % 10000 == 0:
        print count
    print {
        "page_id":int(row[0])
    }, {
        "$addToSet": {"categories" : int(row[1]) }
    }
    '''
    db.pages.update({
        "page_id": row[0]
    },{
        "$addToSet": {
            { "categories" : category_id } 
        }
    })
    '''
    count += 1
    
