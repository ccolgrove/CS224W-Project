import csv
import pymongo

db = pymongo.Connection('localhost', 1000).wp

CATEGORY_CSV = '../data/category_links_new.csv'

pageReader = csv.reader(open(CATEGORY_CSV, 'r'), delimiter=',')
count = 0
for row in pageReader:
    if count % 10000 == 0:
        print count
    try:
    	db.pages.update({
        	"_id": int(row[0])
    	}, {
        	"$addToSet": {"categories" : row[1] }
    	})
    except Exception as e:
	print "Error ", row[1]
    count += 1
    
