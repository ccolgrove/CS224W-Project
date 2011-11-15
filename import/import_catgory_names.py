import csv
import pymongo

db = pymongo.Connection('localhost', 1000).wp

CATEGORY_CSV = '../data/category.csv'

pageReader = csv.reader(open(CATEGORY_CSV, 'r'), delimiter=',')
count = 0

for row in pageReader:
    if count % 10000 == 0:
        print count
    try:
    	db.categories.insert({
        	"_id" : row[1],
	        "name": row[0]
    	})
    except Exception as e:
	print "not unicode"
    count += 1
    
