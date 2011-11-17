import csv
import pymongo

SERVER = 'ec2-50-112-32-119.us-west-2.compute.amazonaws.com'
PORT = 1000 #1000

db = pymongo.Connection(SERVER, PORT).wp

CATEGORY_CSV = '../data/category.csv'

pageReader = csv.reader(open(CATEGORY_CSV, 'r'), delimiter=',')
count = 0

for row in pageReader:
	if count % 10000 == 0:
		print count
        
	if count > 430000:
		id = int(row[-1])
		name = ','.join(row[0:-1])
		name = name.decode('latin-1').encode('utf-8')
		db.categories.insert({
     		"_id" : id,
	   		"name": name
    	})
	count += 1
    
