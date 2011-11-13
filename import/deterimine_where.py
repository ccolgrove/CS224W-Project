import csv
import pymongo

db = pymongo.Connection('localhost', 1000).wp
f = open('page_links.csv', 'r')
count = 0
length = 0
while 1:
    lines = f.readlines(1000000)
    if not lines:
        break
    for line in lines:
        if count % 10000 == 0:
            print count, length
        length += len(line)
        count += 1
    
