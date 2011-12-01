import csv
import pymongo

db = pymongo.Connection('localhost', 1000).wp

f = open('/store/page_links.csv', 'r')
count = 0
lines = f.readlines(count)
while 1:
    lines = f.readlines(1000000)
    if not lines:
        break
    for line in lines:
        row = line.split(',')
        if count % 10000 == 0:
            print count
            print "Percent done %2f" % (count/float(336720000)*100)
        start = int(row[0])
        end = int(row[1])
        
        if end_document is not None:

                db.pages.update({
                    "_id": start
                },{
                    "$push": {"outgoing_edges" : end}
                })
    
                db.pages.update({
                    "_id": end
                },{
                    "$push": {"incoming_edges" : start}
                })
                
        count += 1
    
