import csv
import pymongo

db = pymongo.Connection('localhost', 1000).wp

f = open('/store/page_links.csv', 'r')
count = 60100000
lines = f.readlines(60100000)
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
        end_document = db.pages.find_one({"title": row[1].strip().decode('latin-1').encode('utf-8')}, fields=["_id"])
        if end_document is not None:
                
                end = int(end_document["_id"])
                
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
    
