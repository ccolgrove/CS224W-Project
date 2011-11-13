import csv
import pymongo

db = pymongo.Connection('localhost', 1000).wp
f = open('page_links.csv', 'r')
f.seek(616941356)
count = 24060000

while 1:
    lines = f.readlines(1000000)
    if not lines:
        break
    for line in lines:
        row = line.split(',')
        if count % 10000 == 0:
            print count
            print "Percent done %2f" % (count/float(336720000)*100)
        try:    
            start = int(row[0])
            end = db.pages.find_one({"page_title": row[1]}, fields=["page_id"])
            if end is not None:
                db.pages.update({
                    "page_id": start
                },{
                    "$addToSet": {"outgoing_links" : end["page_id"] }
                })
    
                db.pages.update({
                    "page_id": end["page_id"]
                },{
                    "$addToSet": {"incoming_links" : start}
                })
        except Exception as e:
            print "Unicode error"     
        count += 1
    
