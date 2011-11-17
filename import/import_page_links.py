import csv
import pymongo

db = pymongo.Connection('localhost', 1000).wp

SERVER = 'ec2-50-112-32-119.us-west-2.compute.amazonaws.com'
PORT = 1000 #1000

clouddb = pymongo.Connection(SERVER, PORT).wp

f = open('page_links.csv', 'r')
count = 0

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
        end_document = db.pages.find_one({"page_title": unicode(row[1].strip(), "utf-8")}, fields=["page_id"])
        
        if end_document is not None:
                
                end = int(end_document["page_id"])
                
                clouddb.pages.update({
                    "_id": start
                },{
                    "$push": {"outgoing_links_2" : end}
                })
    
                clouddb.pages.update({
                    "_id": end
                },{
                    "$push": {"incoming_links_2" : start}
                })
                
        count += 1
    
