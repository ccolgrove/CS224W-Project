import csv
import pymongo

db = pymongo.Connection('localhost', 1001).wp

edge_db = pymongo.Connection('localhost', 1000).wp
#db.drop_collection("pages")
#db.pages.create_index("page_id")
#db.pages.create_index("page_title")

pageReader = csv.reader(open('page_info.csv', 'r'), delimiter=',')
count = 0
for row in pageReader:
    if count % 10000 == 0:
        print count
    if count >= 285854:
        pages = edge_db.pages.find({"page_id":int(row[0])})
        outgoing_links = set()
        incoming_links = set()
        for page in pages:
            if "outgoing_links" in page:
                outgoing_links.update(set(page["outgoing_links"]))
            if "incoming_links" in page:
                incoming_links.update(set(page["incoming_links"]))
        
        db.pages.insert({
            "_id": int(row[0]),
            "page_title": row[1],
            "page_is_redirect": row[2],
            "page_is_new": row[3],
            "page_latest": row[4],
            "page_len": row[5],
            "outgoing_links": list(outgoing_links),
            "incoming_links": list(incoming_links)
        })

    count += 1
    
